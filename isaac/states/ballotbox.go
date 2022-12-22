package isaacstates

import (
	"context"
	"math"
	"sort"
	"sync"
	"time"

	"github.com/pkg/errors"
	"github.com/rs/zerolog"
	"github.com/spikeekips/mitum/base"
	"github.com/spikeekips/mitum/isaac"
	"github.com/spikeekips/mitum/util"
	"github.com/spikeekips/mitum/util/logging"
)

type Ballotbox struct {
	*util.ContextDaemon
	*logging.Logging
	local            base.Address
	getSuffragef     isaac.GetSuffrageByBlockHeight
	isValidVoteproof func(base.Voteproof, base.Suffrage) error
	suffrageVote     func(base.SuffrageWithdrawOperation) error
	newBallot        func(base.Ballot)
	vrs              map[string]*voterecords
	vpch             chan base.Voteproof
	lsp              *util.Locked[[2]interface{}]
	removed          []*voterecords
	vrsLock          sync.RWMutex
	countLock        sync.Mutex
	countAfter       time.Duration
	interval         time.Duration
}

func NewBallotbox(
	local base.Address,
	getSuffragef isaac.GetSuffrageByBlockHeight,
	isValidVoteproof func(base.Voteproof, base.Suffrage) error,
	countAfter time.Duration,
) *Ballotbox {
	box := &Ballotbox{
		Logging: logging.NewLogging(func(zctx zerolog.Context) zerolog.Context {
			return zctx.Str("module", "ballotbox")
		}),
		local:            local,
		getSuffragef:     getSuffragef,
		vrs:              map[string]*voterecords{},
		vpch:             make(chan base.Voteproof, math.MaxUint16),
		lsp:              util.EmptyLocked([2]interface{}{}),
		isValidVoteproof: isValidVoteproof,
		newBallot:        func(base.Ballot) {},
		countAfter:       countAfter,
		interval:         time.Second,
	}

	box.ContextDaemon = util.NewContextDaemon(box.start)

	return box
}

func (box *Ballotbox) Vote(bl base.Ballot, threshold base.Threshold) (bool, error) {
	box.vrsLock.Lock()
	defer box.vrsLock.Unlock()

	var withdraws []base.SuffrageWithdrawOperation
	if i, ok := bl.(isaac.BallotWithdraws); ok {
		withdraws = i.Withdraws()
	}

	voted, callback, err := box.vote(
		bl.SignFact(),
		bl.Voteproof(),
		withdraws,
		threshold,
	)
	if err != nil {
		return false, err
	}

	if callback != nil {
		go func() {
			_ = callback()
		}()
	}

	box.newBallot(bl)

	return voted, nil
}

func (box *Ballotbox) VoteSignFact(sf base.BallotSignFact, threshold base.Threshold) (bool, error) {
	voted, callback, err := box.vote(sf, nil, nil, threshold)
	if err != nil {
		return false, err
	}

	if callback != nil {
		go func() {
			_ = callback()
		}()
	}

	return voted, nil
}

func (box *Ballotbox) Voteproof() <-chan base.Voteproof {
	if box == nil {
		return nil
	}

	return box.vpch
}

// Count should be called for checking next voteproofs after new block saved.
func (box *Ballotbox) Count(threshold base.Threshold) {
	box.countLock.Lock()
	defer box.countLock.Unlock()

	rvrs, nvrs := box.notFinishedVoterecords()

	for i := range rvrs {
		vr := rvrs[len(rvrs)-i-1]
		if vp := box.count(vr, threshold); vp != nil {
			break
		}
	}

	for i := range nvrs {
		vr := nvrs[i]
		if _, found, err := vr.getSuffrage(); !found || err != nil {
			break
		}

		_ = box.count(vr, threshold)
	}

	box.clean()
}

func (box *Ballotbox) SetSuffrageVote(f func(base.SuffrageWithdrawOperation) error) {
	box.suffrageVote = f
}

func (box *Ballotbox) SetNewBallot(f func(base.Ballot)) {
	box.newBallot = f
}

func (box *Ballotbox) countWithVoterecords(
	vr *voterecords, threshold base.Threshold,
) base.Voteproof {
	box.countLock.Lock()
	defer box.countLock.Unlock()

	vp := box.count(vr, threshold)

	box.clean()

	return vp
}

func (box *Ballotbox) vote(
	signfact base.BallotSignFact,
	vp base.Voteproof,
	withdraws []base.SuffrageWithdrawOperation,
	threshold base.Threshold,
) (bool, func() base.Voteproof, error) {
	e := util.StringErrorFunc("failed to vote")

	last, lastIsMajority := box.lastStagePoint()

	fact := signfact.Fact().(base.BallotFact) //nolint:forcetypeassert //...

	l := box.Log().With().Interface("sign_fact", signfact).Logger()

	if !isNewBallotboxStagePoint(last, fact.Point(), lastIsMajority, isSuffrageConfirmBallotFact(fact)) {
		l.Debug().Bool("voted", false).Bool("validated", false).Msg("ballot voted")

		return false, nil, nil
	}

	vr := box.newVoterecords(fact.Point(), isSuffrageConfirmBallotFact(fact))

	voted, validated, err := vr.vote(signfact, vp, withdraws)
	if err != nil {
		l.Error().Bool("voted", voted).Bool("validated", validated).Msg("ballot voted")

		return false, nil, e(err, "")
	}

	l.Debug().Bool("voted", voted).Bool("validated", validated).Msg("ballot voted")

	var digged base.Voteproof

	if voted && vp != nil {
		if !validated {
			if err := isValidVoteproofWithSuffrage(box.getSuffragef, box.isValidVoteproof, vp); err == nil {
				validated = true
			}
		}

		if validated {
			last, lastIsMajority := box.lastStagePoint()

			if isnew, overthreshold := isNewBallotVoteproofWithThreshold(
				box.local,
				vp,
				last,
				lastIsMajority,
				threshold,
			); isnew && overthreshold {
				l.Debug().Interface("voteproof", vp).Msg("new voteproof from ballot") //nolint:goconst //...

				digged = vp
			}
		}
	}

	if digged != nil {
		box.setLastStagePoint(digged.Point(), digged.Result() == base.VoteResultMajority)
	}

	return voted,
		func() base.Voteproof {
			if digged != nil {
				box.vpch <- digged
			}

			return box.countWithVoterecords(vr, threshold)
		},
		nil
}

func (box *Ballotbox) newVoterecords( //revive:disable-line:flag-parameter
	stagepoint base.StagePoint,
	isSIGN bool,
) *voterecords {
	vrkey := stagepoint.String()
	if isSIGN {
		vrkey = "sign-" + vrkey
	}

	if vr, found := box.vrs[vrkey]; found {
		return vr
	}

	vr := newVoterecords(
		stagepoint,
		box.isValidVoteproof,
		box.getSuffragef,
		box.suffrageVote,
		isSIGN,
		box.Logging.Log(),
	)

	box.vrs[vrkey] = vr

	return vr
}

func (box *Ballotbox) lastStagePoint() (point base.StagePoint, lastIsMajority bool) {
	i, isempty := box.lsp.Value()
	if isempty {
		return point, false
	}

	return i[0].(base.StagePoint), i[1].(bool) //nolint:forcetypeassert //...
}

func (box *Ballotbox) setLastStagePoint(p base.StagePoint, isMajority bool) bool {
	_, err := box.lsp.Set(func(i [2]interface{}, isempty bool) ([2]interface{}, error) {
		if isempty {
			return [2]interface{}{p, isMajority}, nil
		}

		last := i[0].(base.StagePoint) //nolint:forcetypeassert //...
		lastIsMajority := i[1].(bool)  //nolint:forcetypeassert //...

		switch {
		case !lastIsMajority && isMajority &&
			p.Height() == last.Height() &&
			p.Stage() == base.StageINIT &&
			last.Stage() == base.StageINIT:
		case p.Compare(last) < 0:
			return [2]interface{}{}, errors.Errorf("not higher")
		}

		return [2]interface{}{p, isMajority}, nil
	})

	return err == nil
}

func (box *Ballotbox) count(vr *voterecords, threshold base.Threshold) base.Voteproof {
	last, lastIsMajority := box.lastStagePoint()
	if !isNewBallotboxStagePoint(last, vr.stagepoint, lastIsMajority, vr.isSIGN) {
		return nil
	}

	vps := vr.count(box.local, last, lastIsMajority, threshold, box.countAfter)
	if len(vps) < 1 {
		return nil
	}

	lastvp := vps[len(vps)-1]
	box.setLastStagePoint(lastvp.Point(), lastvp.Result() == base.VoteResultMajority)

	for i := range vps {
		box.vpch <- vps[i]
	}

	return lastvp
}

func (box *Ballotbox) clean() {
	box.vrsLock.Lock()
	defer box.vrsLock.Unlock()

	for i := range box.removed {
		voterecordsPoolPut(box.removed[i])
	}

	last, _ := box.lastStagePoint()
	stagepoint := last.Decrease()

	if last.IsZero() || stagepoint.IsZero() {
		return
	}

	removed := make([]*voterecords, len(box.vrs))

	var n int

	for i := range box.vrs {
		vr := box.vrs[i]

		if vr.stagepoint.Compare(stagepoint) > 0 {
			continue
		}

		delete(box.vrs, i)

		removed[n] = vr
		n++
	}

	box.removed = removed[:n]
}

// notFinishedVoterecords sorts higher point will be counted first
func (box *Ballotbox) notFinishedVoterecords() ([]*voterecords, []*voterecords) {
	box.vrsLock.RLock()
	defer box.vrsLock.RUnlock()

	last, _ := box.lastStagePoint()

	vrs := util.FilterMap(box.vrs, func(_ string, vr *voterecords) bool {
		switch {
		case vr.finished():
			return false
		case !last.IsZero() && vr.stagepoint.Compare(last) < 1:
			return false
		default:
			return true
		}
	})

	if len(vrs) < 2 { //nolint:gomnd //...
		return nil, vrs
	}

	sort.Slice(vrs, func(i, j int) bool {
		return vrs[i].stagepoint.Compare(vrs[j].stagepoint) < 0
	})

	lastvr := vrs[len(vrs)-1]
	p := lastvr.stagepoint

	switch {
	case p.Stage() == base.StageACCEPT:
		p = p.SetStage(base.StageINIT)
	case p.Stage() == base.StageINIT:
		p = base.NewStagePoint(base.NewPoint(p.Height()-1, base.Round(0)), base.StageACCEPT)
	}

	var rvrs, nvrs []*voterecords

	for i := range vrs {
		vr := vrs[i]
		stagepoint := vr.stagepoint

		switch {
		case stagepoint.Height() == p.Height() && stagepoint.Compare(p.SetStage(base.StageINIT)) != 0:
			rvrs = append(rvrs, vr)

			continue
		case stagepoint.Compare(p) < 0:
			continue
		}

		nvrs = append(nvrs, vr)
	}

	return rvrs, nvrs
}

func (box *Ballotbox) countHoldeds() {
	vrs := box.sortedVoterecords()

	last, lastIsMajority := box.lastStagePoint()

	for i := range vrs {
		vps := vrs[i].countHolded(box.local, last, lastIsMajority, box.countAfter)

		for i := range vps {
			box.vpch <- vps[i]
		}
	}
}

func (box *Ballotbox) start(ctx context.Context) error {
	ticker := time.NewTicker(box.interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			box.countHoldeds()
		}
	}
}

func (box *Ballotbox) sortedVoterecords() (vrs []*voterecords) {
	box.vrsLock.Lock()
	defer box.vrsLock.Unlock()

	if len(box.vrs) < 1 {
		return nil
	}

	vrs = make([]*voterecords, len(box.vrs))

	var n int

	for i := range box.vrs {
		vrs[n] = box.vrs[i]
		n++
	}

	sort.Slice(vrs, func(i, j int) bool {
		return vrs[i].stagepoint.Compare(vrs[j].stagepoint) < 0
	})

	return vrs
}

type voterecords struct {
	*logging.Logging
	ballots          map[string]base.BallotSignFact
	isValidVoteproof func(base.Voteproof, base.Suffrage) error
	suffrageVote     func(base.SuffrageWithdrawOperation) error
	getSuffragef     isaac.GetSuffrageByBlockHeight
	voted            map[string]base.BallotSignFact
	withdraws        map[string][]base.SuffrageWithdrawOperation
	vps              map[string]base.Voteproof
	log              zerolog.Logger
	countAfter       time.Time
	stagepoint       base.StagePoint
	lastthreshold    base.Threshold
	sync.RWMutex
	f      bool
	isSIGN bool
}

func newVoterecords(
	stagepoint base.StagePoint,
	isValidVoteproof func(base.Voteproof, base.Suffrage) error,
	getSuffragef isaac.GetSuffrageByBlockHeight,
	suffrageVote func(base.SuffrageWithdrawOperation) error,
	isSIGN bool,
	log *zerolog.Logger,
) *voterecords {
	vr := voterecordsPool.Get().(*voterecords) //nolint:forcetypeassert //...

	vr.stagepoint = stagepoint
	vr.isValidVoteproof = isValidVoteproof
	vr.getSuffragef = getSuffragef
	vr.voted = map[string]base.BallotSignFact{}
	vr.f = false
	vr.ballots = map[string]base.BallotSignFact{}
	vr.withdraws = map[string][]base.SuffrageWithdrawOperation{}
	vr.vps = map[string]base.Voteproof{}
	vr.suffrageVote = suffrageVote
	vr.isSIGN = isSIGN

	switch {
	case log == nil:
		vr.log = zerolog.Nop()
	default:
		vr.log = log.With().Interface("point", stagepoint).Logger()
	}

	return vr
}

func (vr *voterecords) vote(
	signfact base.BallotSignFact,
	vp base.Voteproof,
	withdraws []base.SuffrageWithdrawOperation,
) (voted bool, validated bool, err error) {
	vr.Lock()
	defer vr.Unlock()

	node := signfact.Node().String()
	if _, found := vr.vps[node]; found {
		return false, false, nil
	}

	if _, found := vr.ballots[node]; found {
		return false, false, nil
	}

	e := util.StringErrorFunc("failed to vote")

	if vp != nil {
		vr.vps[node] = vp
	}

	if len(withdraws) > 0 {
		vr.withdraws[node] = withdraws

		if err := vr.learnWithdraws(withdraws); err != nil {
			vr.log.Error().Err(err).
				Interface("sign_fact", signfact).
				Interface("withdraws", withdraws).
				Msg("failed learn suffrage withdraws; ignored")
		}
	}

	var suf base.Suffrage

	switch i, found, err := vr.getSuffrage(); {
	case err != nil:
		return false, false, e(err, "")
	case !found:
		vr.ballots[node] = signfact

		return true, false, nil
	default:
		suf = i
	}

	if vp != nil {
		if err := vr.isValidBallotWithSuffrage(signfact, vp, suf); err != nil {
			vr.log.Error().Err(err).
				Interface("sign_fact", signfact).
				Interface("voteproof", vp). //nolint:goconst //...
				Interface("suffrage", suf).
				Msg("invalid ballot")

			return false, false, nil
		}
	}

	if err := vr.learnWithdraws(withdraws); err != nil {
		vr.log.Error().Err(err).
			Interface("sign_fact", signfact).
			Interface("withdraws", withdraws).
			Msg("failed learn suffrage withdraws")

		return true, true, e(err, "")
	}

	vr.voted[node] = signfact

	return true, true, nil
}

func (vr *voterecords) finished() bool {
	vr.RLock()
	defer vr.RUnlock()

	return vr.f
}

func (vr *voterecords) count(
	local base.Address,
	lastStagePoint base.StagePoint,
	lastIsMajority bool,
	threshold base.Threshold,
	countAfter time.Duration,
) []base.Voteproof {
	vr.Lock()
	defer vr.Unlock()

	if len(vr.voted) < 1 && len(vr.ballots) < 1 {
		return nil
	}

	suf, found, err := vr.getSuffrage()
	if err != nil || !found || suf == nil {
		return nil
	}

	var vps []base.Voteproof

	if len(vr.ballots) > 0 {
		if vp := vr.countFromBallots(local, lastStagePoint, lastIsMajority, threshold, suf); vp != nil {
			vr.log.Debug().Interface("voteproof", vp).Msg("new voteproof; count from ballot")

			vps = append(vps, vp)
		}
	}

	if vp := vr.countFromVoted(local, threshold, countAfter); vp != nil {
		vr.log.Debug().Interface("voteproof", vp).Msg("new voteproof; count from voted")

		vr.countAfter = time.Time{}

		vps = append(vps, vp)
	}

	return vps
}

func (vr *voterecords) countFromBallots(
	local base.Address,
	lastStagePoint base.StagePoint,
	lastIsMajority bool,
	threshold base.Threshold,
	suf base.Suffrage,
) base.Voteproof {
	if len(vr.ballots) < 1 {
		return nil
	}

	var vpchecked bool
	var digged base.Voteproof

	for i := range vr.ballots {
		signfact := vr.ballots[i]
		vp := vr.vps[i]

		if vp != nil {
			if err := vr.isValidBallotWithSuffrage(signfact, vp, suf); err != nil {
				delete(vr.ballots, i)

				continue
			}
		}

		if err := vr.learnWithdraws(vr.withdraws[i]); err != nil {
			continue
		}

		vr.voted[signfact.Node().String()] = signfact

		if digged == nil && !vpchecked && vp != nil {
			isnew, overthreshold := isNewBallotVoteproofWithThreshold(
				local, vp, lastStagePoint, lastIsMajority, threshold)
			if isnew && overthreshold {
				digged = vp
			}

			vpchecked = !isnew || overthreshold
		}
	}

	vr.ballots = map[string]base.BallotSignFact{}

	return digged
}

func (vr *voterecords) countHolded(
	local base.Address,
	lastStagePoint base.StagePoint,
	lastIsMajority bool,
	duration time.Duration,
) []base.Voteproof {
	vr.RLock()
	countAfter := vr.countAfter
	vr.RUnlock()

	switch {
	case countAfter.IsZero():
		return nil
	case time.Now().Before(countAfter.Add(duration)):
		return nil
	default:
		return vr.count(local, lastStagePoint, lastIsMajority, vr.lastthreshold, duration)
	}
}

func (vr *voterecords) countFromVoted(
	local base.Address,
	threshold base.Threshold,
	countAfter time.Duration,
) base.Voteproof {
	switch {
	case vr.f: // NOTE if finished, return nil
		return nil
	case len(vr.voted) < 1:
		return nil
	}

	var suf base.Suffrage

	switch i, found, err := vr.getSuffrage(); {
	case err != nil, !found:
		return nil
	default:
		suf = i
	}

	sfs := make([]base.BallotSignFact, len(vr.voted))
	var i int

	for k := range vr.voted {
		sfs[i] = vr.voted[k]
		i++
	}

	var set []string
	var m map[string]base.BallotFact

	switch i, j := base.CountBallotSignFacts(sfs); {
	case len(i) < 1:
		return nil
	default:
		set = i
		m = j
	}

	var withdrawsnotyet bool

	switch found, wsfs, majority, withdraws := vr.countWithWithdraws(local, suf, threshold); {
	case majority == nil:
		withdrawsnotyet = found
	default:
		vr.f = true

		return vr.newVoteproof(wsfs, majority, threshold, withdraws)
	}

	var majority base.BallotFact

	switch result, majoritykey := threshold.VoteResult(uint(suf.Len()), set); result {
	case base.VoteResultDraw:
		// NOTE draw with INIT withdraw, hold voteproof for seconds
		if withdrawsnotyet && vr.stagepoint.Stage() == base.StageINIT {
			if vr.countAfter.IsZero() || time.Now().Before(vr.countAfter.Add(countAfter)) {
				vr.countAfter = time.Now()
				vr.lastthreshold = threshold

				return nil
			}
		}
	case base.VoteResultMajority:
		majority = m[majoritykey]
	default:
		return nil
	}

	vr.f = true

	return vr.newVoteproof(sfs, majority, threshold, nil)
}

func (vr *voterecords) countWithWithdraws(
	local base.Address,
	suf base.Suffrage,
	threshold base.Threshold,
) (
	bool,
	[]base.BallotSignFact,
	base.BallotFact,
	[]base.SuffrageWithdrawOperation,
) {
	sorted := sortBallotSignFactsByWithdraws(local, vr.voted, vr.withdraws)
	if len(sorted) < 1 {
		return false, nil, nil, nil
	}

	for i := range sorted {
		wfacts := sorted[i][0].([]base.SuffrageWithdrawFact)         //nolint:forcetypeassert //...
		wsfs := sorted[i][1].([]base.BallotSignFact)                 //nolint:forcetypeassert //...
		withdraws := sorted[i][2].([]base.SuffrageWithdrawOperation) //nolint:forcetypeassert //...

		set, m := base.CountBallotSignFacts(wsfs)

		newthreshold := threshold
		quorum := uint(suf.Len())

		if uint(len(wfacts)) > quorum-base.DefaultThreshold.Threshold(quorum) {
			newthreshold = base.MaxThreshold
			quorum = uint(suf.Len() - len(wfacts))
		}

		if uint(len(set)) < newthreshold.Threshold(quorum) {
			continue
		}

		var majority base.BallotFact

		switch result, majoritykey := newthreshold.VoteResult(quorum, set); result {
		case base.VoteResultDraw:
		case base.VoteResultMajority:
			majority = m[majoritykey]
		default:
			continue
		}

		return true, wsfs, majority, withdraws
	}

	return true, nil, nil, nil
}

func (vr *voterecords) newVoteproof(
	sfs []base.BallotSignFact,
	majority base.BallotFact,
	threshold base.Threshold,
	withdraws []base.SuffrageWithdrawOperation,
) base.Voteproof {
	switch vr.stagepoint.Stage() {
	case base.StageINIT:
		vp := isaac.NewINITVoteproof(vr.stagepoint.Point)
		_ = vp.
			SetSignFacts(sfs).
			SetMajority(majority).
			SetThreshold(threshold).
			SetWithdraws(withdraws).
			Finish()

		return vp
	case base.StageACCEPT:
		vp := isaac.NewACCEPTVoteproof(vr.stagepoint.Point)
		_ = vp.
			SetSignFacts(sfs).
			SetMajority(majority).
			SetThreshold(threshold).
			SetWithdraws(withdraws).
			Finish()

		return vp
	default:
		panic("unknown stage found to create voteproof")
	}
}

func (vr *voterecords) getSuffrage() (base.Suffrage, bool, error) {
	return vr.getSuffragef(vr.stagepoint.Height())
}

func (vr *voterecords) learnWithdraws(withdraws []base.SuffrageWithdrawOperation) error {
	if vr.suffrageVote == nil {
		return nil
	}

	if len(withdraws) < 1 {
		return nil
	}

	for i := range withdraws {
		if err := vr.suffrageVote(withdraws[i]); err != nil {
			return err
		}
	}

	return nil
}

func (vr *voterecords) isValidBallotWithSuffrage(
	signfact base.BallotSignFact,
	vp base.Voteproof,
	suf base.Suffrage,
) error {
	e := util.ErrInvalid.Errorf("invalid ballot with suffrage")

	fact := signfact.Fact().(base.BallotFact) //nolint:forcetypeassert //...

	if !suf.ExistsPublickey(signfact.Node(), signfact.Signer()) {
		return e.Errorf("node not in suffrage")
	}

	if withdraws := vr.withdraws[signfact.Node().String()]; len(withdraws) > 0 {
		for i := range withdraws {
			if err := isaac.IsValidWithdrawWithSuffrage(fact.Point().Height(), withdraws[i], suf); err != nil {
				return e.Wrap(err)
			}
		}
	}

	if err := isValidVoteproofWithSuffrage(vr.getSuffragef, vr.isValidVoteproof, vp); err != nil {
		return e.Wrap(err)
	}

	return nil
}

var voterecordsPool = sync.Pool{
	New: func() interface{} {
		return new(voterecords)
	},
}

var voterecordsPoolPut = func(vr *voterecords) {
	vr.Lock()
	defer vr.Unlock()

	vr.stagepoint = base.ZeroStagePoint
	vr.isValidVoteproof = nil
	vr.getSuffragef = nil
	vr.voted = nil
	vr.f = false
	vr.ballots = nil
	vr.withdraws = nil
	vr.vps = nil
	vr.log = zerolog.Nop()

	voterecordsPool.Put(vr)
}

func isNewBallotVoteproof( //revive:disable-line:flag-parameter
	vp base.Voteproof,
	last base.StagePoint,
	lastIsMajority bool,
) bool {
	if last.IsZero() {
		return true
	}

	point := vp.Point()

	switch {
	case !lastIsMajority && vp.Result() == base.VoteResultMajority:
		// NOTE when last voteproof is based on SuffrageConfirm ballots, ignore
		// the next ballots, which same height and not ACCEPT stage
		switch {
		case point.Height() < last.Height():
			return false
		case point.Height() > last.Height():
			return true
		case last.Stage() == base.StageINIT && vp.Point().Stage() == base.StageINIT:
			return true
		}
	case lastIsMajority && point.Height() == last.Height() && point.Stage().Compare(last.Stage()) == 0:
		if _, ok := vp.Majority().(isaac.SuffrageConfirmBallotFact); ok {
			return true
		}

		return false
	case lastIsMajority && point.Height() == last.Height() && point.Stage().Compare(last.Stage()) < 1:
		// NOTE when last voteproof is majority, ignore the next ballots, which
		// has same height and not higher stage.
		return false
	}

	return point.Compare(last) >= 0
}

func isNewBallotVoteproofWithThreshold( //revive:disable-line:flag-parameter
	local base.Address,
	vp base.Voteproof,
	last base.StagePoint,
	lastIsMajority bool,
	threshold base.Threshold,
) (isnew bool, overthreshold bool) {
	if w, ok := vp.(isaac.WithdrawVoteproof); ok {
		withdraws := w.Withdraws()

		for i := range withdraws {
			// NOTE if local is in withdraws, ignore voteproof.
			if withdraws[i].WithdrawFact().Node().Equal(local) {
				return false, false
			}
		}
	}

	return isNewBallotVoteproof(
			vp,
			last,
			lastIsMajority,
		),
		vp.Threshold() >= threshold
}

func sortBallotSignFactsByWithdraws(
	local base.Address,
	signfacts map[string]base.BallotSignFact,
	withdraws map[string][]base.SuffrageWithdrawOperation,
) [][3]interface{} {
	sfs := make([]base.BallotSignFact, len(signfacts))

	{
		var n int

		for i := range signfacts {
			sfs[n] = signfacts[i]

			n++
		}
	}

	mw := map[string][3]interface{}{}

	for i := range signfacts {
		signfact := signfacts[i]

		w := withdraws[i]
		if len(w) < 1 {
			continue
		}

		if _, found := mw[signfact.Fact().Hash().String()]; found {
			continue
		}

		m, found := extractWithdrawsFromBallot(local, w, sfs)
		if !found {
			continue
		}

		mw[signfact.Fact().Hash().String()] = m
	}

	if len(mw) < 1 {
		return nil
	}

	ms := make([][3]interface{}, len(mw))

	var n int

	for i := range mw {
		ms[n] = mw[i]

		n++
	}

	sort.Slice(ms, func(i, j int) bool {
		return len(ms[i][0].([]base.SuffrageWithdrawFact)) > //nolint:forcetypeassert //...
			len(ms[j][0].([]base.SuffrageWithdrawFact)) //nolint:forcetypeassert //...
	})

	return ms
}

func extractWithdrawsFromBallot(
	local base.Address,
	withdraws []base.SuffrageWithdrawOperation,
	sfs []base.BallotSignFact,
) (m [3]interface{}, found bool) {
	if len(withdraws) < 1 {
		return m, false
	}

	wfacts := make([]base.SuffrageWithdrawFact, len(withdraws))

	for i := range withdraws {
		fact := withdraws[i].WithdrawFact()
		if fact.Node().Equal(local) { // NOTE if local is in withdraws, ignore
			return m, false
		}

		wfacts[i] = fact
	}

	m[0] = wfacts
	m[1] = util.FilterSlice(sfs, func(j base.BallotSignFact) bool {
		return util.InSliceFunc(wfacts, func(sf base.SuffrageWithdrawFact) bool {
			return sf.Node().Equal(j.Node())
		}) < 0
	})
	m[2] = withdraws

	return m, true
}

func isSuffrageConfirmBallotFact(fact base.Fact) bool {
	_, ok := fact.(isaac.SuffrageConfirmBallotFact)

	return ok
}

func isNewBallotboxStagePoint( //revive:disable-line:flag-parameter
	last, point base.StagePoint,
	lastIsMajority, isSIGN bool,
) bool {
	if last.IsZero() {
		return true
	}

	if isSIGN {
		switch {
		case last.Stage() == base.StageACCEPT:
		case last.Height() == point.Height():
			if lastIsMajority && point.Stage().Compare(last.Stage()) < 0 {
				return false
			}

			return true
		}
	}

	switch c := point.Compare(last); {
	case c < 1:
		return false
	case lastIsMajority && point.Height() == last.Height() && point.Stage().Compare(last.Stage()) < 1:
		// NOTE when last voteproof is majority, ignore the next ballots, which
		// has same height and not higher stage.
		return false
	default:
		return true
	}
}

func isValidVoteproofWithSuffrage(
	getSuffragef isaac.GetSuffrageByBlockHeight,
	isValidVoteproof func(base.Voteproof, base.Suffrage) error,
	vp base.Voteproof,
) error {
	e := util.ErrInvalid.Errorf("invalid voteproof with suffrage")

	var vsuf base.Suffrage

	switch i, found, err := getSuffragef(vp.Point().Height()); {
	case err != nil:
		return e.Wrap(err)
	case !found:
		return e.Errorf("suffrage not found")
	default:
		vsuf = i
	}

	if err := isaac.IsValidVoteproofWithSuffrage(vp, vsuf); err != nil {
		return e.Wrap(err)
	}

	if isValidVoteproof != nil {
		if err := isValidVoteproof(vp, vsuf); err != nil {
			return e.Wrap(err)
		}
	}

	return nil
}
