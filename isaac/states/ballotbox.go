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
	getSuffrage      isaac.GetSuffrageByBlockHeight
	isValidVoteproof func(base.Voteproof, base.Suffrage) error
	suffrageVote     func(base.SuffrageWithdrawOperation) error
	newBallot        func(base.Ballot)
	vrs              map[string]*voterecords
	vpch             chan base.Voteproof
	lsp              *util.Locked
	removed          []*voterecords
	vrsLock          sync.RWMutex
	countLock        sync.Mutex
	countAfter       time.Duration
	interval         time.Duration
}

func NewBallotbox(
	local base.Address,
	getSuffrage isaac.GetSuffrageByBlockHeight,
	isValidVoteproof func(base.Voteproof, base.Suffrage) error,
	countAfter time.Duration,
) *Ballotbox {
	box := &Ballotbox{
		Logging: logging.NewLogging(func(zctx zerolog.Context) zerolog.Context {
			return zctx.Str("module", "ballotbox")
		}),
		local:            local,
		getSuffrage:      getSuffrage,
		vrs:              map[string]*voterecords{},
		vpch:             make(chan base.Voteproof, math.MaxUint16),
		lsp:              util.EmptyLocked(),
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

	voted, callback, err := box.vote(bl, threshold)
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

func (box *Ballotbox) filterNewBallot(bl base.Ballot) bool {
	last, lastIsMajority := box.lastStagePoint()

	return isNewBallotboxStagePoint(last, bl.Point(), lastIsMajority, isSIGNBallotFact(bl.SignFact().Fact()))
}

func (box *Ballotbox) vote(bl base.Ballot, threshold base.Threshold) (bool, func() base.Voteproof, error) {
	e := util.StringErrorFunc("failed to vote")

	if !box.filterNewBallot(bl) {
		return false, nil, nil
	}

	vr := box.newVoterecords(bl)

	voted, validated, err := vr.vote(bl)
	if err != nil {
		return false, nil, e(err, "")
	}

	box.Log().Debug().Interface("ballot", bl).Bool("voted", voted).Bool("validated", validated).Msg("ballot voted")

	var vp base.Voteproof

	if voted && validated {
		last, lastIsMajority := box.lastStagePoint()
		if isnew, overthreshold := isNewBallotVoteproof(
			box.local,
			bl,
			last,
			lastIsMajority,
			threshold,
		); isnew && overthreshold {
			vp = bl.Voteproof()
		}
	}

	if vp != nil {
		box.setLastStagePoint(vp.Point(), vp.Result() == base.VoteResultMajority)
	}

	return voted,
		func() base.Voteproof {
			if vp != nil {
				box.vpch <- vp
			}

			return box.countWithVoterecords(vr, threshold)
		},
		nil
}

func (box *Ballotbox) newVoterecords(bl base.Ballot) *voterecords {
	stagepoint := bl.Point()

	vrkey := stagepoint.String()
	if isSIGNBallotFact(bl.SignFact().Fact()) {
		vrkey = "sign-" + vrkey
	}

	if vr, found := box.vrs[vrkey]; found {
		return vr
	}

	vr := newVoterecords(
		stagepoint,
		box.isValidVoteproof,
		box.getSuffrage,
		box.suffrageVote,
		isSIGNBallotFact(bl.SignFact().Fact()),
		box.Logging.Log(),
	)

	box.vrs[vrkey] = vr

	return vr
}

func (box *Ballotbox) lastStagePoint() (point base.StagePoint, lastIsMajority bool) {
	i, _ := box.lsp.Value()
	if i == nil {
		return point, false
	}

	j := i.([2]interface{}) //nolint:forcetypeassert //...

	return j[0].(base.StagePoint), j[1].(bool) //nolint:forcetypeassert //...
}

func (box *Ballotbox) setLastStagePoint(p base.StagePoint, lastIsMajority bool) bool {
	_, err := box.lsp.Set(func(_ bool, i interface{}) (interface{}, error) {
		if i == nil {
			return [2]interface{}{p, lastIsMajority}, nil
		}

		j := i.([2]interface{}) //nolint:forcetypeassert //...

		last := j[0].(base.StagePoint) //nolint:forcetypeassert //...

		if p.Compare(last) < 0 {
			return nil, errors.Errorf("not higher")
		}

		return [2]interface{}{p, lastIsMajority}, nil
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

	vrs := util.FilterMap(box.vrs, func(_ interface{}, i string) bool {
		switch vr := box.vrs[i]; {
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
	ballots          map[string]base.Ballot
	isValidVoteproof func(base.Voteproof, base.Suffrage) error
	suffrageVote     func(base.SuffrageWithdrawOperation) error
	getSuffrageFunc  isaac.GetSuffrageByBlockHeight
	nodes            map[string]struct{} // revive:disable-line:nested-structs
	voted            map[string]base.Ballot
	suf              *util.Locked
	vsuf             *util.Locked // NOTE suffrage for voteproof
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
	getSuffrageFunc isaac.GetSuffrageByBlockHeight,
	suffrageVote func(base.SuffrageWithdrawOperation) error,
	isSIGN bool,
	log *zerolog.Logger,
) *voterecords {
	vr := voterecordsPool.Get().(*voterecords) //nolint:forcetypeassert //...

	vr.stagepoint = stagepoint
	vr.isValidVoteproof = isValidVoteproof
	vr.getSuffrageFunc = getSuffrageFunc
	vr.voted = map[string]base.Ballot{}
	vr.nodes = map[string]struct{}{}
	vr.f = false
	vr.ballots = map[string]base.Ballot{}
	vr.suf = util.EmptyLocked()
	vr.vsuf = util.EmptyLocked()
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

func (vr *voterecords) vote(bl base.Ballot) (voted bool, validated bool, err error) {
	vr.Lock()
	defer vr.Unlock()

	node := bl.SignFact().Node().String()
	if _, found := vr.nodes[node]; found {
		return false, false, nil
	}

	if _, found := vr.ballots[node]; found {
		return false, false, nil
	}

	e := util.StringErrorFunc("failed to vote")

	var suf base.Suffrage

	switch i, found, err := vr.getSuffrage(); {
	case err != nil:
		return false, false, e(err, "")
	case !found:
		vr.ballots[node] = bl

		return true, false, nil
	default:
		suf = i
	}

	if err := vr.isValidBallotWithSuffrage(bl, suf); err != nil {
		vr.log.Error().Err(err).Interface("ballot", bl).Interface("suffrage", suf).Msg("invalid ballot")

		return false, false, nil
	}

	if err := vr.learnWithdraws(bl); err != nil {
		vr.log.Error().Err(err).Interface("ballot", bl).Msg("failed suffrage voting")

		return true, true, err
	}

	vr.voted[node] = bl
	vr.nodes[node] = struct{}{}

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

	var digged base.Voteproof
	var vpchecked bool

	if len(vr.ballots) > 0 {
		for i := range vr.ballots {
			bl := vr.ballots[i]

			if err := vr.isValidBallotWithSuffrage(bl, suf); err != nil {
				delete(vr.ballots, i)

				continue
			}

			if err := vr.learnWithdraws(bl); err != nil {
				continue
			}

			vr.voted[bl.SignFact().Node().String()] = bl

			if !vpchecked {
				isnew, overthreshold := isNewBallotVoteproof(local, bl, lastStagePoint, lastIsMajority, threshold)
				if isnew && overthreshold {
					digged = bl.Voteproof()
				}

				vpchecked = !isnew || overthreshold
			}
		}

		vr.ballots = map[string]base.Ballot{}
	}

	var vps []base.Voteproof
	if digged != nil {
		vps = append(vps, digged)
	}

	if vp := vr.countFromBallots(local, threshold, countAfter); vp != nil {
		vr.countAfter = time.Time{}

		vps = append(vps, vp)
	}

	return vps
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

func (vr *voterecords) countFromBallots(
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
		sfs[i] = vr.voted[k].SignFact()
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
	sorted := sortBallotSignFactsByWithdraws(local, vr.voted)
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

func (vr *voterecords) getSuffrage() (base.Suffrage, bool, error) { // FIXME move to ballotbox
	i, err := vr.suf.Set(func(_ bool, i interface{}) (interface{}, error) {
		if i != nil {
			return i, nil
		}

		switch j, found, err := vr.getSuffrageFunc(vr.stagepoint.Height()); {
		case err != nil:
			return nil, err
		case !found:
			return nil, util.ErrNotFound.Call()
		default:
			vr.log.Debug().Interface("suffrage", j).Msg("suffrage found")

			return j, nil
		}
	})

	switch {
	case err == nil:
		return i.(base.Suffrage), true, nil //nolint:forcetypeassert //...
	case errors.Is(err, util.ErrNotFound):
		return nil, false, nil
	default:
		return nil, false, err
	}
}

func (vr *voterecords) getVoteproofSuffrage() (base.Suffrage, bool, error) {
	i, err := vr.vsuf.Set(func(_ bool, i interface{}) (interface{}, error) {
		if i != nil {
			return i, nil
		}

		var height base.Height

		switch {
		case vr.stagepoint.Stage() == base.StageACCEPT,
			vr.stagepoint.Round() > 0:
			height = vr.stagepoint.Height()
		default:
			height = vr.stagepoint.Height().Prev()
		}

		switch j, found, err := vr.getSuffrageFunc(height); {
		case err != nil:
			return nil, err
		case !found:
			return nil, util.ErrNotFound.Call()
		default:
			return j, nil
		}
	})

	switch {
	case err == nil:
		return i.(base.Suffrage), true, nil //nolint:forcetypeassert //...
	case errors.Is(err, util.ErrNotFound):
		return nil, false, nil
	default:
		return nil, false, err
	}
}

func (vr *voterecords) learnWithdraws(bl base.Ballot) error {
	if vr.suffrageVote == nil {
		return nil
	}

	bw, ok := bl.(isaac.BallotWithdraws)
	if !ok {
		return nil
	}

	w := bw.Withdraws()

	for i := range w {
		if err := vr.suffrageVote(w[i]); err != nil {
			return err
		}
	}

	return nil
}

func (vr *voterecords) isValidBallotWithSuffrage(bl base.Ballot, suf base.Suffrage) error {
	e := util.ErrInvalid.Errorf("invalid ballot with suffrage")

	var vsuf base.Suffrage

	switch i, found, err := vr.getVoteproofSuffrage(); {
	case err != nil:
		return e.Wrap(err)
	case !found:
		return e.Errorf("voteproof suffrage not found")
	default:
		vsuf = i
	}

	if !suf.ExistsPublickey(bl.SignFact().Node(), bl.SignFact().Signer()) {
		return e.Errorf("node not in suffrage")
	}

	if wbl, ok := bl.(isaac.BallotWithdraws); ok {
		withdraws := wbl.Withdraws()

		for i := range withdraws {
			if err := isaac.IsValidWithdrawWithSuffrage(bl.Point().Height(), withdraws[i], suf); err != nil {
				return e.Wrap(err)
			}
		}
	}

	if err := isaac.IsValidVoteproofWithSuffrage(bl.Voteproof(), vsuf); err != nil {
		return e.Wrap(err)
	}

	if vr.isValidVoteproof != nil {
		if err := vr.isValidVoteproof(bl.Voteproof(), vsuf); err != nil {
			return e.Wrap(err)
		}
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
	vr.getSuffrageFunc = nil
	vr.voted = nil
	vr.nodes = nil
	vr.f = false
	vr.ballots = nil
	vr.suf = nil
	vr.vsuf = nil
	vr.log = zerolog.Nop()

	voterecordsPool.Put(vr)
}

func isNewBallotVoteproof( //revive:disable-line:flag-parameter
	local base.Address,
	bl base.Ballot,
	last base.StagePoint,
	lastIsMajority bool,
	threshold base.Threshold,
) (isnew bool, overthreshold bool) {
	vp := bl.Voteproof()

	if w, ok := vp.(isaac.WithdrawVoteproof); ok {
		withdraws := w.Withdraws()

		for i := range withdraws {
			// NOTE if local is in withdraws, ignore voteproof.
			if withdraws[i].WithdrawFact().Node().Equal(local) {
				return false, false
			}
		}
	}

	overthreshold = vp.Threshold() >= threshold

	if last.IsZero() {
		return true, overthreshold
	}

	point := vp.Point()

	if !lastIsMajority {
		// NOTE when last voteproof is based on SIGN ballots, ignore the next
		// ballots, which same height and not ACCEPT stage

		switch {
		case point.Height() < last.Height():
			return false, overthreshold
		case point.Height() > last.Height():
			return true, overthreshold
		case isSIGNBallotFact(bl.SignFact().Fact()):
			return true, overthreshold
		}
	} else if point.Height() == last.Height() && point.Stage().Compare(last.Stage()) < 1 {
		// NOTE when last voteproof is majority, ignore the next ballots, which
		// has same height and not higher stage.
		return false, overthreshold
	}

	return point.Compare(last) >= 0, overthreshold
}

func sortBallotSignFactsByWithdraws(
	local base.Address,
	ballots map[string]base.Ballot,
) [][3]interface{} {
	sfs := make([]base.BallotSignFact, len(ballots))

	{
		var n int

		for i := range ballots {
			sfs[n] = ballots[i].SignFact()

			n++
		}
	}

	mw := map[string][3]interface{}{}

	for i := range ballots {
		bl := ballots[i]

		if _, ok := bl.(isaac.BallotWithdraws); !ok {
			continue
		}

		if _, found := mw[bl.SignFact().Fact().Hash().String()]; found {
			continue
		}

		m, found := extractWithdrawsFromBallot(local, bl, sfs)
		if !found {
			continue
		}

		mw[bl.SignFact().Fact().Hash().String()] = m
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
	bl base.Ballot,
	sfs []base.BallotSignFact,
) (m [3]interface{}, found bool) {
	var withdraws []base.SuffrageWithdrawOperation

	switch w, ok := bl.(isaac.BallotWithdraws); {
	case !ok:
		return m, false
	default:
		withdraws = w.Withdraws()
	}

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
	m[1] = util.FilterSlice(sfs, func(_ interface{}, j int) bool {
		return util.InSliceFunc(wfacts, func(_ interface{}, k int) bool {
			return wfacts[k].Node().Equal(sfs[j].Node())
		}) < 0
	})
	m[2] = withdraws

	return m, true
}

func isSIGNBallotFact(fact base.Fact) bool {
	_, ok := fact.(isaac.SIGNBallotFact)

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
		// NOTE when last voteproof is majority, ignore the next ballots, which has
		// same height and not higher stage.
		return false
	default:
		return true
	}
}
