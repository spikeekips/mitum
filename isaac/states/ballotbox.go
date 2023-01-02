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
	vpch             chan base.Voteproof
	vrs              *util.ShardedMap[string, *voterecords]
	lsp              *util.Locked[[2]interface{}]
	removed          *util.Locked[[]*voterecords]
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
	vrs, _ := util.NewShardedMap("", (*voterecords)(nil), 4) //nolint:gomnd // last 4 stages

	box := &Ballotbox{
		Logging: logging.NewLogging(func(zctx zerolog.Context) zerolog.Context {
			return zctx.Str("module", "ballotbox")
		}),
		local:            local,
		getSuffragef:     getSuffragef,
		vrs:              vrs,
		vpch:             make(chan base.Voteproof, math.MaxUint16),
		lsp:              util.EmptyLocked([2]interface{}{}),
		isValidVoteproof: isValidVoteproof,
		newBallot:        func(base.Ballot) {},
		countAfter:       countAfter,
		interval:         time.Second,
		removed:          util.EmptyLocked(([]*voterecords)(nil)),
	}

	box.ContextDaemon = util.NewContextDaemon(box.start)

	return box
}

func (box *Ballotbox) Vote(bl base.Ballot, threshold base.Threshold) (bool, error) {
	var withdraws []base.SuffrageWithdrawOperation
	if i, ok := bl.(isaac.WithdrawBallot); ok {
		withdraws = i.Withdraws()
	}

	voted, deferred, err := box.vote(
		bl.SignFact(),
		bl.Voteproof(),
		withdraws,
		threshold,
	)
	if err != nil {
		return false, errors.WithMessage(err, "failed to vote")
	}

	box.Log().Debug().
		Interface("ballot", bl).
		Interface("sign_fact", bl.SignFact()).
		Bool("voted", voted).
		Msg("ballot voted")

	go func() {
		if deferred != nil {
			deferred()
		}

		box.newBallot(bl)
	}()

	return voted, nil
}

func (box *Ballotbox) VoteSignFact(sf base.BallotSignFact, threshold base.Threshold) (bool, error) {
	{ // FIXME remove
		fact := sf.Fact().(base.BallotFact) //nolint:forcetypeassert //...
		if fact.Point().Height() == 2 && sf.Node().String() == "no2sas" {
			panic("showme")
		}
	}

	voted, deferred, err := box.vote(sf, nil, nil, threshold)
	if err != nil {
		return false, errors.WithMessage(err, "failed to vote sign fact")
	}

	box.Log().Debug().Interface("sign_fact", sf).Bool("voted", voted).Msg("ballot voted")

	if deferred != nil {
		go deferred()
	}

	return voted, nil
}

func (box *Ballotbox) Count(threshold base.Threshold) bool {
	vrs := box.notFinishedVoterecords()

	var hasvoteproof bool

	for i := range vrs {
		if vp := box.countVoterecords(vrs[i], threshold); vp != nil {
			hasvoteproof = true
		}
	}

	return hasvoteproof
}

func (box *Ballotbox) Voteproof() <-chan base.Voteproof {
	if box == nil {
		return nil
	}

	return box.vpch
}

func (box *Ballotbox) Voted(point base.StagePoint, nodes []base.Address) []base.BallotSignFact {
	vr, found := box.voterecords(point, false)
	if !found {
		return nil
	}

	return vr.ballotSignFacts(nodes)
}

func (box *Ballotbox) StuckVoteproof(
	point base.StagePoint,
	threshold base.Threshold,
	withdraws []base.SuffrageWithdrawOperation,
) (base.Voteproof, error) {
	if len(withdraws) < 1 {
		return nil, errors.Errorf("empty withdraws")
	}

	vr, found := box.voterecords(point, false)
	if !found {
		return nil, nil
	}

	_ = box.countVoterecords(vr, threshold)

	return vr.stuckVoteproof(threshold, withdraws)
}

func (box *Ballotbox) MissingNodes(point base.StagePoint, threshold base.Threshold) ([]base.Address, bool, error) {
	vr, found := box.voterecords(point, false)

	switch {
	case !found:
		return nil, false, nil
	case vr.finished():
		return nil, true, nil
	default:
		_ = box.countVoterecords(vr, threshold)

		if vr.finished() {
			return nil, true, nil
		}
	}

	switch suf, found, err := vr.getSuffrage(); {
	case err != nil:
		return nil, false, err
	case !found:
		return nil, false, nil
	default:
		sufnodes := suf.Nodes()

		filtered := make([]base.Address, suf.Len())

		voted := vr.copyVoted(suf)

		var n int
		_ = util.FilterSlice(sufnodes, func(i base.Node) bool {
			if i.Address().Equal(box.local) {
				return false
			}

			_, found := voted[i.Address().String()]

			if !found {
				filtered[n] = i.Address()
				n++
			}

			return !found
		})

		return filtered[:n], true, nil
	}
}

func (box *Ballotbox) SetSuffrageVote(f func(base.SuffrageWithdrawOperation) error) {
	box.suffrageVote = f
}

func (box *Ballotbox) SetNewBallot(f func(base.Ballot)) {
	box.newBallot = f
}

func (box *Ballotbox) vote(
	signfact base.BallotSignFact,
	vp base.Voteproof,
	withdraws []base.SuffrageWithdrawOperation,
	threshold base.Threshold,
) (bool, func() base.Voteproof, error) {
	fact := signfact.Fact().(base.BallotFact) //nolint:forcetypeassert //...

	l := box.Log().With().Interface("sign_fact", signfact).Logger()

	if !box.isNew(fact.Point(), isSuffrageConfirmBallotFact(fact)) {
		l.Debug().Msg("ballot not voted; old")

		return false, nil, nil
	}

	vr := box.newVoterecords(fact.Point(), isSuffrageConfirmBallotFact(fact))

	voted, validated, err := vr.vote(signfact, vp, withdraws)
	if err != nil {
		l.Error().Err(err).Msg("ballot not voted")

		return false, nil, err
	}

	var digged base.Voteproof

	if voted && vp != nil {
		if !validated {
			l.Debug().Msg("ballot voted, but not validated")

			if err := isValidVoteproofWithSuffrage(box.getSuffragef, box.isValidVoteproof, vp); err == nil {
				validated = true
			}
		}

		if validated {
			l.Debug().Msg("ballot voted and validated")

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
		if !box.setLastStagePoint(digged.Point(), digged.Result() == base.VoteResultMajority) {
			digged = nil
		}

		if wvp, ok := digged.(base.HasWithdrawVoteproof); ok {
			if err := box.learnWithdraws(wvp.Withdraws()); err != nil {
				return false, nil, err
			}
		}
	}

	if digged != nil {
		l.Debug().Msg("voteproof found from ballot")

		box.vpch <- digged
	}

	return voted,
		func() base.Voteproof {
			return box.countVoterecords(vr, threshold)
		},
		nil
}

func (box *Ballotbox) isNew(point base.StagePoint, isSuffrageConfirm bool) bool { //revive:disable-line:flag-parameter
	_, err := box.lsp.Set(func(i [2]interface{}, isempty bool) (v [2]interface{}, _ error) {
		if isempty {
			return v, util.ErrLockedSetIgnore.Call()
		}

		last := i[0].(base.StagePoint) //nolint:forcetypeassert //...
		lastIsMajority := i[1].(bool)  //nolint:forcetypeassert //...

		if isNewBallotboxStagePoint(last, point, lastIsMajority, isSuffrageConfirm) {
			return v, util.ErrLockedSetIgnore.Call()
		}

		return v, errors.Errorf("old ballot")
	})

	return err == nil
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

func (box *Ballotbox) voterecords( //revive:disable-line:flag-parameter
	stagepoint base.StagePoint,
	isSuffrageConfirm bool,
) (*voterecords, bool) {
	key := stagepoint.String()
	if isSuffrageConfirm {
		key = "sign-" + key
	}

	return box.vrs.Value(key)
}

func (box *Ballotbox) newVoterecords( //revive:disable-line:flag-parameter
	stagepoint base.StagePoint,
	isSuffrageConfirm bool,
) *voterecords {
	key := stagepoint.String()
	if isSuffrageConfirm {
		key = "sign-" + key
	}

	vr, _ := box.vrs.Set(key, func(i *voterecords, found bool) (*voterecords, error) {
		if found {
			return i, nil
		}

		return newVoterecords(
			stagepoint,
			box.isValidVoteproof,
			box.getSuffragef,
			box.learnWithdraws,
			isSuffrageConfirm,
			box.Logging.Log(),
		), nil
	})

	return vr
}

func (box *Ballotbox) countVoterecords(vr *voterecords, threshold base.Threshold) base.Voteproof {
	box.countLock.Lock()
	defer box.countLock.Unlock()

	if !box.isNew(vr.stagepoint, vr.isSuffrageConfirm) {
		return nil
	}

	last, lastIsMajority := box.lastStagePoint()

	vps := vr.count(box.local, last, lastIsMajority, threshold, box.countAfter)
	if len(vps) < 1 {
		return nil
	}

	lastvp := vps[len(vps)-1]
	box.setLastStagePoint(lastvp.Point(), lastvp.Result() == base.VoteResultMajority)

	for i := range vps {
		box.vpch <- vps[i]
	}

	box.clean()

	return lastvp
}

func (box *Ballotbox) notFinishedVoterecords() []*voterecords {
	last, _ := box.lastStagePoint()

	var vrs []*voterecords

	box.vrs.Traverse(func(_ string, vr *voterecords) bool {
		switch {
		case vr.finished():
		case !last.IsZero() && vr.stagepoint.Compare(last) < 1:
		default:
			vrs = append(vrs, vr)
		}

		return true
	})

	if len(vrs) < 2 { //nolint:gomnd //...
		return vrs
	}

	sort.Slice(vrs, func(i, j int) bool {
		return vrs[i].stagepoint.Compare(vrs[j].stagepoint) < 0
	})

	return vrs
}

func (box *Ballotbox) clean() {
	_, _ = box.removed.Set(func(removed []*voterecords, isempty bool) ([]*voterecords, error) {
		if !isempty {
			for i := range removed {
				voterecordsPoolPut(removed[i])
			}

			removed = nil
		}

		last, _ := box.lastStagePoint()
		stagepoint := last.Decrease()

		if last.IsZero() || stagepoint.IsZero() {
			return nil, nil
		}

		box.vrs.Traverse(func(_ string, vr *voterecords) bool {
			if vr.stagepoint.Compare(stagepoint) <= 0 {
				removed = append(removed, vr)
			}

			return true
		})

		for i := range removed {
			vr := removed[i]

			key := vr.stagepoint.String()
			if vr.isSuffrageConfirm {
				key = "sign-" + key
			}

			_ = box.vrs.RemoveValue(key)
		}

		return removed, nil
	})
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

func (box *Ballotbox) countHoldeds() {
	vrs := box.notFinishedVoterecords()

	last, lastIsMajority := box.lastStagePoint()

	for i := range vrs {
		vps := vrs[i].countHolded(box.local, last, lastIsMajority, box.countAfter)

		for i := range vps {
			box.vpch <- vps[i]
		}
	}
}

func (box *Ballotbox) learnWithdraws(withdraws []base.SuffrageWithdrawOperation) error {
	if box.suffrageVote == nil {
		return nil
	}

	for i := range withdraws {
		if err := box.suffrageVote(withdraws[i]); err != nil {
			return err
		}
	}

	return nil
}

type voterecords struct {
	*logging.Logging
	ballots          map[string]base.BallotSignFact
	isValidVoteproof func(base.Voteproof, base.Suffrage) error
	learnWithdraws   func([]base.SuffrageWithdrawOperation) error
	getSuffragef     isaac.GetSuffrageByBlockHeight
	voted            map[string]base.BallotSignFact
	withdraws        map[string][]base.SuffrageWithdrawOperation
	vps              map[string]base.Voteproof
	log              zerolog.Logger
	vp               base.Voteproof
	countAfter       time.Time
	stagepoint       base.StagePoint
	lastthreshold    base.Threshold
	sync.RWMutex
	f                 bool
	isSuffrageConfirm bool
}

func newVoterecords(
	stagepoint base.StagePoint,
	isValidVoteproof func(base.Voteproof, base.Suffrage) error,
	getSuffragef isaac.GetSuffrageByBlockHeight,
	learnWithdraws func([]base.SuffrageWithdrawOperation) error,
	isSuffrageConfirm bool,
	log *zerolog.Logger,
) *voterecords {
	vr := voterecordsPool.Get().(*voterecords) //nolint:forcetypeassert //...

	vr.stagepoint = stagepoint
	vr.isValidVoteproof = isValidVoteproof
	vr.getSuffragef = getSuffragef
	vr.voted = map[string]base.BallotSignFact{}
	vr.ballots = map[string]base.BallotSignFact{}
	vr.withdraws = map[string][]base.SuffrageWithdrawOperation{}
	vr.vps = map[string]base.Voteproof{}
	vr.learnWithdraws = learnWithdraws
	vr.isSuffrageConfirm = isSuffrageConfirm
	vr.vp = nil

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

	return vr.vp != nil
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

	sfs, set, m := vr.sfs(vr.voted)

	var withdrawsnotyet bool

	switch found, wsfs, majority, withdraws := vr.countWithWithdraws(local, suf, threshold); {
	case majority == nil:
		withdrawsnotyet = found
	default:
		vr.vp = vr.newVoteproof(wsfs, majority, threshold, withdraws)

		return vr.vp
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

	vr.vp = vr.newVoteproof(sfs, majority, threshold, nil)

	return vr.vp
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
		if len(withdraws) > 0 {
			vp := isaac.NewINITWithdrawVoteproof(vr.stagepoint.Point)
			_ = vp.
				SetSignFacts(sfs).
				SetMajority(majority).
				SetThreshold(threshold)
			_ = vp.SetWithdraws(withdraws)
			_ = vp.Finish()

			return vp
		}

		vp := isaac.NewINITVoteproof(vr.stagepoint.Point)
		_ = vp.
			SetSignFacts(sfs).
			SetMajority(majority).
			SetThreshold(threshold).
			Finish()

		return vp
	case base.StageACCEPT:
		if len(withdraws) > 0 {
			vp := isaac.NewACCEPTWithdrawVoteproof(vr.stagepoint.Point)
			_ = vp.
				SetSignFacts(sfs).
				SetMajority(majority).
				SetThreshold(threshold)
			_ = vp.SetWithdraws(withdraws)
			_ = vp.Finish()

			return vp
		}

		vp := isaac.NewACCEPTVoteproof(vr.stagepoint.Point)
		_ = vp.
			SetSignFacts(sfs).
			SetMajority(majority).
			SetThreshold(threshold).
			Finish()

		return vp
	default:
		panic("unknown stage found to create voteproof")
	}
}

func (vr *voterecords) getSuffrage() (base.Suffrage, bool, error) {
	return vr.getSuffragef(vr.stagepoint.Height())
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

func (*voterecords) sfs(voted map[string]base.BallotSignFact) (
	sfs []base.BallotSignFact, set []string, m map[string]base.BallotFact,
) {
	if len(voted) < 1 {
		return sfs, set, m
	}

	sfs = make([]base.BallotSignFact, len(voted))

	var i int

	for k := range voted {
		sfs[i] = voted[k]
		i++
	}

	set, m = base.CountBallotSignFacts(sfs)

	return sfs, set, m
}

func (vr *voterecords) copyVoted(suf base.Suffrage) map[string]base.BallotSignFact {
	voted := map[string]base.BallotSignFact{}

	for i := range vr.voted {
		if !suf.Exists(vr.voted[i].Node()) {
			continue
		}

		voted[i] = vr.voted[i]
	}

	if len(vr.ballots) > 0 {
		for i := range vr.ballots {
			signfact := vr.ballots[i]
			vp := vr.vps[i]

			if !suf.Exists(signfact.Node()) {
				continue
			}

			if vp != nil {
				if err := vr.isValidBallotWithSuffrage(signfact, vp, suf); err != nil {
					delete(vr.ballots, i)

					continue
				}
			}

			voted[signfact.Node().String()] = signfact
		}

		vr.ballots = map[string]base.BallotSignFact{}
	}

	return voted
}

func (vr *voterecords) stuckVoteproof(
	threshold base.Threshold,
	withdraws []base.SuffrageWithdrawOperation,
) (base.Voteproof, error) {
	vr.Lock()
	defer vr.Unlock()

	switch {
	case vr.vp != nil:
		return nil, nil
	case len(withdraws) < 1:
		return nil, errors.Errorf("empty withdraws")
	case len(vr.voted) < 1 && len(vr.ballots) < 1:
		return nil, nil
	}

	var suf base.Suffrage

	switch i, found, err := vr.getSuffrage(); {
	case err != nil:
		return nil, err
	case !found || i == nil:
		return nil, nil
	default:
		suf = i
	}

	voted := vr.copyVoted(suf)
	if len(voted) < 1 {
		return nil, nil
	}

	for i := range withdraws {
		if err := isaac.IsValidWithdrawWithSuffrage(vr.stagepoint.Height(), withdraws[i], suf); err != nil {
			return nil, err
		}
	}

	filteredvoted := map[string]base.BallotSignFact{}

	_ = util.FilterMap(voted, func(k string, sf base.BallotSignFact) bool {
		if util.InSliceFunc(withdraws, func(i base.SuffrageWithdrawOperation) bool {
			return i.WithdrawFact().Node().Equal(sf.Node())
		}) < 0 {
			filteredvoted[k] = sf
		}

		return false
	})

	if len(filteredvoted) < 1 {
		return nil, nil
	}

	switch i, err := isaac.NewSuffrageWithWithdraws(suf, threshold, withdraws); {
	case err != nil:
		return nil, err
	default:
		suf = i
	}

	sfs, set, m := vr.sfs(filteredvoted)

	var majority base.BallotFact

	switch result, majoritykey := threshold.VoteResult(uint(suf.Len()), set); result {
	case base.VoteResultDraw:
	case base.VoteResultMajority:
		majority = m[majoritykey]
	default:
		return nil, nil
	}

	return vr.newStuckVoteproof(sfs, majority, withdraws)
}

func (vr *voterecords) ballotSignFacts(nodes []base.Address) []base.BallotSignFact {
	vr.RLock()
	defer vr.RUnlock()

	return util.FilterMap(vr.voted, func(node string, sf base.BallotSignFact) bool {
		return util.InSliceFunc(nodes, func(i base.Address) bool {
			return node == i.String()
		}) >= 0
	})
}

func (vr *voterecords) newStuckVoteproof(
	sfs []base.BallotSignFact,
	majority base.BallotFact,
	withdraws []base.SuffrageWithdrawOperation,
) (base.Voteproof, error) {
	switch vr.stagepoint.Stage() {
	case base.StageINIT:
		ivp := isaac.NewINITStuckVoteproof(vr.stagepoint.Point)

		_ = ivp.
			SetSignFacts(sfs).
			SetMajority(majority)
		_ = ivp.SetWithdraws(withdraws)
		_ = ivp.Finish()

		return ivp, nil
	case base.StageACCEPT:
		avp := isaac.NewACCEPTStuckVoteproof(vr.stagepoint.Point)
		_ = avp.
			SetSignFacts(sfs).
			SetMajority(majority)
		_ = avp.SetWithdraws(withdraws)
		_ = avp.Finish()

		return avp, nil
	default:
		return nil, errors.Errorf("unknown voteproof stage, %T", vr.stagepoint)
	}
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
		switch {
		case point.Height() < last.Height():
			return false
		case point.Height() > last.Height():
			return true
		case last.Stage() == base.StageINIT && point.Stage() == base.StageINIT:
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

	return point.Compare(last) > 0
}

func isNewBallotVoteproofWithThreshold( //revive:disable-line:flag-parameter
	local base.Address,
	vp base.Voteproof,
	last base.StagePoint,
	lastIsMajority bool,
	threshold base.Threshold,
) (isnew bool, overthreshold bool) {
	if w, ok := vp.(base.HasWithdrawVoteproof); ok {
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
	lastIsMajority, isSuffrageConfirm bool,
) bool {
	if last.IsZero() {
		return true
	}

	if isSuffrageConfirm {
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
