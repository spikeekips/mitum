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
	local             base.Address
	getSuffragef      isaac.GetSuffrageByBlockHeight
	isValidVoteprooff func(base.Voteproof, base.Suffrage) error
	suffrageVotef     func(base.SuffrageWithdrawOperation) error
	newBallotf        func(base.Ballot)
	vpch              chan base.Voteproof
	vrs               *util.ShardedMap[string, *voterecords]
	lsp               *util.Locked[LastPoint]
	removed           *util.Locked[[]*voterecords]
	countLock         sync.Mutex
	countAfter        time.Duration
	interval          time.Duration
}

func NewBallotbox(
	local base.Address,
	getSuffragef isaac.GetSuffrageByBlockHeight,
) *Ballotbox {
	vrs, _ := util.NewShardedMap("", (*voterecords)(nil), 4) //nolint:gomnd // last 4 stages

	box := &Ballotbox{
		Logging: logging.NewLogging(func(zctx zerolog.Context) zerolog.Context {
			return zctx.Str("module", "ballotbox")
		}),
		local:             local,
		getSuffragef:      getSuffragef,
		vrs:               vrs,
		vpch:              make(chan base.Voteproof, math.MaxUint16),
		lsp:               util.EmptyLocked(LastPoint{}),
		isValidVoteprooff: func(base.Voteproof, base.Suffrage) error { return nil },
		suffrageVotef:     func(base.SuffrageWithdrawOperation) error { return nil },
		newBallotf:        func(base.Ballot) {},
		countAfter:        time.Second * 5, //nolint:gomnd //...
		interval:          time.Second,
		removed:           util.EmptyLocked(([]*voterecords)(nil)),
	}

	box.ContextDaemon = util.NewContextDaemon(box.start)

	return box
}

func (box *Ballotbox) SetIsValidVoteproofFunc(f func(base.Voteproof, base.Suffrage) error) *Ballotbox {
	box.isValidVoteprooff = f

	return box
}

func (box *Ballotbox) SetSuffrageVoteFunc(f func(base.SuffrageWithdrawOperation) error) *Ballotbox {
	box.suffrageVotef = f

	return box
}

func (box *Ballotbox) SetNewBallotFunc(f func(base.Ballot)) *Ballotbox {
	box.newBallotf = f

	return box
}

func (box *Ballotbox) SetInterval(d time.Duration) *Ballotbox {
	box.interval = d

	return box
}

func (box *Ballotbox) SetCountAfter(d time.Duration) *Ballotbox {
	box.countAfter = d

	return box
}

func (box *Ballotbox) Vote(bl base.Ballot, threshold base.Threshold) (bool, error) {
	if !box.checkBallot(bl) {
		return false, nil
	}

	var withdraws []base.SuffrageWithdrawOperation
	if w, ok := bl.(base.HasWithdraws); ok {
		withdraws = w.Withdraws()
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

	if deferred != nil {
		go func() {
			deferred()
		}()
	}

	if voted {
		go func() {
			box.newBallotf(bl)
		}()
	}

	return voted, nil
}

func (box *Ballotbox) VoteSignFact(sf base.BallotSignFact, threshold base.Threshold) (bool, error) {
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
	vrs := box.unfinishedVoterecords()

	var hasvoteproof bool

	for i := range vrs {
		if vps := box.countVoterecords(vrs[i], threshold); len(vps) > 0 {
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
) (vp base.Voteproof, _ error) {
	if len(withdraws) < 1 {
		return vp, errors.Errorf("empty withdraws")
	}

	vr, found := box.voterecords(point, false)
	if !found {
		return vp, nil
	}

	_ = box.countVoterecords(vr, threshold)

	return vr.stuckVoteproof(threshold, withdraws)
}

func (box *Ballotbox) MissingNodes(point base.StagePoint, threshold base.Threshold) ([]base.Address, bool, error) {
	vr, found := box.voterecords(point, false)

	switch {
	case !found:
		return nil, false, nil
	case vr.finishedLocked():
		return nil, true, nil
	default:
		_ = box.countVoterecords(vr, threshold)

		if vr.finishedLocked() {
			return nil, true, nil
		}
	}

	switch suf, found, err := vr.getSuffrage(); {
	case err != nil:
		return nil, false, err
	case !found:
		return nil, false, nil
	default:
		voted := vr.copyVotedLocked(suf)

		filtered := make([]base.Address, suf.Len())
		sufnodes := suf.Nodes()
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

func (box *Ballotbox) checkBallot(bl base.Ballot) bool {
	f := func(w base.HasWithdraws) bool {
		withdraws := w.Withdraws()

		for i := range withdraws {
			// NOTE if local is in withdraws, ignore.
			if withdraws[i].WithdrawFact().Node().Equal(box.local) {
				return false
			}
		}

		return true
	}

	switch suf, found, err := box.getSuffragef(bl.Point().Height()); {
	case err != nil:
		return false
	case !found:
	default:
		if !suf.Exists(bl.SignFact().Node()) {
			return false
		}
	}

	if w, ok := bl.Voteproof().(base.HasWithdraws); ok {
		if !f(w) {
			return false
		}
	}

	if w, ok := bl.(base.HasWithdraws); ok {
		if !f(w) {
			return false
		}
	}

	return true
}

func (box *Ballotbox) unfinishedVoterecords() []*voterecords {
	last := box.LastPoint()

	var vrs []*voterecords

	_ = box.removed.Get(func(removed []*voterecords, isempty bool) error {
		removedm := map[string]struct{}{}

		if len(removed) > 0 {
			for i := range removed {
				removedm[removed[i].sp.String()] = struct{}{}
			}
		}

		box.vrs.Traverse(func(_ string, vr *voterecords) bool {
			if _, found := removedm[vr.stagepoint().String()]; found {
				return true
			}

			switch {
			case vr.finishedLocked():
			case !last.IsZero() && vr.stagepoint().Compare(last.StagePoint) < 1:
			default:
				vrs = append(vrs, vr)
			}

			return true
		})

		if len(vrs) > 1 {
			sort.Slice(vrs, func(i, j int) bool {
				return vrs[i].sp.Compare(vrs[j].sp) < 0
			})
		}

		return nil
	})

	return vrs
}

func (box *Ballotbox) vote(
	signfact base.BallotSignFact,
	vp base.Voteproof,
	withdraws []base.SuffrageWithdrawOperation,
	threshold base.Threshold,
) (bool, func() []base.Voteproof, error) {
	fact := signfact.Fact().(base.BallotFact) //nolint:forcetypeassert //...

	l := box.Log().With().Interface("sign_fact", signfact).Logger()

	if !box.isNewBallot(fact.Point(), isaac.IsSuffrageConfirmBallotFact(fact)) {
		l.Debug().Msg("ballot not voted; old")

		return false, nil, nil
	}

	vr := box.newVoterecords(fact.Point(), isaac.IsSuffrageConfirmBallotFact(fact))

	voted, validated, err := vr.vote(signfact, vp, withdraws, box.LastPoint())
	if err != nil {
		l.Error().Err(err).Msg("ballot not voted")

		return false, nil, err
	}

	l.Debug().Bool("validated", validated).Msg("ballot validated?")

	if !validated {
		var deferred func() []base.Voteproof

		if vp != nil {
			deferred = func() []base.Voteproof {
				last := box.LastPoint()

				if vr.voteproofFromBallotsLocked(vp, last, threshold, isNewVoteproof) {
					box.vpch <- vp

					return []base.Voteproof{vp}
				}

				return nil
			}
		}

		return voted, deferred, nil
	}

	return voted, func() []base.Voteproof {
		return box.countVoterecords(vr, threshold)
	}, nil
}

func (box *Ballotbox) isNewBallot( //revive:disable-line:flag-parameter
	point base.StagePoint,
	isSuffrageConfirm bool,
) bool {
	_, err := box.lsp.Set(func(last LastPoint, isempty bool) (v LastPoint, _ error) {
		if isempty {
			return v, util.ErrLockedSetIgnore.Call()
		}

		if isNewBallot(last, point, isSuffrageConfirm) {
			return v, util.ErrLockedSetIgnore.Call()
		}

		return v, errors.Errorf("old ballot")
	})

	return err == nil
}

func (box *Ballotbox) LastPoint() LastPoint {
	last, _ := box.lsp.Value()

	return last
}

func (box *Ballotbox) SetLastPointFromVoteproof(vp base.Voteproof) bool {
	p, err := newLastPointFromVoteproof(vp)
	if err != nil {
		return false
	}

	return box.SetLastPoint(p)
}

func (box *Ballotbox) SetLastPoint(point LastPoint) bool {
	_, err := box.lsp.Set(func(last LastPoint, isempty bool) (v LastPoint, _ error) {
		if !last.Before(point.StagePoint, point.isSuffrageConfirm) {
			return v, errors.Errorf("old")
		}

		return point, nil
	})

	return err == nil
}

func (box *Ballotbox) voterecords( //revive:disable-line:flag-parameter
	stagepoint base.StagePoint,
	isSuffrageConfirm bool,
) (*voterecords, bool) {
	key := stagepoint.String()
	if isSuffrageConfirm {
		key = "sf-" + key
	}

	return box.vrs.Value(key)
}

func (box *Ballotbox) newVoterecords( //revive:disable-line:flag-parameter
	stagepoint base.StagePoint,
	isSuffrageConfirm bool,
) *voterecords {
	key := stagepoint.String()
	if isSuffrageConfirm {
		key = "sf-" + key
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

func (box *Ballotbox) countVoterecords(vr *voterecords, threshold base.Threshold) []base.Voteproof {
	box.countLock.Lock()
	defer box.countLock.Unlock()

	if !box.isNewBallot(vr.stagepoint(), vr.isSuffrageConfirm()) {
		return nil
	}

	vps := vr.count(box.local, box.LastPoint(), threshold, box.countAfter)
	if len(vps) < 1 {
		return nil
	}

	var filtered []base.Voteproof
	_ = box.lsp.Get(func(last LastPoint, isempty bool) error {
		if isempty {
			filtered = vps

			return nil
		}

		nvps := make([]base.Voteproof, len(vps))

		var n int
		for i := range vps {
			if !isNewVoteproofWithSuffrageConfirmFunc(vr.isSuffrageConfirm())(last, vps[i]) {
				continue
			}

			nvps[n] = vps[i]
			n++
		}

		filtered = nvps[:n]

		return nil
	})

	if len(filtered) > 0 {
		lastvp := filtered[len(filtered)-1]
		box.SetLastPointFromVoteproof(lastvp)

		for i := range filtered {
			box.vpch <- filtered[i]
		}
	}

	box.clean()

	return filtered
}

func (box *Ballotbox) clean() {
	_, _ = box.removed.Set(func(removed []*voterecords, isempty bool) ([]*voterecords, error) {
		if !isempty {
			for i := range removed {
				voterecordsPoolPut(removed[i])
			}

			removed = nil
		}

		last := box.LastPoint()
		stagepoint := last.Decrease().Decrease().Decrease()

		if last.IsZero() || stagepoint.IsZero() {
			return nil, nil
		}

		box.vrs.Traverse(func(_ string, vr *voterecords) bool {
			if vr.stagepoint().Compare(stagepoint) <= 0 {
				removed = append(removed, vr)
			}

			return true
		})

		for i := range removed {
			vr := removed[i]

			key := vr.stagepoint().String()
			if vr.isSuffrageConfirm() {
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
	vrs := box.unfinishedVoterecords()

	last := box.LastPoint()

	for i := range vrs {
		vps := vrs[i].countHolded(box.local, last, box.countAfter)

		for i := range vps {
			box.vpch <- vps[i]
		}
	}
}

func (box *Ballotbox) learnWithdraws(withdraws []base.SuffrageWithdrawOperation) error {
	for i := range withdraws {
		if err := box.suffrageVotef(withdraws[i]); err != nil {
			return err
		}
	}

	return nil
}

func (box *Ballotbox) isValidVoteproof(vp base.Voteproof, suf base.Suffrage) error {
	if err := isaac.IsValidVoteproofWithSuffrage(vp, suf); err != nil {
		return err
	}

	return box.isValidVoteprooff(vp, suf)
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
	sp               base.StagePoint
	lastthreshold    base.Threshold
	sync.RWMutex
	isc bool
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

	vr.Lock()
	defer vr.Unlock()

	vr.sp = stagepoint
	vr.isValidVoteproof = isValidVoteproof
	vr.getSuffragef = getSuffragef
	vr.voted = map[string]base.BallotSignFact{}
	vr.ballots = map[string]base.BallotSignFact{}
	vr.withdraws = map[string][]base.SuffrageWithdrawOperation{}
	vr.vps = map[string]base.Voteproof{}
	vr.learnWithdraws = learnWithdraws
	vr.isc = isSuffrageConfirm
	vr.vp = nil

	switch {
	case log == nil:
		vr.log = zerolog.Nop()
	default:
		vr.log = log.With().Interface("point", stagepoint).Logger()
	}

	return vr
}

func (vr *voterecords) stagepoint() base.StagePoint {
	vr.RLock()
	defer vr.RUnlock()

	return vr.sp
}

func (vr *voterecords) isSuffrageConfirm() bool {
	vr.RLock()
	defer vr.RUnlock()

	return vr.isc
}

func (vr *voterecords) isVoted(node base.Address) bool {
	n := node.String()

	if _, found := vr.vps[n]; found {
		return true
	}

	if _, found := vr.ballots[n]; found {
		return true
	}

	if _, found := vr.voted[n]; found {
		return true
	}

	return false
}

func (vr *voterecords) vote(
	signfact base.BallotSignFact,
	vp base.Voteproof,
	withdraws []base.SuffrageWithdrawOperation,
	last LastPoint,
) (voted bool, validated bool, err error) {
	vr.Lock()
	defer vr.Unlock()

	if vr.sp.IsZero() {
		return false, false, nil
	}

	node := signfact.Node()

	switch {
	case !last.Before(vr.sp, vr.isc):
		return false, false, nil
	case vr.vp != nil:
		return false, false, nil
	case vr.isVoted(node):
		return false, false, nil
	}

	if vp != nil {
		vr.vps[node.String()] = vp
	}

	if len(withdraws) > 0 {
		vr.withdraws[node.String()] = withdraws

		if err := vr.learnWithdraws(withdraws); err != nil {
			vr.log.Error().Err(err).
				Interface("sign_fact", signfact).
				Interface("withdraws", withdraws).
				Msg("failed learn suffrage withdraws; ignored")
		}
	}

	switch _, found, err := vr.getSuffrage(); {
	case err != nil:
		return false, false, errors.WithMessage(err, "failed to vote")
	case !found:
		vr.ballots[node.String()] = signfact

		return true, false, nil
	default:
		vr.voted[node.String()] = signfact

		return true, true, nil
	}
}

func (vr *voterecords) finishedLocked() bool {
	vr.RLock()
	defer vr.RUnlock()

	return vr.finished()
}

func (vr *voterecords) finished() bool {
	return vr.vp != nil
}

func (vr *voterecords) count(
	local base.Address,
	last LastPoint,
	threshold base.Threshold,
	countAfter time.Duration,
) []base.Voteproof {
	vr.Lock()
	defer vr.Unlock()

	if vr.sp.IsZero() {
		return nil
	}

	switch {
	case !last.Before(vr.sp, vr.isc):
		return nil
	case vr.vp != nil:
		return nil
	case len(vr.voted) < 1 && len(vr.ballots) < 1:
		return nil
	}

	var vps []base.Voteproof

	if i := vr.voteproofFromBallot(
		last, threshold,
		isNewVoteproofWithSuffrageConfirmFunc(vr.isc),
	); i != nil {
		vps = append(vps, i)
	}

	suf, found, err := vr.getSuffrage()
	if err != nil || !found || suf == nil {
		return vps
	}

	if len(vr.ballots) > 0 {
		vr.countFromBallots(suf)
	}

	if i := vr.countFromVoted(local, threshold, suf, countAfter); i != nil {
		vr.log.Debug().Interface("voteproofs", i).Msg("new voteproof; count from voted")

		vr.countAfter = time.Time{}

		vps = append(vps, i)
	}

	return vps
}

func (vr *voterecords) countFromBallots(suf base.Suffrage) {
	for i := range vr.ballots {
		signfact := vr.ballots[i]

		if err := vr.isValidBallot(signfact, suf); err != nil {
			delete(vr.ballots, i)

			continue
		}

		vr.voted[signfact.Node().String()] = signfact
	}

	vr.ballots = map[string]base.BallotSignFact{}
}

func (vr *voterecords) countFromVoted(
	local base.Address,
	threshold base.Threshold,
	suf base.Suffrage,
	countAfter time.Duration,
) (vp base.Voteproof) {
	if len(vr.voted) < 1 {
		return vp
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
		if withdrawsnotyet && vr.sp.Stage() == base.StageINIT {
			if vr.countAfter.IsZero() || time.Now().Before(vr.countAfter.Add(countAfter)) {
				vr.countAfter = time.Now()
				vr.lastthreshold = threshold

				return vp
			}
		}
	case base.VoteResultMajority:
		majority = m[majoritykey]
	default:
		return vp
	}

	vr.vp = vr.newVoteproof(sfs, majority, threshold, nil)

	return vr.vp
}

func (vr *voterecords) countHolded(
	local base.Address,
	last LastPoint,
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
		return vr.count(local, last, vr.lastthreshold, duration)
	}
}

func (vr *voterecords) isValidBallot(
	signfact base.BallotSignFact,
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

	return nil
}

func (vr *voterecords) newVoteproof(
	sfs []base.BallotSignFact,
	majority base.BallotFact,
	threshold base.Threshold,
	withdraws []base.SuffrageWithdrawOperation,
) base.Voteproof {
	switch s := vr.sp.Stage(); {
	case s == base.StageINIT && len(withdraws) > 0:
		ivp := isaac.NewINITWithdrawVoteproof(vr.sp.Point)
		_ = ivp.
			SetSignFacts(sfs).
			SetMajority(majority).
			SetThreshold(threshold)
		_ = ivp.SetWithdraws(withdraws)
		_ = ivp.Finish()

		return ivp
	case s == base.StageINIT:
		ivp := isaac.NewINITVoteproof(vr.sp.Point)
		_ = ivp.
			SetSignFacts(sfs).
			SetMajority(majority).
			SetThreshold(threshold).
			Finish()

		return ivp
	case s == base.StageACCEPT && len(withdraws) > 0:
		avp := isaac.NewACCEPTWithdrawVoteproof(vr.sp.Point)
		_ = avp.
			SetSignFacts(sfs).
			SetMajority(majority).
			SetThreshold(threshold)
		_ = avp.SetWithdraws(withdraws)
		_ = avp.Finish()

		return avp
	case s == base.StageACCEPT:
		avp := isaac.NewACCEPTVoteproof(vr.sp.Point)
		_ = avp.
			SetSignFacts(sfs).
			SetMajority(majority).
			SetThreshold(threshold).
			Finish()

		return avp
	default:
		panic("unknown stage found to create voteproof")
	}
}

func (vr *voterecords) getSuffrage() (base.Suffrage, bool, error) {
	return vr.getSuffragef(vr.sp.Height())
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

func (vr *voterecords) copyVotedLocked(suf base.Suffrage) map[string]base.BallotSignFact {
	vr.RLock()
	defer vr.RUnlock()

	return vr.copyVoted(suf)
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
				if err := vr.isValidBallot(signfact, suf); err != nil {
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
) (vp base.Voteproof, _ error) {
	vr.Lock()
	defer vr.Unlock()

	if vr.sp.IsZero() {
		return vp, nil
	}

	switch {
	case vr.vp != nil:
		return vp, nil
	case len(withdraws) < 1:
		return vr.vp, errors.Errorf("empty withdraws")
	case len(vr.voted) < 1 && len(vr.ballots) < 1:
		return vr.vp, nil
	}

	var suf base.Suffrage

	switch i, found, err := vr.getSuffrage(); {
	case err != nil:
		return vp, err
	case !found || i == nil:
		return vp, nil
	default:
		suf = i
	}

	voted := vr.copyVoted(suf)
	if len(voted) < 1 {
		return vp, nil
	}

	for i := range withdraws {
		if err := isaac.IsValidWithdrawWithSuffrage(vr.sp.Height(), withdraws[i], suf); err != nil {
			return vp, err
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
		return vp, nil
	}

	switch i, err := isaac.NewSuffrageWithWithdraws(suf, threshold, withdraws); {
	case err != nil:
		return vp, err
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
		return vp, nil
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
	switch vr.sp.Stage() {
	case base.StageINIT:
		ivp := isaac.NewINITStuckVoteproof(vr.sp.Point)

		_ = ivp.
			SetSignFacts(sfs).
			SetMajority(majority)
		_ = ivp.SetWithdraws(withdraws)
		_ = ivp.Finish()

		return ivp, nil
	case base.StageACCEPT:
		avp := isaac.NewACCEPTStuckVoteproof(vr.sp.Point)
		_ = avp.
			SetSignFacts(sfs).
			SetMajority(majority)
		_ = avp.SetWithdraws(withdraws)
		_ = avp.Finish()

		return avp, nil
	default:
		return nil, errors.Errorf("unknown voteproof stage, %T", vr.sp)
	}
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

// voteproofFromBallot finds voteproof from ballots. If voterecords is suffrage
// confirm, the old majority voteproof will be returns.
func (vr *voterecords) voteproofFromBallot(
	last LastPoint,
	threshold base.Threshold,
	filter func(LastPoint, base.Voteproof) bool,
) base.Voteproof {
	if vr.finished() {
		return nil
	}

	for i := range vr.vps {
		vp := vr.vps[i]

		if vr.voteproofFromBallots(vp, last, threshold, filter) {
			return vp
		}
	}

	return nil
}

func (vr *voterecords) voteproofFromBallotsLocked(
	vp base.Voteproof,
	last LastPoint,
	threshold base.Threshold,
	filter func(LastPoint, base.Voteproof) bool,
) bool {
	vr.RLock()
	defer vr.RUnlock()

	return vr.voteproofFromBallots(vp, last, threshold, filter)
}

func (vr *voterecords) voteproofFromBallots(
	vp base.Voteproof,
	last LastPoint,
	threshold base.Threshold,
	filter func(LastPoint, base.Voteproof) bool,
) bool {
	if vr.finished() {
		return false
	}

	switch {
	case !filter(last, vp):
		return false
	case vp.Threshold() < threshold:
		return false
	}

	if wvp, ok := vp.(base.HasWithdraws); ok {
		_ = vr.learnWithdraws(wvp.Withdraws())
	}

	switch suf, found, err := vr.getSuffragef(vp.Point().Height()); {
	case err != nil:
		return false
	case !found || suf == nil:
		return false
	default:
		if err := vr.isValidVoteproof(vp, suf); err != nil {
			return false
		}

		return true
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

	vr.sp = base.ZeroStagePoint
	vr.isValidVoteproof = nil
	vr.getSuffragef = nil
	vr.voted = nil
	vr.ballots = nil
	vr.withdraws = nil
	vr.vps = nil
	vr.log = zerolog.Nop()

	voterecordsPool.Put(vr)
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

func isNewVoteproofWithSuffrageConfirmFunc(isSuffrageConfirm bool) func(LastPoint, base.Voteproof) bool {
	return func(last LastPoint, vp base.Voteproof) bool {
		switch {
		case isNewVoteproof(last, vp),
			!last.isMajority && isSuffrageConfirm:
			return true
		default:
			return false
		}
	}
}
