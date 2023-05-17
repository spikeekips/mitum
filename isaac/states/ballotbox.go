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
	params            base.LocalParams
	getSuffrage       isaac.GetSuffrageByBlockHeight
	isValidVoteprooff func(base.Voteproof, base.Suffrage) error
	suffrageVotef     func(base.SuffrageExpelOperation) error
	newBallotf        func(base.Ballot)
	vpch              chan base.Voteproof
	vrs               *util.ShardedMap[string, *voterecords]
	lsp               *util.Locked[isaac.LastPoint]
	removed           *util.Locked[[]*voterecords]
	countLock         sync.Mutex
	countAfter        time.Duration
	interval          time.Duration
}

func NewBallotbox(
	local base.Address,
	params base.LocalParams,
	getSuffrage isaac.GetSuffrageByBlockHeight,
) *Ballotbox {
	vrs, _ := util.NewShardedMap[string, *voterecords](4) //nolint:gomnd // last 4 stages

	box := &Ballotbox{
		Logging: logging.NewLogging(func(zctx zerolog.Context) zerolog.Context {
			return zctx.Str("module", "ballotbox")
		}),
		local:             local,
		params:            params,
		getSuffrage:       getSuffrage,
		vrs:               vrs,
		vpch:              make(chan base.Voteproof, math.MaxUint16),
		lsp:               util.EmptyLocked[isaac.LastPoint](),
		isValidVoteprooff: func(base.Voteproof, base.Suffrage) error { return nil },
		suffrageVotef:     func(base.SuffrageExpelOperation) error { return nil },
		newBallotf:        func(base.Ballot) {},
		countAfter:        time.Second * 5, //nolint:gomnd //...
		interval:          time.Second,
		removed:           util.EmptyLocked[[]*voterecords](),
	}

	box.ContextDaemon = util.NewContextDaemon(box.start)

	return box
}

func (box *Ballotbox) SetIsValidVoteproofFunc(f func(base.Voteproof, base.Suffrage) error) *Ballotbox {
	box.isValidVoteprooff = f

	return box
}

func (box *Ballotbox) SetSuffrageVoteFunc(f func(base.SuffrageExpelOperation) error) *Ballotbox {
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

func (box *Ballotbox) Vote(bl base.Ballot) (bool, error) {
	if !box.checkBallot(bl) {
		return false, nil
	}

	var expels []base.SuffrageExpelOperation
	if w, ok := bl.(base.HasExpels); ok {
		expels = w.Expels()
	}

	voted, deferred, err := box.vote(
		bl.SignFact(),
		bl.Voteproof(),
		expels,
	)
	if err != nil {
		return false, errors.WithMessage(err, "vote")
	}

	if voted {
		box.Log().Debug().
			Interface("ballot", bl).
			Interface("sign_fact", bl.SignFact()).
			Bool("voted", voted).
			Msg("ballot voted")
	}

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

func (box *Ballotbox) VoteSignFact(sf base.BallotSignFact) (bool, error) {
	voted, deferred, err := box.vote(sf, nil, nil)
	if err != nil {
		return false, errors.WithMessage(err, "vote sign fact")
	}

	if voted {
		box.Log().Debug().Interface("sign_fact", sf).Bool("voted", voted).Msg("ballot voted")
	}

	if deferred != nil {
		go deferred()
	}

	return voted, nil
}

func (box *Ballotbox) Count() bool {
	vrs := box.unfinishedVoterecords()

	var hasvoteproof bool

	for i := range vrs {
		if vps := box.countVoterecords(vrs[i]); len(vps) > 0 {
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
	expels []base.SuffrageExpelOperation,
) (vp base.Voteproof, _ error) {
	if len(expels) < 1 {
		return vp, errors.Errorf("empty expels")
	}

	vr, found := box.voterecords(point, false)
	if !found {
		return vp, nil
	}

	_ = box.countVoterecords(vr)

	return vr.stuckVoteproof(box.params.Threshold(), expels)
}

func (box *Ballotbox) MissingNodes(point base.StagePoint) ([]base.Address, bool, error) {
	vr, found := box.voterecords(point, false)

	switch {
	case !found:
		return nil, false, nil
	case vr.isFinishedLocked():
		return nil, true, nil
	default:
		_ = box.countVoterecords(vr)

		if vr.isFinishedLocked() {
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
	f := func(w base.HasExpels) bool {
		expels := w.Expels()

		for i := range expels {
			// NOTE if local is in expels, ignore.
			if expels[i].ExpelFact().Node().Equal(box.local) {
				return false
			}
		}

		return true
	}

	switch suf, found, err := box.getSuffrage(bl.Point().Height().SafePrev()); {
	case err != nil:
		return false
	case !found:
	default:
		if !suf.Exists(bl.SignFact().Node()) {
			return false
		}
	}

	if w, ok := bl.Voteproof().(base.HasExpels); ok {
		if !f(w) {
			return false
		}
	}

	if w, ok := bl.(base.HasExpels); ok {
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
			case vr.isFinishedLocked():
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
	expels []base.SuffrageExpelOperation,
) (bool, func() []base.Voteproof, error) {
	fact := signfact.Fact().(base.BallotFact) //nolint:forcetypeassert //...

	l := box.Log().With().Interface("sign_fact", signfact).Logger()

	if !box.isNewBallot(fact.Point(), isaac.IsSuffrageConfirmBallotFact(fact)) {
		l.Debug().Msg("ballot not voted; old")

		return false, nil, nil
	}

	vr := box.newVoterecords(fact.Point(), isaac.IsSuffrageConfirmBallotFact(fact))

	voted, validated, err := vr.vote(signfact, vp, expels, box.LastPoint())
	if err != nil {
		l.Error().Err(err).Msg("ballot not voted")

		return false, nil, err
	}

	if voted {
		l.Debug().Bool("validated", validated).Msg("ballot validated?")
	}

	if !validated {
		var deferred func() []base.Voteproof

		if vp != nil {
			deferred = func() []base.Voteproof {
				last := box.LastPoint()

				if vr.voteproofFromBallotsLocked(vp, last, box.params.Threshold(), isaac.IsNewVoteproof) {
					box.vpch <- vp

					return []base.Voteproof{vp}
				}

				return nil
			}
		}

		return voted, deferred, nil
	}

	return voted, func() []base.Voteproof {
		return box.countVoterecords(vr)
	}, nil
}

func (box *Ballotbox) isNewBallot( //revive:disable-line:flag-parameter
	point base.StagePoint,
	isSuffrageConfirm bool,
) bool {
	_, err := box.lsp.Set(func(last isaac.LastPoint, isempty bool) (v isaac.LastPoint, _ error) {
		if isempty {
			return v, util.ErrLockedSetIgnore.WithStack()
		}

		if isaac.IsNewBallot(last, point, isSuffrageConfirm) {
			return v, util.ErrLockedSetIgnore.WithStack()
		}

		return v, errors.Errorf("old ballot")
	})

	return err == nil
}

func (box *Ballotbox) LastPoint() isaac.LastPoint {
	last, _ := box.lsp.Value()

	return last
}

func (box *Ballotbox) SetLastPointFromVoteproof(vp base.Voteproof) bool {
	p, err := isaac.NewLastPointFromVoteproof(vp)
	if err != nil {
		return false
	}

	return box.SetLastPoint(p)
}

func (box *Ballotbox) SetLastPoint(point isaac.LastPoint) bool {
	_, err := box.lsp.Set(func(last isaac.LastPoint, isempty bool) (v isaac.LastPoint, _ error) {
		if !last.Before(point.StagePoint, point.IsSuffrageConfirm()) {
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
			box.getSuffrage,
			box.learnExpels,
			isSuffrageConfirm,
			box.Logging.Log(),
		), nil
	})

	return vr
}

func (box *Ballotbox) countVoterecords(vr *voterecords) []base.Voteproof {
	box.countLock.Lock()
	defer box.countLock.Unlock()

	if !box.isNewBallot(vr.stagepoint(), vr.isSuffrageConfirm()) {
		return nil
	}

	vps := vr.count(box.local, box.LastPoint(), box.params.Threshold(), box.countAfter)
	if len(vps) < 1 {
		return nil
	}

	var filtered []base.Voteproof
	_ = box.lsp.Get(func(last isaac.LastPoint, isempty bool) error {
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
			return errors.WithStack(ctx.Err())
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

func (box *Ballotbox) learnExpels(expels []base.SuffrageExpelOperation) error {
	for i := range expels {
		if err := box.suffrageVotef(expels[i]); err != nil {
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
	learnExpels      func([]base.SuffrageExpelOperation) error
	getSuffrageFunc  isaac.GetSuffrageByBlockHeight
	voted            map[string]base.BallotSignFact
	expels           map[string][]base.SuffrageExpelOperation
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
	getSuffrageFunc isaac.GetSuffrageByBlockHeight,
	learnExpels func([]base.SuffrageExpelOperation) error,
	isSuffrageConfirm bool,
	log *zerolog.Logger,
) *voterecords {
	vr := voterecordsPool.Get().(*voterecords) //nolint:forcetypeassert //...

	vr.Lock()
	defer vr.Unlock()

	vr.sp = stagepoint
	vr.isValidVoteproof = isValidVoteproof
	vr.getSuffrageFunc = getSuffrageFunc
	vr.voted = map[string]base.BallotSignFact{}
	vr.ballots = map[string]base.BallotSignFact{}
	vr.expels = map[string][]base.SuffrageExpelOperation{}
	vr.vps = map[string]base.Voteproof{}
	vr.learnExpels = learnExpels
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
	expels []base.SuffrageExpelOperation,
	last isaac.LastPoint,
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

	if len(expels) > 0 {
		vr.expels[node.String()] = expels

		if err := vr.learnExpels(expels); err != nil {
			vr.log.Error().Err(err).
				Interface("sign_fact", signfact).
				Interface("expels", expels).
				Msg("failed learn suffrage expels; ignored")
		}
	}

	switch _, found, err := vr.getSuffrage(); {
	case err != nil:
		return false, false, errors.WithMessage(err, "vote")
	case !found:
		vr.ballots[node.String()] = signfact

		return true, false, nil
	default:
		vr.voted[node.String()] = signfact

		return true, true, nil
	}
}

func (vr *voterecords) isFinishedLocked() bool {
	vr.RLock()
	defer vr.RUnlock()

	return vr.isFinished()
}

func (vr *voterecords) isFinished() bool {
	return vr.vp != nil
}

func (vr *voterecords) count(
	local base.Address,
	last isaac.LastPoint,
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
		vr.log.Debug().Interface("voteproof", i).Msg("new voteproof; count from voted")

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

	var expelsnotyet bool

	switch found, wsfs, majority, expels := vr.countWithExpels(local, suf, threshold); {
	case majority == nil:
		expelsnotyet = found
	default:
		vr.vp = vr.newVoteproof(wsfs, majority, threshold, expels)

		return vr.vp
	}

	var majority base.BallotFact

	switch result, majoritykey := threshold.VoteResult(uint(suf.Len()), set); result {
	case base.VoteResultDraw:
		// NOTE draw with INIT expel, hold voteproof for seconds
		if expelsnotyet && vr.sp.Stage() == base.StageINIT {
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
	last isaac.LastPoint,
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

	if expels := vr.expels[signfact.Node().String()]; len(expels) > 0 {
		for i := range expels {
			if err := isaac.IsValidExpelWithSuffrage(fact.Point().Height(), expels[i], suf); err != nil {
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
	expels []base.SuffrageExpelOperation,
) base.Voteproof {
	switch s := vr.sp.Stage(); {
	case s == base.StageINIT && len(expels) > 0:
		ivp := isaac.NewINITExpelVoteproof(vr.sp.Point)
		_ = ivp.
			SetSignFacts(sfs).
			SetMajority(majority).
			SetThreshold(threshold)
		_ = ivp.SetExpels(expels)
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
	case s == base.StageACCEPT && len(expels) > 0:
		avp := isaac.NewACCEPTExpelVoteproof(vr.sp.Point)
		_ = avp.
			SetSignFacts(sfs).
			SetMajority(majority).
			SetThreshold(threshold)
		_ = avp.SetExpels(expels)
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
	return vr.getSuffrageFunc(vr.sp.Height().SafePrev())
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
	expels []base.SuffrageExpelOperation,
) (vp base.Voteproof, _ error) {
	vr.Lock()
	defer vr.Unlock()

	if vr.sp.IsZero() {
		return vp, nil
	}

	switch {
	case vr.vp != nil:
		return vp, nil
	case len(expels) < 1:
		return vr.vp, errors.Errorf("empty expels")
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

	for i := range expels {
		if err := isaac.IsValidExpelWithSuffrage(vr.sp.Height(), expels[i], suf); err != nil {
			return vp, err
		}
	}

	filteredvoted := map[string]base.BallotSignFact{}

	_ = util.FilterMap(voted, func(k string, sf base.BallotSignFact) bool {
		if util.InSliceFunc(expels, func(i base.SuffrageExpelOperation) bool {
			return i.ExpelFact().Node().Equal(sf.Node())
		}) < 0 {
			filteredvoted[k] = sf
		}

		return false
	})

	if len(filteredvoted) < 1 {
		return vp, nil
	}

	switch i, err := isaac.NewSuffrageWithExpels(suf, threshold, expels); {
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

	return vr.newStuckVoteproof(sfs, majority, expels)
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
	expels []base.SuffrageExpelOperation,
) (base.Voteproof, error) {
	switch vr.sp.Stage() {
	case base.StageINIT:
		ivp := isaac.NewINITStuckVoteproof(vr.sp.Point)

		_ = ivp.
			SetSignFacts(sfs).
			SetMajority(majority)
		_ = ivp.SetExpels(expels)
		_ = ivp.Finish()

		return ivp, nil
	case base.StageACCEPT:
		avp := isaac.NewACCEPTStuckVoteproof(vr.sp.Point)
		_ = avp.
			SetSignFacts(sfs).
			SetMajority(majority)
		_ = avp.SetExpels(expels)
		_ = avp.Finish()

		return avp, nil
	default:
		return nil, errors.Errorf("unknown voteproof stage, %T", vr.sp)
	}
}

func (vr *voterecords) countWithExpels(
	local base.Address,
	suf base.Suffrage,
	threshold base.Threshold,
) (
	bool,
	[]base.BallotSignFact,
	base.BallotFact,
	[]base.SuffrageExpelOperation,
) {
	sorted := sortBallotSignFactsByExpels(local, vr.voted, vr.expels)
	if len(sorted) < 1 {
		return false, nil, nil, nil
	}

	for i := range sorted {
		wfacts := sorted[i][0].([]base.SuffrageExpelFact)      //nolint:forcetypeassert //...
		wsfs := sorted[i][1].([]base.BallotSignFact)           //nolint:forcetypeassert //...
		expels := sorted[i][2].([]base.SuffrageExpelOperation) //nolint:forcetypeassert //...

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

		vr.log.Debug().
			Interface("point", vr.sp).
			Uint("quorum", quorum).
			Interface("threshold", threshold).
			Interface("set", set).
			Interface("majority", majority).
			Msg("expel counted")

		return true, wsfs, majority, expels
	}

	return true, nil, nil, nil
}

// voteproofFromBallot finds voteproof from ballots. If voterecords is suffrage
// confirm, the old majority voteproof will be returns.
func (vr *voterecords) voteproofFromBallot(
	last isaac.LastPoint,
	threshold base.Threshold,
	filter func(isaac.LastPoint, base.Voteproof) bool,
) base.Voteproof {
	if vr.isFinished() {
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
	last isaac.LastPoint,
	threshold base.Threshold,
	filter func(isaac.LastPoint, base.Voteproof) bool,
) bool {
	vr.RLock()
	defer vr.RUnlock()

	return vr.voteproofFromBallots(vp, last, threshold, filter)
}

func (vr *voterecords) voteproofFromBallots(
	vp base.Voteproof,
	last isaac.LastPoint,
	threshold base.Threshold,
	filter func(isaac.LastPoint, base.Voteproof) bool,
) bool {
	if vr.isFinished() {
		return false
	}

	switch {
	case !filter(last, vp):
		return false
	case vp.Threshold() < threshold:
		return false
	}

	if wvp, ok := vp.(base.HasExpels); ok {
		_ = vr.learnExpels(wvp.Expels())
	}

	switch suf, found, err := vr.getSuffrageFunc(vp.Point().Height().SafePrev()); {
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
	vr.getSuffrageFunc = nil
	vr.voted = nil
	vr.ballots = nil
	vr.expels = nil
	vr.vps = nil
	vr.log = zerolog.Nop()

	voterecordsPool.Put(vr)
}

func sortBallotSignFactsByExpels(
	local base.Address,
	signfacts map[string]base.BallotSignFact,
	expels map[string][]base.SuffrageExpelOperation,
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

		w := expels[i]
		if len(w) < 1 {
			continue
		}

		if _, found := mw[signfact.Fact().Hash().String()]; found {
			continue
		}

		m, found := extractExpelsFromBallot(local, w, sfs)
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
		return len(ms[i][0].([]base.SuffrageExpelFact)) > //nolint:forcetypeassert //...
			len(ms[j][0].([]base.SuffrageExpelFact)) //nolint:forcetypeassert //...
	})

	return ms
}

func extractExpelsFromBallot(
	local base.Address,
	expels []base.SuffrageExpelOperation,
	sfs []base.BallotSignFact,
) (m [3]interface{}, found bool) {
	if len(expels) < 1 {
		return m, false
	}

	wfacts := make([]base.SuffrageExpelFact, len(expels))

	for i := range expels {
		fact := expels[i].ExpelFact()
		if fact.Node().Equal(local) { // NOTE if local is in expels, ignore
			return m, false
		}

		wfacts[i] = fact
	}

	m[0] = wfacts
	m[1] = util.FilterSlice(sfs, func(j base.BallotSignFact) bool {
		return util.InSliceFunc(wfacts, func(sf base.SuffrageExpelFact) bool {
			return sf.Node().Equal(j.Node())
		}) < 0
	})
	m[2] = expels

	return m, true
}

func isNewVoteproofWithSuffrageConfirmFunc(isSuffrageConfirm bool) func(isaac.LastPoint, base.Voteproof) bool {
	return func(last isaac.LastPoint, vp base.Voteproof) bool {
		switch {
		case isaac.IsNewVoteproof(last, vp), isSuffrageConfirm && !last.IsMajority():
			return true
		default:
			return false
		}
	}
}
