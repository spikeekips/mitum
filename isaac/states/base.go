package isaacstates

import (
	"context"
	"fmt"
	"reflect"
	"sync"
	"time"

	"github.com/pkg/errors"
	"github.com/rs/zerolog"
	"github.com/spikeekips/mitum/base"
	"github.com/spikeekips/mitum/isaac"
	"github.com/spikeekips/mitum/util"
	"github.com/spikeekips/mitum/util/logging"
)

type baseHandler struct {
	*logging.Logging
	ctx                  context.Context
	cancel               func()
	local                base.LocalNode
	policy               isaac.NodePolicy
	proposalSelector     isaac.ProposalSelector
	stt                  StateType
	sts                  *States
	timers               *util.Timers // NOTE only for testing
	switchStateFunc      func(switchContext) error
	broadcastBallotFunc  func(base.Ballot) error
	lastVoteproofFunc    func() LastVoteproofs
	setLastVoteproofFunc func(base.Voteproof) bool
}

func newBaseHandler(
	state StateType,
	local base.LocalNode,
	policy isaac.NodePolicy,
	proposalSelector isaac.ProposalSelector,
) *baseHandler {
	lvps := NewLastVoteproofs()

	return &baseHandler{
		Logging: logging.NewLogging(func(lctx zerolog.Context) zerolog.Context {
			return lctx.Str("module", fmt.Sprintf("state-handler-%s", state))
		}),
		stt:              state,
		local:            local,
		policy:           policy,
		proposalSelector: proposalSelector,
		broadcastBallotFunc: func(base.Ballot) error {
			return nil
		},
		lastVoteproofFunc: func() LastVoteproofs {
			return lvps.Last()
		},
		setLastVoteproofFunc: func(vp base.Voteproof) bool {
			return lvps.Set(vp)
		},
	}
}

func (st *baseHandler) enter(switchContext) (func(), error) { // nolint:unparam
	st.ctx, st.cancel = context.WithCancel(context.Background())

	return func() {}, nil
}

func (st *baseHandler) exit(switchContext) (func(), error) { // nolint:unparam
	st.cancel()

	return func() {}, nil
}

func (*baseHandler) newVoteproof(base.Voteproof) error {
	return nil
}

func (st *baseHandler) state() StateType {
	return st.stt
}

func (st *baseHandler) lastVoteproof() LastVoteproofs { // BLOCK rename to lastVoteproofs
	return st.lastVoteproofFunc()
}

func (st *baseHandler) setLastVoteproof(vp base.Voteproof) bool {
	return st.setLastVoteproofFunc(vp)
}

func (st *baseHandler) switchState(sctx switchContext) {
	elem := reflect.ValueOf(sctx)
	p := reflect.New(elem.Type())
	p.Elem().Set(elem)

	if i, ok := p.Interface().(interface{ setFrom(StateType) }); ok {
		i.setFrom(st.stt)
	}

	nsctx := p.Elem().Interface().(switchContext)

	l := st.Log().With().Dict("next_state", switchContextLog(nsctx)).Logger()

	switch err := st.switchStateFunc(nsctx); {
	case err == nil:
		l.Debug().Msg("state switched")
	case errors.Is(err, ignoreSwithingStateError):
		l.Error().Err(err).Msg("failed to switch state; ignore")
	case nsctx.next() == StateBroken:
		l.Error().Err(err).Msg("failed to switch state; panic")

		panic(err)
	default:
		l.Error().Err(err).Msg("failed to switch state; moves to broken")

		go st.switchState(newBrokenSwitchContext(st.stt, err))
	}
}

func (st *baseHandler) setStates(sts *States) {
	st.sts = sts

	st.switchStateFunc = func(sctx switchContext) error {
		return st.sts.newState(sctx)
	}

	st.broadcastBallotFunc = func(bl base.Ballot) error {
		return st.sts.broadcastBallot(bl)
	}

	st.timers = st.sts.timers

	st.lastVoteproofFunc = func() LastVoteproofs {
		return st.sts.lastVoteproof()
	}
	st.setLastVoteproofFunc = func(vp base.Voteproof) bool {
		return st.sts.setLastVoteproof(vp)
	}
}

func (st *baseHandler) setNewVoteproof(vp base.Voteproof) (LastVoteproofs, base.Voteproof) {
	lvps := st.lastVoteproof()

	if st.sts == nil && !lvps.IsNew(vp) {
		return LastVoteproofs{}, nil
	}

	_ = st.setLastVoteproof(vp)

	return lvps, vp
}

func (st *baseHandler) broadcastBallot(
	bl base.Ballot,
	timerid util.TimerID,
	initialWait time.Duration,
) error {
	// BLOCK vote ballot to local if tolocal is true

	iw := initialWait
	if iw < 1 {
		iw = time.Nanosecond
	}

	l := st.Log().With().
		Stringer("ballot_hash", bl.SignedFact().Fact().Hash()).
		Dur("initial_wait", iw).
		Logger()
	l.Debug().Interface("ballot", bl).Object("point", bl.Point()).Msg("trying to broadcast ballot")

	e := util.StringErrorFunc("failed to broadcast ballot")

	ct := util.NewContextTimer(
		timerid,
		st.policy.IntervalBroadcastBallot(),
		func(int) (bool, error) {
			if err := st.broadcastBallotFunc(bl); err != nil {
				l.Error().Err(err).Msg("failed to broadcast ballot; keep going")
			}

			return true, nil
		},
	).SetInterval(func(i int, d time.Duration) time.Duration {
		if i < 1 {
			return iw
		}

		return d
	})

	if err := st.timers.SetTimer(ct); err != nil {
		return e(err, "")
	}

	return nil
}

func (st *baseHandler) broadcastINITBallot(bl base.Ballot) error {
	return st.broadcastBallot(bl, timerIDBroadcastINITBallot, 0)
}

func (st *baseHandler) broadcastACCEPTBallot(bl base.Ballot, initialWait time.Duration) error {
	return st.broadcastBallot(bl, timerIDBroadcastACCEPTBallot, initialWait)
}

func (st *baseHandler) prepareNextRound(vp base.Voteproof, prevBlock util.Hash) (base.INITBallot, error) {
	l := st.Log().With().Dict("voteproof", base.VoteproofLog(vp)).Logger()

	point := vp.Point().Point.NextRound()

	l.Debug().Object("point", point).Msg("preparing next round")

	// NOTE find next proposal
	pr, err := st.proposalSelector.Select(st.ctx, point)
	if err != nil {
		l.Error().Err(err).Msg("failed to select proposal")

		return nil, newBrokenSwitchContext(st.stt, err)
	}

	l.Debug().Interface("proposal", pr).Msg("proposal selected")

	e := util.StringErrorFunc("failed to move to next round")

	fact := isaac.NewINITBallotFact(
		point,
		prevBlock,
		pr.Fact().Hash(),
	)
	sf := isaac.NewINITBallotSignedFact(st.local.Address(), fact)

	if err := sf.Sign(st.local.Privatekey(), st.policy.NetworkID()); err != nil {
		return nil, newBrokenSwitchContext(st.stt, e(err, "failed to make next round init ballot"))
	}

	return isaac.NewINITBallot(vp, sf), nil
}

func (st *baseHandler) prepareNextBlock(avp base.ACCEPTVoteproof, suf base.Suffrage) (base.INITBallot, error) {
	e := util.StringErrorFunc("failed to prepare next block")

	point := avp.Point().Point.NextHeight()

	l := st.Log().With().Dict("voteproof", base.VoteproofLog(avp)).Object("point", point).Logger()

	switch ok, err := isInSuffrage(st.local.Address(), suf); {
	case err != nil:
		l.Debug().Interface("height", point.Height()).Msg("empty suffrage of next block; moves to broken state")

		return nil, e(err, "local not in suffrage for next block")
	case !ok:
		l.Debug().
			Interface("height", point.Height()).
			Msg("local is not in suffrage at next block; moves to syncing state")

		return nil, newSyncingSwitchContext(StateConsensus, point.Height())
	}

	// NOTE find next proposal
	pr, err := st.proposalSelector.Select(st.ctx, point)
	switch {
	case err == nil:
	case errors.Is(err, context.Canceled):
		l.Debug().Err(err).Msg("canceled to select proposal; ignore")

		return nil, nil
	default:
		l.Error().Err(err).Msg("failed to select proposal")

		return nil, e(err, "")
	}

	l.Debug().Interface("proposal", pr).Msg("proposal selected")

	// NOTE broadcast next init ballot
	fact := isaac.NewINITBallotFact(
		point,
		avp.BallotMajority().NewBlock(),
		pr.Fact().Hash(),
	)
	sf := isaac.NewINITBallotSignedFact(st.local.Address(), fact)

	if err := sf.Sign(st.local.Privatekey(), st.policy.NetworkID()); err != nil {
		return nil, e(err, "failed to make next init ballot")
	}

	return isaac.NewINITBallot(avp, sf), nil
}

type LastVoteproofsHandler struct {
	sync.RWMutex
	ivp base.INITVoteproof
	avp base.ACCEPTVoteproof
	mvp base.Voteproof
}

func NewLastVoteproofs() *LastVoteproofsHandler { // BLOCK rename to lastVoteproofsHandler
	return &LastVoteproofsHandler{}
}

func (l *LastVoteproofsHandler) Last() LastVoteproofs {
	l.RLock()
	defer l.RUnlock()

	return LastVoteproofs{
		ivp: l.ivp,
		avp: l.avp,
		mvp: l.mvp,
	}
}

func (l *LastVoteproofsHandler) IsNew(vp base.Voteproof) bool {
	l.RLock()
	defer l.RUnlock()

	if lvp := findLastVoteproofs(l.ivp, l.avp); lvp != nil && vp.Point().Compare(lvp.Point()) < 1 {
		return false
	}

	return true
}

func (l *LastVoteproofsHandler) Set(vp base.Voteproof) bool {
	l.Lock()
	defer l.Unlock()

	if lvp := findLastVoteproofs(l.ivp, l.avp); lvp != nil && vp.Point().Compare(lvp.Point()) < 1 {
		return false
	}

	switch vp.Point().Stage() {
	case base.StageINIT:
		l.ivp = vp.(base.INITVoteproof)
	case base.StageACCEPT:
		l.avp = vp.(base.ACCEPTVoteproof)
	}

	if vp.Result() == base.VoteResultMajority {
		l.mvp = vp
	}

	return true
}

type LastVoteproofs struct {
	ivp base.INITVoteproof
	avp base.ACCEPTVoteproof
	mvp base.Voteproof
}

func (l LastVoteproofs) Cap() base.Voteproof {
	return findLastVoteproofs(l.ivp, l.avp)
}

func (l LastVoteproofs) INIT() base.INITVoteproof {
	return l.ivp
}

// PreviousBlockForNextRound finds the previous block hash from last majority
// voteproof.
//
// --------------------------------------
// | m        | v      |   | heights    |
// --------------------------------------
// | init     | init   | X |            |
// | accept   | init   | O | m == v - 1 |
// | init     | accept | O | m == v     |
// | accept   | accept | O | m == v - 1 |
// --------------------------------------
//
// * 'm' is last majority voteproof
// * 'v' is draw voteproof, new incoming voteproof for next round
func (l LastVoteproofs) PreviousBlockForNextRound(vp base.Voteproof) util.Hash {
	switch {
	case l.mvp == nil:
		return nil
	case vp.Result() != base.VoteResultDraw:
		return nil
	}

	switch l.mvp.Point().Stage() {
	case base.StageINIT:
		if l.mvp.Point().Height() != vp.Point().Height() {
			return nil
		}

		return l.mvp.Majority().(base.INITBallotFact).PreviousBlock()
	case base.StageACCEPT:
		if l.mvp.Point().Height() != vp.Point().Height()-1 {
			return nil
		}

		return l.mvp.Majority().(base.ACCEPTBallotFact).NewBlock()
	}

	return nil
}

func (l LastVoteproofs) ACCEPT() base.ACCEPTVoteproof {
	return l.avp
}

func (l LastVoteproofs) IsNew(vp base.Voteproof) bool {
	if lvp := l.Cap(); lvp != nil && vp.Point().Compare(lvp.Point()) < 1 {
		return false
	}

	return true
}

func findLastVoteproofs(ivp, avp base.Voteproof) base.Voteproof {
	switch {
	case ivp == nil:
		return avp
	case avp == nil:
		return ivp
	}

	switch c := avp.Point().Point.Compare(ivp.Point().Point); {
	case c < 0:
		return ivp
	default:
		return avp
	}
}

func isInSuffrage(local base.Address, suf base.Suffrage) (bool, error) {
	switch {
	case suf == nil:
		return false, errors.Errorf("empty suffrage")
	case !suf.Exists(local):
		return false, nil
	default:
		return true, nil
	}
}
