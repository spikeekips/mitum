package isaac

import (
	"context"
	"fmt"
	"reflect"
	"sync"
	"time"

	"github.com/pkg/errors"
	"github.com/rs/zerolog"
	"github.com/spikeekips/mitum/base"
	"github.com/spikeekips/mitum/util"
	"github.com/spikeekips/mitum/util/logging"
)

type baseStateHandler struct {
	*logging.Logging
	ctx                  context.Context
	cancel               func()
	local                LocalNode
	policy               Policy
	proposalSelector     ProposalSelector
	getSuffrage          func(base.Height) base.Suffrage
	stt                  StateType
	sts                  *States
	timers               *util.Timers // NOTE only for testing
	switchStateFunc      func(stateSwitchContext) error
	broadcastBallotFunc  func(base.Ballot) error
	lastVoteproofFunc    func() lastVoteproofs
	setLastVoteproofFunc func(base.Voteproof) bool
}

func newBaseStateHandler(
	state StateType,
	local LocalNode,
	policy Policy,
	proposalSelector ProposalSelector,
	getSuffrage func(base.Height) base.Suffrage,
) *baseStateHandler {
	lvps := newLastVoteproofs()

	return &baseStateHandler{
		Logging: logging.NewLogging(func(lctx zerolog.Context) zerolog.Context {
			return lctx.Str("module", fmt.Sprintf("state-handler-%s", state))
		}),
		stt:              state,
		local:            local,
		policy:           policy,
		proposalSelector: proposalSelector,
		getSuffrage:      getSuffrage,
		broadcastBallotFunc: func(base.Ballot) error {
			return nil
		},
		lastVoteproofFunc: func() lastVoteproofs {
			return lvps.last()
		},
		setLastVoteproofFunc: func(vp base.Voteproof) bool {
			return lvps.set(vp)
		},
	}
}

func (st *baseStateHandler) enter(stateSwitchContext) (func(), error) {
	st.ctx, st.cancel = context.WithCancel(context.Background())

	return func() {}, nil
}

func (st *baseStateHandler) exit(stateSwitchContext) (func(), error) {
	st.cancel()

	return func() {}, nil
}

func (st *baseStateHandler) newVoteproof(vp base.Voteproof) (lastVoteproofs, base.Voteproof, error) {
	lvps := st.lastVoteproof()

	if st.sts == nil && !lvps.isNew(vp) {
		return lastVoteproofs{}, nil, nil
	}

	_ = st.setLastVoteproof(vp)

	return lvps, vp, nil
}

func (st *baseStateHandler) state() StateType {
	return st.stt
}

func (st *baseStateHandler) lastVoteproof() lastVoteproofs {
	return st.lastVoteproofFunc()
}

func (st *baseStateHandler) setLastVoteproof(vp base.Voteproof) bool {
	return st.setLastVoteproofFunc(vp)
}

func (st *baseStateHandler) switchState(sctx stateSwitchContext) {
	elem := reflect.ValueOf(sctx)
	p := reflect.New(elem.Type())
	p.Elem().Set(elem)

	if i, ok := p.Interface().(interface{ setFrom(StateType) }); ok {
		i.setFrom(st.stt)
	}

	nsctx := p.Elem().Interface().(stateSwitchContext)

	l := st.Log().With().Dict("next_state", stateSwitchContextLog(nsctx)).Logger()

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

func (st *baseStateHandler) setStates(sts *States) {
	st.sts = sts

	st.switchStateFunc = func(sctx stateSwitchContext) error {
		return st.sts.newState(sctx)
	}

	st.broadcastBallotFunc = func(bl base.Ballot) error {
		return st.sts.broadcastBallot(bl)
	}

	st.timers = st.sts.timers

	st.lastVoteproofFunc = func() lastVoteproofs {
		return st.sts.lastVoteproof()
	}
	st.setLastVoteproofFunc = func(vp base.Voteproof) bool {
		return st.sts.setLastVoteproof(vp)
	}
}

func (st *baseStateHandler) broadcastBallot(
	bl base.Ballot,
	tolocal bool,
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
				l.Error().Err(err).Msg("failed to broadcast ballot; timer will be stopped")

				return false, e(err, "")
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

func (st *baseStateHandler) broadcastINITBallot(bl base.Ballot, tolocal bool) error {
	return st.broadcastBallot(bl, tolocal, timerIDBroadcastINITBallot, 0)
}

func (st *baseStateHandler) broadcastACCEPTBallot(bl base.Ballot, tolocal bool, initialWait time.Duration) error {
	return st.broadcastBallot(bl, tolocal, timerIDBroadcastACCEPTBallot, initialWait)
}

func (st *baseStateHandler) isLocalInSuffrage(height base.Height) (bool /* in suffrage */, error) {
	suf := st.getSuffrage(height)
	switch {
	case suf == nil:
		return false, errors.Errorf("empty suffrage")
	case !suf.Exists(st.local.Address()):
		return false, nil
	default:
		return true, nil
	}
}

func (st *baseStateHandler) nextRound(vp base.Voteproof, prevBlock util.Hash) {
	l := st.Log().With().Dict("voteproof", base.VoteproofLog(vp)).Logger()

	point := vp.Point().Point.NextRound()

	l.Debug().Object("point", point).Msg("preparing next round")

	// NOTE find next proposal
	pr, err := st.proposalSelector.Select(st.ctx, point)
	if err != nil {
		l.Error().Err(err).Msg("failed to select proposal")

		go st.switchState(newBrokenSwitchContext(st.stt, err))

		return
	}

	l.Debug().Interface("proposal", pr).Msg("proposal selected")

	e := util.StringErrorFunc("failed to move to next round")

	fact := NewINITBallotFact(
		point,
		prevBlock,
		pr.Fact().Hash(),
	)
	sf := NewINITBallotSignedFact(st.local.Address(), fact)

	if err := sf.Sign(st.local.Privatekey(), st.policy.NetworkID()); err != nil {
		go st.switchState(newBrokenSwitchContext(st.stt, e(err, "failed to make next round init ballot")))

		return
	}

	bl := NewINITBallot(vp, sf)
	if err := st.broadcastINITBallot(bl, true); err != nil {
		go st.switchState(newBrokenSwitchContext(st.stt, e(err, "failed to broadcast next round init ballot")))
	}

	if err := st.timers.StartTimers([]util.TimerID{timerIDBroadcastINITBallot}, true); err != nil {
		l.Error().Err(e(err, "")).Msg("failed to start timers for broadcasting next round init ballot")

		return
	}

	l.Debug().Interface("ballot", bl).Msg("next round init ballot broadcasted")
}

type lastVoteproofsHandler struct {
	sync.RWMutex
	ivp base.INITVoteproof
	avp base.ACCEPTVoteproof
	mvp base.Voteproof
}

func newLastVoteproofs() *lastVoteproofsHandler {
	return &lastVoteproofsHandler{}
}

func (l *lastVoteproofsHandler) last() lastVoteproofs {
	l.RLock()
	defer l.RUnlock()

	return lastVoteproofs{
		ivp: l.ivp,
		avp: l.avp,
		mvp: l.mvp,
	}
}

func (l *lastVoteproofsHandler) isNew(vp base.Voteproof) bool {
	l.RLock()
	defer l.RUnlock()

	if lvp := findLastVoteproofs(l.ivp, l.avp); lvp != nil && vp.Point().Compare(lvp.Point()) < 1 {
		return false
	}

	return true
}

func (l *lastVoteproofsHandler) set(vp base.Voteproof) bool {
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

type lastVoteproofs struct {
	ivp base.INITVoteproof
	avp base.ACCEPTVoteproof
	mvp base.Voteproof
}

func (l lastVoteproofs) cap() base.Voteproof {
	return findLastVoteproofs(l.ivp, l.avp)
}

func (l lastVoteproofs) init() base.INITVoteproof {
	return l.ivp
}

// previousBlockForNextRound finds the previous block hash from last majority
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
func (l lastVoteproofs) previousBlockForNextRound(vp base.Voteproof) util.Hash {
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

func (l lastVoteproofs) accept() base.ACCEPTVoteproof {
	return l.avp
}

func (l lastVoteproofs) isNew(vp base.Voteproof) bool {
	if lvp := l.cap(); lvp != nil && vp.Point().Compare(lvp.Point()) < 1 {
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
