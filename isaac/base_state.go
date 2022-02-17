package isaac

import (
	"fmt"
	"reflect"
	"sync"

	"github.com/pkg/errors"
	"github.com/rs/zerolog"
	"github.com/spikeekips/mitum/base"
	"github.com/spikeekips/mitum/util"
	"github.com/spikeekips/mitum/util/logging"
)

type baseStateHandler struct {
	*logging.Logging
	stt                  StateType
	sts                  *States
	ts                   *util.Timers // NOTE only for testing
	switchStateFunc      func(stateSwitchContext) error
	broadcastBallotFunc  func(base.Ballot, bool /* tolocal */) error
	lastVoteproofFunc    func() lastVoteproofs
	setLastVoteproofFunc func(base.Voteproof) bool
}

func newBaseStateHandler(state StateType) *baseStateHandler {
	lvps := newLastVoteproofs()

	return &baseStateHandler{
		Logging: logging.NewLogging(func(lctx zerolog.Context) zerolog.Context {
			return lctx.Str("module", fmt.Sprintf("state-handler-%s", state))
		}),
		stt: state,
		broadcastBallotFunc: func(base.Ballot, bool) error {
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

func (*baseStateHandler) enter(stateSwitchContext) (func() error, error) {
	return func() error { return nil }, nil
}

func (*baseStateHandler) exit() (func() error, error) {
	return func() error { return nil }, nil
}

func (st *baseStateHandler) newVoteproof(vp base.Voteproof) (lastVoteproofs, base.Voteproof, error) {
	lvps := st.lastVoteproof()

	if st.sts == nil {
		c := lvps.cap()
		l := st.Log().With().
			Dict("voteproof", base.VoteproofLog(vp)).
			Dict("last_voteproof", base.VoteproofLog(c)).
			Logger()

		if vp.Point().Compare(c.Point()) < 1 {
			l.Debug().Msg("new voteproof received, but old; ignore")

			return lastVoteproofs{}, nil, nil
		}
	}

	_ = st.setLastVoteproof(vp)

	return lvps, vp, nil
}

func (*baseStateHandler) newProposal(base.ProposalFact) error {
	return nil
}

func (st *baseStateHandler) state() StateType {
	return st.stt
}

func (st *baseStateHandler) timers() *util.Timers {
	return st.ts
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
	case errors.Is(err, IgnoreSwithingStateError):
		l.Error().Err(err).Msg("failed to switch state; ignore")
	case nsctx.next() == StateBroken:
		l.Error().Err(err).Msg("failed to switch state; panic")

		panic(err)
	default:
		l.Error().Err(err).Msg("failed to switch state; moves to broken")

		go st.switchState(newBrokenSwitchContext(st.stt, err))
	}
}

func (st *baseStateHandler) broadcastBallot(bl base.Ballot, tolocal bool) error {
	return st.broadcastBallotFunc(bl, tolocal)
}

func (st *baseStateHandler) setStates(sts *States) {
	st.sts = sts

	st.switchStateFunc = func(sctx stateSwitchContext) error {
		return st.sts.newState(sctx)
	}

	st.broadcastBallotFunc = func(bl base.Ballot, tolocal bool) error {
		return st.sts.broadcastBallot(bl, tolocal)
	}

	st.ts = st.sts.timers

	st.lastVoteproofFunc = func() lastVoteproofs {
		return st.sts.lastVoteproof()
	}
	st.setLastVoteproofFunc = func(vp base.Voteproof) bool {
		return st.sts.setLastVoteproof(vp)
	}
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

type lastVoteproofs struct { // BLOCK apply
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
