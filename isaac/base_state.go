package isaac

import (
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
	stt                 StateType
	sts                 *States
	ts                  *util.Timers // NOTE only for testing
	lvps                *lastVoteproofs
	switchStateFunc     func(stateSwitchContext) error
	broadcastBallotFunc func(base.Ballot, bool /* tolocal */) error
}

func newBaseStateHandler(state StateType) *baseStateHandler {
	return &baseStateHandler{
		Logging: logging.NewLogging(func(lctx zerolog.Context) zerolog.Context {
			return lctx.Stringer("module", state)
		}),
		stt:  state,
		lvps: newLastVoteproofs(),
	}
}

func (*baseStateHandler) enter(stateSwitchContext) (func() error, error) {
	return func() error { return nil }, nil
}

func (*baseStateHandler) exit() (func() error, error) {
	return func() error { return nil }, nil
}

func (*baseStateHandler) newVoteproof(base.Voteproof) error {
	return nil
}

func (*baseStateHandler) newProposal(base.ProposalFact) error {
	return nil
}

func (st *baseStateHandler) state() StateType {
	return st.stt
}

func (st *baseStateHandler) timers() *util.Timers {
	if st.ts != nil {
		return st.ts
	}

	return st.sts.timers
}

func (st *baseStateHandler) lastVoteproof() base.Voteproof {
	if st.sts != nil {
		return st.sts.lastVoteproof()
	}

	return st.lvps.last()
}

func (st *baseStateHandler) lastINITVoteproof() base.INITVoteproof {
	if st.sts != nil {
		return st.sts.lastINITVoteproof()
	}

	return st.lvps.init()
}

func (st *baseStateHandler) lastINITMajorityVoteproof() base.INITVoteproof {
	if st.sts != nil {
		return st.sts.lastINITMajorityVoteproof()
	}

	return st.lvps.initMajority()
}

func (st *baseStateHandler) lastACCEPTVoteproof() base.ACCEPTVoteproof {
	if st.sts != nil {
		return st.sts.lastACCEPTVoteproof()
	}

	return st.lvps.accept()
}

func (st *baseStateHandler) setLastVoteproof(vp base.Voteproof) bool {
	if st.sts != nil {
		return st.sts.setLastVoteproof(vp)
	}

	return st.lvps.set(vp)
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

	var err error
	if st.switchStateFunc != nil {
		err = st.switchStateFunc(nsctx)
	} else {
		err = st.sts.newState(nsctx)
	}

	switch {
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
	if st.broadcastBallotFunc != nil {
		return st.broadcastBallotFunc(bl, tolocal)
	}

	return st.sts.broadcastBallot(bl, tolocal)
}

type lastVoteproofs struct {
	sync.RWMutex
	ivp  base.INITVoteproof
	avp  base.ACCEPTVoteproof
	mivp base.INITVoteproof
}

func newLastVoteproofs() *lastVoteproofs {
	return &lastVoteproofs{}
}

func (l *lastVoteproofs) last() base.Voteproof {
	l.RLock()
	defer l.RUnlock()

	return l.getLast()
}

func (l *lastVoteproofs) init() base.INITVoteproof {
	l.RLock()
	defer l.RUnlock()

	return l.ivp
}

func (l *lastVoteproofs) initMajority() base.INITVoteproof {
	l.RLock()
	defer l.RUnlock()

	return l.mivp
}

func (l *lastVoteproofs) accept() base.ACCEPTVoteproof {
	l.RLock()
	defer l.RUnlock()

	return l.avp
}

func (l *lastVoteproofs) getLast() base.Voteproof {
	switch {
	case l.ivp == nil:
		return l.avp
	case l.avp == nil:
		return l.ivp
	}

	switch c := l.avp.Point().Point.Compare(l.ivp.Point().Point); {
	case c < 0:
		return l.ivp
	default:
		return l.avp
	}
}

func (l *lastVoteproofs) set(vp base.Voteproof) bool {
	l.Lock()
	defer l.Unlock()

	if lvp := l.getLast(); lvp != nil && vp.Point().Compare(lvp.Point()) < 1 {
		return false
	}

	switch vp.Point().Stage() {
	case base.StageINIT:
		l.ivp = vp.(base.INITVoteproof)

		if vp.Result() == base.VoteResultMajority {
			l.mivp = l.ivp
		}
	case base.StageACCEPT:
		l.avp = vp.(base.ACCEPTVoteproof)
	}

	return true
}
