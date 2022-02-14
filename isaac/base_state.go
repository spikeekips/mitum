package isaac

import (
	"reflect"

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
	switchStateFunc     func(stateSwitchContext) error
	broadcastBallotFunc func(base.Ballot, bool /* tolocal */) error
}

func newBaseStateHandler(state StateType) *baseStateHandler {
	return &baseStateHandler{
		Logging: logging.NewLogging(func(lctx zerolog.Context) zerolog.Context {
			return lctx.Stringer("module", state)
		}),
		stt: state,
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

func (st *baseStateHandler) switchState(sctx stateSwitchContext) {
	elem := reflect.ValueOf(sctx)
	p := reflect.New(elem.Type())
	p.Elem().Set(elem)

	if i, ok := p.Interface().(interface{ setFrom(StateType) }); ok {
		i.setFrom(st.stt)
	}

	nsctx := p.Elem().Interface().(stateSwitchContext)

	var err error
	if st.switchStateFunc != nil {
		err = st.switchStateFunc(nsctx)
	} else {
		err = st.sts.newState(nsctx)
	}

	l := st.Log().With().Dict("next_state", stateSwitchContextLog(sctx)).Logger()

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
