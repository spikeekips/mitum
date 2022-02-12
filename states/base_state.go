package states

import (
	"reflect"

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
	switchStateFunc     func(stateSwitchContext)
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

func (st *baseStateHandler) enter(stateSwitchContext) (func() error, error) {
	return func() error { return nil }, nil
}

func (st *baseStateHandler) exit() (func() error, error) {
	return func() error { return nil }, nil
}

func (st *baseStateHandler) newVoteproof(base.Voteproof) error {
	return nil
}

func (st *baseStateHandler) newProposal(base.ProposalFact) error {
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

	if st.sts != nil {
		go func() {
			st.sts.statech <- nsctx
		}()
	}

	st.switchStateFunc(nsctx)
}

func (st *baseStateHandler) broadcastBallot(bl base.Ballot, tolocal bool) error {
	if st.sts != nil {
		return st.sts.broadcastBallot(bl, tolocal)
	}

	return st.broadcastBallotFunc(bl, tolocal)
}
