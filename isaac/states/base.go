package isaacstates

import (
	"context"
	"fmt"

	"github.com/pkg/errors"
	"github.com/rs/zerolog"
	"github.com/spikeekips/mitum/base"
	"github.com/spikeekips/mitum/isaac"
	"github.com/spikeekips/mitum/util"
	"github.com/spikeekips/mitum/util/logging"
)

type baseHandler struct {
	local base.LocalNode
	ctx   context.Context //nolint:containedctx //...
	*logging.Logging
	params                *isaac.LocalParams
	setLastVoteproofFunc  func(base.Voteproof) bool
	forceSetLastVoteproof func(base.Voteproof) bool
	cancel                func()
	lastVoteproofFunc     func() LastVoteproofs
	sts                   *States
	timers                *util.Timers
	switchStateFunc       func(switchContext) error
	onEmptyMembersf       func()
	stt                   StateType
}

func newBaseHandler(
	state StateType,
	local base.LocalNode,
	params *isaac.LocalParams,
) *baseHandler {
	lvps := NewLastVoteproofsHandler()

	return &baseHandler{
		Logging: logging.NewLogging(func(lctx zerolog.Context) zerolog.Context {
			return lctx.Str("module", fmt.Sprintf("state-handler-%s", state))
		}),
		stt:    state,
		local:  local,
		params: params,
		lastVoteproofFunc: func() LastVoteproofs {
			return lvps.Last()
		},
		setLastVoteproofFunc: func(vp base.Voteproof) bool {
			return lvps.Set(vp)
		},
		forceSetLastVoteproof: func(vp base.Voteproof) bool {
			return lvps.ForceSet(vp)
		},
		onEmptyMembersf: func() {},
	}
}

func (st *baseHandler) new() *baseHandler {
	return &baseHandler{
		Logging:               st.Logging,
		local:                 st.local,
		stt:                   st.stt,
		params:                st.params,
		sts:                   st.sts,
		timers:                st.timers,
		cancel:                func() {},
		lastVoteproofFunc:     st.lastVoteproofFunc,
		setLastVoteproofFunc:  st.setLastVoteproofFunc,
		forceSetLastVoteproof: st.forceSetLastVoteproof,
		switchStateFunc:       st.switchStateFunc,
	}
}

func (st *baseHandler) enter(StateType, switchContext) (func(), error) { //nolint:unparam //...
	st.ctx, st.cancel = context.WithCancel(context.Background())

	return func() {}, nil
}

func (st *baseHandler) exit(switchContext) (func(), error) { //nolint:unparam //...
	st.cancel()

	return func() {}, nil
}

func (*baseHandler) newVoteproof(base.Voteproof) error {
	return nil
}

func (st *baseHandler) onEmptyMembers() {
	st.onEmptyMembersf()
}

func (st *baseHandler) SetOnEmptyMembers(f func()) {
	st.onEmptyMembersf = f
}

func (st *baseHandler) state() StateType {
	return st.stt
}

func (st *baseHandler) lastVoteproofs() LastVoteproofs {
	return st.lastVoteproofFunc()
}

func (st *baseHandler) setLastVoteproof(vp base.Voteproof) bool {
	return st.setLastVoteproofFunc(vp)
}

func (st *baseHandler) switchState(sctx switchContext) {
	l := st.Log().With().Dict("next_state", switchContextLog(sctx)).Logger()

	switch err := st.switchStateFunc(sctx); {
	case err == nil:
		l.Debug().Msg("state switched")
	case errors.Is(err, ErrIgnoreSwithingState):
		l.Error().Err(err).Msg("failed to switch state; ignore")
	case sctx.next() == StateBroken:
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
		return st.sts.MoveState(sctx)
	}

	st.timers = st.sts.timers

	st.lastVoteproofFunc = func() LastVoteproofs {
		return st.sts.lastVoteproof()
	}
	st.setLastVoteproofFunc = func(vp base.Voteproof) bool {
		return st.sts.setLastVoteproof(vp)
	}
}

func (st *baseHandler) setNewVoteproof(vp base.Voteproof) (LastVoteproofs, base.Voteproof, bool) {
	lvps := st.lastVoteproofs()

	if st.sts == nil && !lvps.IsNew(vp) {
		return LastVoteproofs{}, nil, false
	}

	return lvps, vp, st.setLastVoteproof(vp)
}
