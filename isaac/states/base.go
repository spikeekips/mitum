package isaacstates

import (
	"context"
	"fmt"

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
	params                 *isaac.LocalParams
	voteproofsFunc         func(base.StagePoint) (isaac.LastVoteproofs, bool)
	lastVoteproofFunc      func() isaac.LastVoteproofs
	setLastVoteproofFunc   func(base.Voteproof) bool
	forceSetLastVoteproof  func(base.Voteproof) bool
	cancel                 func()
	sts                    *States
	timers                 *util.SimpleTimers
	allowedConsensusLocked *util.Locked[bool]
	switchStateFunc        func(switchContext) error
	handoverXBrokerFunc    func() *HandoverXBroker
	handoverYBrokerFunc    func() *HandoverYBroker
	stt                    StateType
}

func newBaseHandlerType(
	state StateType,
	local base.LocalNode,
	params *isaac.LocalParams,
) *baseHandler {
	lvps := isaac.NewLastVoteproofsHandler()

	return &baseHandler{
		Logging: logging.NewLogging(func(lctx zerolog.Context) zerolog.Context {
			return lctx.Str("module", fmt.Sprintf("state-handler-%s", state))
		}),
		stt:    state,
		local:  local,
		params: params,
		voteproofsFunc: func(point base.StagePoint) (isaac.LastVoteproofs, bool) {
			return lvps.Voteproofs(point)
		},
		lastVoteproofFunc: func() isaac.LastVoteproofs {
			return lvps.Last()
		},
		setLastVoteproofFunc: func(vp base.Voteproof) bool {
			return lvps.Set(vp)
		},
		forceSetLastVoteproof: func(vp base.Voteproof) bool {
			return lvps.ForceSet(vp)
		},
	}
}

func (st *baseHandler) new() *baseHandler {
	return &baseHandler{
		Logging:                st.Logging,
		local:                  st.local,
		stt:                    st.stt,
		params:                 st.params,
		sts:                    st.sts,
		timers:                 st.timers,
		cancel:                 func() {},
		voteproofsFunc:         st.voteproofsFunc,
		lastVoteproofFunc:      st.lastVoteproofFunc,
		setLastVoteproofFunc:   st.setLastVoteproofFunc,
		forceSetLastVoteproof:  st.forceSetLastVoteproof,
		switchStateFunc:        st.switchStateFunc,
		allowedConsensusLocked: util.NewLocked(true),
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

func (st *baseHandler) state() StateType {
	return st.stt
}

func (st *baseHandler) voteproofs(point base.StagePoint) (isaac.LastVoteproofs, bool) {
	return st.voteproofsFunc(point)
}

func (st *baseHandler) lastVoteproofs() isaac.LastVoteproofs {
	return st.lastVoteproofFunc()
}

func (st *baseHandler) setLastVoteproof(vp base.Voteproof) bool {
	return st.setLastVoteproofFunc(vp)
}

func (st *baseHandler) switchState(sctx switchContext) {
	if err := st.switchStateFunc(sctx); err != nil {
		panic(err)
	}
}

func (st *baseHandler) setStates(sts *States) {
	st.sts = sts

	st.switchStateFunc = func(sctx switchContext) error {
		return st.sts.AskMoveState(sctx)
	}

	st.timers = st.sts.timers

	st.voteproofsFunc = func(point base.StagePoint) (isaac.LastVoteproofs, bool) {
		return st.sts.voteproofs(point)
	}
	st.lastVoteproofFunc = func() isaac.LastVoteproofs {
		return st.sts.lastVoteproof()
	}
	st.setLastVoteproofFunc = func(vp base.Voteproof) bool {
		return st.sts.setLastVoteproof(vp)
	}
}

func (st *baseHandler) setNewVoteproof(vp base.Voteproof) (isaac.LastVoteproofs, base.Voteproof, bool) {
	lvps := st.lastVoteproofs()

	if st.sts == nil && !lvps.IsNew(vp) {
		return isaac.LastVoteproofs{}, nil, false
	}

	return lvps, vp, st.setLastVoteproof(vp)
}

func (st *baseHandler) allowedConsensus() bool {
	if st.sts == nil {
		i, _ := st.allowedConsensusLocked.Value()

		return i
	}

	return st.sts.AllowedConsensus()
}

func (st *baseHandler) setAllowConsensus(allow bool) bool {
	if st.sts == nil {
		var changed bool

		_, _ = st.allowedConsensusLocked.Set(func(prev bool, _ bool) (bool, error) {
			changed = prev != allow

			return allow, nil
		})

		st.whenSetAllowConsensus(allow)

		return changed
	}

	return st.sts.setAllowConsensus(allow)
}

func (*baseHandler) whenSetAllowConsensus(bool) {}

func (st *baseHandler) handoverXBroker() *HandoverXBroker {
	if st.handoverXBrokerFunc != nil {
		v := st.handoverXBrokerFunc()

		switch {
		case v == nil:
		default:
			if err := v.isCanceled(); err != nil {
				return nil
			}
		}

		return v
	}

	if st.sts == nil {
		return nil
	}

	return st.sts.HandoverXBroker()
}

func (st *baseHandler) handoverYBroker() *HandoverYBroker {
	if st.handoverYBrokerFunc != nil {
		v := st.handoverYBrokerFunc()

		switch {
		case v == nil:
		default:
			if err := v.isCanceled(); err != nil {
				return nil
			}
		}

		return v
	}

	if st.sts == nil {
		return nil
	}

	return st.sts.HandoverYBroker()
}

func (st *baseHandler) cleanHandovers() {
	if st.sts == nil {
		return
	}

	st.sts.cleanHandovers()
}
