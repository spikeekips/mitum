package isaacstates

import (
	"github.com/spikeekips/mitum/base"
	"github.com/spikeekips/mitum/isaac"
)

type BootingHandler struct {
	*baseHandler
}

func NewBootingHandler(
	local base.LocalNode,
	policy isaac.NodePolicy,
) *BootingHandler {
	return &BootingHandler{
		baseHandler: newBaseHandler(StateBooting, local, policy, nil),
	}
}

func (*BootingHandler) enter(switchContext) (func(), error) {
	// NOTE find last manifest
	// NOTE load last init, accept voteproof and last majority voteproof
	// NOTE if ok, moves to joining

	return nil, nil
}

func (*BootingHandler) newVoteproof(base.Voteproof) error {
	// NOTE in booting, do nothing
	return nil
}

type bootingSwitchContext struct {
	baseSwitchContext
}

func newBootingSwitchContext() bootingSwitchContext {
	return bootingSwitchContext{
		baseSwitchContext: newBaseSwitchContext(StateStopped, StateBooting),
	}
}
