package isaacstates

import (
	"github.com/spikeekips/mitum/base"
	"github.com/spikeekips/mitum/isaac"
)

type BootingHandler struct {
	*baseHandler
}

func NewBootingHandler(
	local isaac.LocalNode,
	policy isaac.Policy,
	getSuffrage func(base.Height) base.Suffrage,
) *BootingHandler {
	return &BootingHandler{
		baseHandler: newBaseHandler(StateBooting, local, policy, nil, getSuffrage),
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
