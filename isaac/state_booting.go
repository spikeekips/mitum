package isaac

import "github.com/spikeekips/mitum/base"

type BootingHandler struct {
	*baseStateHandler
}

func NewBootingHandler(
	local *LocalNode,
	policy Policy,
	getSuffrage func(base.Height) base.Suffrage,
) *BootingHandler {
	return &BootingHandler{
		baseStateHandler: newBaseStateHandler(StateBooting, local, policy, nil, getSuffrage),
	}
}

func (*BootingHandler) enter(stateSwitchContext) error {
	// NOTE find last manifest
	// NOTE find last init and accept voteproof
	// NOTE if ok, moves to joining

	return nil
}

func (*BootingHandler) newVoteproof(base.Voteproof) error {
	// NOTE in booting, do nothing
	return nil
}

func (*BootingHandler) newProposal(base.ProposalFact) error {
	// NOTE in booting, do nothing
	return nil
}

type bootingSwitchContext struct {
	baseStateSwitchContext
}

func newBootingSwitchContext() bootingSwitchContext {
	return bootingSwitchContext{
		baseStateSwitchContext: newBaseStateSwitchContext(StateStopped, StateBooting),
	}
}
