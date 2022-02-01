package states

import "github.com/spikeekips/mitum/base"

type BootingHandler struct {
	*baseStateHandler
}

func NewBootingHandler() *BootingHandler {
	return &BootingHandler{
		baseStateHandler: newBaseStateHandler(StateBooting),
	}
}

func (st *BootingHandler) enter(base.Voteproof) error {
	// NOTE find last manifest
	// NOTE find last init and accept voteproof
	// NOTE if ok, moves to joining

	return nil
}

func (st *BootingHandler) newVoteproof(base.Voteproof) error {
	// NOTE in booting, do nothing
	return nil
}

func (st *BootingHandler) newProposal(base.ProposalFact) error {
	// NOTE in booting, do nothing
	return nil
}
