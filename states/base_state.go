package states

import "github.com/spikeekips/mitum/base"

type baseStateHandler struct {
	stt StateType
}

func newBaseStateHandler(state StateType) *baseStateHandler {
	return &baseStateHandler{
		stt: state,
	}
}

func (st *baseStateHandler) state() StateType {
	return st.stt
}

func (st *baseStateHandler) enter(base.Voteproof) error {
	return nil
}

func (st *baseStateHandler) exit() error {
	return nil
}

func (st *baseStateHandler) newVoteproof(base.Voteproof) error {
	return nil
}

func (st *baseStateHandler) newProposal(base.ProposalFact) error {
	return nil
}
