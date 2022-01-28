//go:build test
// +build test

package states

import (
	"github.com/spikeekips/mitum/base"
)

type dummyStateHandler struct {
	s             StateType
	enterf        func(base.Voteproof) error
	enterdefer    func() error
	exitf         func() error
	exitdefer     func() error
	newVoteprooff func(base.Voteproof) error
}

func newDummyStateHandler(state StateType) *dummyStateHandler {
	return &dummyStateHandler{
		s: state,
	}
}

func (st *dummyStateHandler) state() StateType {
	return st.s
}

func (st *dummyStateHandler) enter(vp base.Voteproof) (func() error, error) {
	if st.enterf == nil {
		return st.enterdefer, nil
	}

	if err := st.enterf(vp); err != nil {
		return nil, err
	}

	return st.enterdefer, nil
}

func (st *dummyStateHandler) exit() (func() error, error) {
	if st.exitf == nil {
		return st.exitdefer, nil
	}

	if err := st.exitf(); err != nil {
		return nil, err
	}

	return st.exitdefer, nil
}

func (st *dummyStateHandler) newVoteproof(vp base.Voteproof) error {
	if st.newVoteprooff == nil {
		return nil
	}

	return st.newVoteprooff(vp)
}

func (st *dummyStateHandler) setEnter(f func(base.Voteproof) error, d func() error) *dummyStateHandler {
	st.enterf = f
	st.enterdefer = d

	return st
}

func (st *dummyStateHandler) setExit(f func() error, d func() error) *dummyStateHandler {
	st.exitf = f
	st.exitdefer = d

	return st
}

func (st *dummyStateHandler) setNewVoteproof(f func(base.Voteproof) error) *dummyStateHandler {
	st.newVoteprooff = f

	return st
}

func (st *dummyStateHandler) newProposal(base.ProposalFact) error {
	return nil
}
