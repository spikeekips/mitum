package states

import "github.com/spikeekips/mitum/base"

type baseState struct {
	s             StateType
	enterf        func(base.Voteproof) error
	enterdefer    func() error
	exitf         func() error
	exitdefer     func() error
	newVoteprooff func(base.Voteproof) error
}

func newBaseState(state StateType) *baseState {
	return &baseState{
		s: state,
	}
}

func (st *baseState) state() StateType {
	return st.s
}

func (st *baseState) enter(vp base.Voteproof) (func() error, error) {
	if st.enterf == nil {
		return st.enterdefer, nil
	}

	if err := st.enterf(vp); err != nil {
		return nil, err
	}

	return st.enterdefer, nil
}

func (st *baseState) exit() (func() error, error) {
	if st.exitf == nil {
		return st.exitdefer, nil
	}

	if err := st.exitf(); err != nil {
		return nil, err
	}

	return st.exitdefer, nil
}

func (st *baseState) newVoteproof(vp base.Voteproof) error {
	if st.newVoteprooff == nil {
		return nil
	}

	return st.newVoteprooff(vp)
}
