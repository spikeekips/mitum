//go:build test
// +build test

package states

import "github.com/spikeekips/mitum/base"

func (st *baseState) setEnter(f func(base.Voteproof) error, d func() error) *baseState {
	st.enterf = f
	st.enterdefer = d

	return st
}

func (st *baseState) setExit(f func() error, d func() error) *baseState {
	st.exitf = f
	st.exitdefer = d

	return st
}

func (st *baseState) setNewVoteproof(f func(base.Voteproof) error) *baseState {
	st.newVoteprooff = f

	return st
}
