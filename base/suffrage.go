package base

import (
	"github.com/pkg/errors"
	"github.com/spikeekips/mitum/util"
)

type Suffrage interface {
	Exists(Address) bool
	ExistsPublickey(Address, Publickey) bool
	Nodes() []Node
	Len() int
}

type SuffrageStateValue interface {
	StateValue
	Height() Height      // NOTE not manifest height
	Previous() util.Hash // NOTE previous state hash of SuffrageStateValue
	Nodes() []Node
	Suffrage() (Suffrage, error)
}

type SuffrageCandidate interface {
	util.IsValider
	Node
	Start() Height
	Deadline() Height
}

type SuffrageCandidateStateValue interface {
	StateValue
	Nodes() []SuffrageCandidate
}

func InterfaceIsSuffrageState(i interface{}) (State, error) {
	switch st, ok := i.(State); {
	case !ok:
		return nil, errors.Errorf("not suffrage state: %T", i)
	case !IsSuffrageState(st):
		return nil, errors.Errorf("not suffrage state value: %T", st.Value())
	default:
		return st, nil
	}
}

func IsSuffrageState(st State) bool {
	if st.Value() == nil {
		return false
	}

	_, ok := st.Value().(SuffrageStateValue)

	return ok
}
