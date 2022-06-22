package base

import (
	"github.com/pkg/errors"
	"github.com/spikeekips/mitum/util"
	"github.com/spikeekips/mitum/util/fixedtree"
)

type Suffrage interface {
	Exists(Address) bool
	ExistsPublickey(Address, Publickey) bool
	Nodes() []Node
	Len() int
}

type SuffrageStateValue interface {
	StateValue
	Height() Height // NOTE not manifest height
	Nodes() []Node
	Suffrage() (Suffrage, error)
}

// FIXME if new suffrage node from candidate, update SuffrageCandidateStateValue

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
	default:
		if _, err := LoadSuffrageState(st); err != nil {
			return nil, err
		}

		return st, nil
	}
}

func IsSuffrageState(st State) bool {
	_, err := LoadSuffrageState(st)

	return err == nil
}

func LoadSuffrageState(st State) (SuffrageStateValue, error) {
	if st == nil || st.Value() == nil {
		return nil, errors.Errorf("empty state")
	}

	j, ok := st.Value().(SuffrageStateValue)
	if !ok {
		return nil, errors.Errorf("expected SuffrageStateValue, but %T", st.Value())
	}

	return j, nil
}

type SuffrageProof interface {
	util.IsValider
	Map() BlockMap
	State() State
	ACCEPTVoteproof() ACCEPTVoteproof
	Proof() fixedtree.Proof
	Suffrage() (Suffrage, error)
	SuffrageHeight() Height
	Prove(previousState State) error
}
