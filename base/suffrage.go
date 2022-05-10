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

// BLOCK if new suffrage node from candidate, update SuffrageCandidateStateValue

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
			return nil, errors.Wrap(err, "")
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

type SuffrageInfo interface {
	util.IsValider
	State() util.Hash // NOTE suffrage state hash
	Height() Height   // NOTE suffrage height
	Suffrage() []Node
	Candidates() []SuffrageCandidate
}
