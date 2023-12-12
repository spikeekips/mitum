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

type SuffrageNodeStateValue interface {
	Node
	Start() Height
}

type SuffrageNodesStateValue interface {
	StateValue
	Height() Height // NOTE not manifest height
	Nodes() []SuffrageNodeStateValue
	Suffrage() (Suffrage, error)
}

type SuffrageCandidateStateValue interface {
	Node
	Start() Height
	Deadline() Height
}

type SuffrageCandidatesStateValue interface {
	StateValue
	Nodes() []SuffrageCandidateStateValue
}

func InterfaceIsSuffrageNodesState(i interface{}) (st State, _ error) {
	if err := util.SetInterfaceValue(i, &st); err != nil {
		return nil, err
	}

	if _, err := LoadSuffrageNodesStateValue(st); err != nil {
		return nil, err
	}

	return st, nil
}

func LoadNodesFromSuffrageCandidatesState(st State) ([]SuffrageCandidateStateValue, error) {
	switch v := st.Value(); {
	case st == nil:
		return nil, nil
	default:
		switch suf, err := util.AssertInterfaceValue[SuffrageCandidatesStateValue](v); {
		case err != nil:
			return nil, err
		default:
			return suf.Nodes(), nil
		}
	}
}

func IsSuffrageNodesState(st State) bool {
	_, err := LoadSuffrageNodesStateValue(st)

	return err == nil
}

func LoadSuffrageNodesStateValue(st State) (v SuffrageNodesStateValue, _ error) {
	if st == nil || st.Value() == nil {
		return nil, errors.Errorf("empty state")
	}

	if err := util.SetInterfaceValue(st.Value(), &v); err != nil {
		return nil, err
	}

	return v, nil
}

type SuffrageProof interface {
	util.IsValider
	Map() BlockMap
	State() State
	Proof() fixedtree.Proof
	Suffrage() (Suffrage, error)
	SuffrageHeight() Height
	Prove(previousState State) error
}

type (
	SuffrageCandidateLimiterFunc func(SuffrageCandidateLimiterRule) (SuffrageCandidateLimiter, error)
	SuffrageCandidateLimiter     func() (uint64, error)
)

type SuffrageExpelFact interface {
	Fact
	Node() Address
	ExpelStart() Height // NOTE available from ExpelStart()
	ExpelEnd() Height   // NOTE not available at ExpelEnd() + 1
	Reason() string
}

type SuffrageExpelOperation interface {
	Operation
	NodeSignFact
	ExpelFact() SuffrageExpelFact
}

type HasExpels interface {
	Expels() []SuffrageExpelOperation
}
