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

func InterfaceIsSuffrageNodesState(i interface{}) (State, error) {
	switch st, ok := i.(State); {
	case !ok:
		return nil, errors.Errorf("not suffrage state: %T", i)
	default:
		if _, err := LoadSuffrageNodesStateValue(st); err != nil {
			return nil, err
		}

		return st, nil
	}
}

func LoadNodesFromSuffrageCandidatesState(st State) ([]SuffrageCandidateStateValue, error) {
	switch v := st.Value(); {
	case st == nil:
		return nil, nil
	default:
		i, ok := v.(SuffrageCandidatesStateValue)
		if !ok {
			return nil, errors.Errorf("expected SuffrageCandidatesStateValue, not %T", v)
		}

		return i.Nodes(), nil
	}
}

func IsSuffrageNodesState(st State) bool {
	_, err := LoadSuffrageNodesStateValue(st)

	return err == nil
}

func LoadSuffrageNodesStateValue(st State) (SuffrageNodesStateValue, error) {
	if st == nil || st.Value() == nil {
		return nil, errors.Errorf("empty state")
	}

	j, ok := st.Value().(SuffrageNodesStateValue)
	if !ok {
		return nil, errors.Errorf("expected SuffrageNodesStateValue, but %T", st.Value())
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

type (
	SuffrageCandidateLimiterFunc func(SuffrageCandidateLimiterRule) (SuffrageCandidateLimiter, error)
	SuffrageCandidateLimiter     func() (uint64, error)
)

type SuffrageWithdrawFact interface {
	Fact
	Node() Address
	WithdrawStart() Height // NOTE available from WithdrawStart()
	WithdrawEnd() Height   // NOTE not available at WithdrawEnd() + 1
	Reason() string
}

type SuffrageWithdrawOperation interface {
	Operation
	NodeSignFact
	WithdrawFact() SuffrageWithdrawFact
}

type HasWithdraws interface {
	Withdraws() []SuffrageWithdrawOperation
}
