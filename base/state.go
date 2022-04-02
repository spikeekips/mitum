package base

import (
	"github.com/spikeekips/mitum/util"
	"github.com/spikeekips/mitum/util/hint"
)

type State interface {
	hint.Hinter
	util.Hasher // NOTE <key> + <value HashByte> + <height>
	util.IsValider
	Key() string
	Value() StateValue
	Height() Height      // NOTE manifest height
	Previous() util.Hash // NOTE previous state hash
	Operations() []util.Hash
	Merger(Height) StateValueMerger
}

type StateValue interface {
	hint.Hinter
	util.HashByter
	util.IsValider
	Equal(StateValue) bool
}

type StateValueMerger interface {
	State
	Merge(value StateValue, operations []util.Hash) error
	Close() error
}

type StatePool interface {
	GetState(key string) (State, bool, error)
	SetStates([]State) error
}

func IsEqualState(a, b State) bool {
	switch {
	case a == nil || b == nil:
		return false
	case a.Hint().Type() != b.Hint().Type():
		return false
	case !a.Hash().Equal(b.Hash()):
		return false
	case a.Key() != b.Key():
		return false
	case a.Height() != b.Height():
		return false
	case !a.Value().Equal(b.Value()):
		return false
	case len(a.Operations()) != len(b.Operations()):
		return false
	default:
		ao := a.Operations()
		bo := b.Operations()

		for i := range ao {
			if !ao[i].Equal(bo[i]) {
				return false
			}
		}

		return true
	}
}
