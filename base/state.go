package base

import (
	"bytes"

	"github.com/spikeekips/mitum/util"
	"github.com/spikeekips/mitum/util/hint"
)

var StateFixedtreeHint = hint.MustNewHint("state-fixedtree-v0.0.1")

type State interface {
	util.Hasher // NOTE <key> + <value HashByte> + <height>
	util.IsValider
	Key() string
	Value() StateValue
	Height() Height          // NOTE manifest height
	Previous() util.Hash     // NOTE previous state hash
	Operations() []util.Hash // NOTE operation fact hash
}

type StateValue interface {
	util.HashByter
	util.IsValider
}

type StateMergeValue interface {
	StateValue
	Key() string
	Value() StateValue
	Merger(Height, State) StateValueMerger
}

type StateValueMerger interface {
	State
	Merge(value StateValue, operation util.Hash) error
	Close() error
}

type GetStateFunc func(key string) (State, bool, error)

func IsEqualState(a, b State) bool {
	switch {
	case a == nil || b == nil:
		return false
	case !a.Hash().Equal(b.Hash()):
		return false
	case a.Key() != b.Key():
		return false
	case a.Height() != b.Height():
		return false
	case !IsEqualStateValue(a.Value(), b.Value()):
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

func IsEqualStateValue(a, b StateValue) bool {
	switch {
	case a == nil || b == nil:
		return false
	case !bytes.Equal(a.HashBytes(), b.HashBytes()):
		return false
	default:
		return true
	}
}
