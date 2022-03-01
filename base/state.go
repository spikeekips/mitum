package base

import (
	"bytes"
	"sort"

	"github.com/spikeekips/mitum/util"
	"github.com/spikeekips/mitum/util/hint"
)

type State interface {
	hint.Hinter
	util.Hasher // NOTE <key> + <value HashByte> + <height>
	util.IsValider
	Key() string
	Value() StateValue
	Height() Height // NOTE manifest height
	Operations() []util.Hash
}

type StateValue interface {
	hint.Hinter
	util.HashByter
	util.IsValider
	Equal(StateValue) bool
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

		sort.Slice(ao, func(i, j int) bool {
			return bytes.Compare(
				ao[i].Bytes(),
				ao[j].Bytes(),
			) < 0
		})

		sort.Slice(bo, func(i, j int) bool {
			return bytes.Compare(
				bo[i].Bytes(),
				bo[j].Bytes(),
			) < 0
		})

		for i := range ao {
			if !ao[i].Equal(bo[i]) {
				return false
			}
		}

		return true
	}
}
