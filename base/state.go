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
	Height() Height // NOTE manifest height
}

type StateValue interface {
	hint.Hinter
	util.HashByter
	util.IsValider
	Equal(StateValue) bool
	Interface() interface{} // NOTE returns native value
	Set(interface{}) (StateValue, error)
}
