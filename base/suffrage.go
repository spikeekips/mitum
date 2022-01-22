package base

import (
	"github.com/spikeekips/mitum/util"
	"github.com/spikeekips/mitum/util/hint"
)

type SuffrageInfo interface {
	hint.Hinter
	util.HashByter
	util.Hasher // NOTE hash of suffrage chain
	util.IsValider
	Threshold() Threshold
	Nodes() []Address
}
