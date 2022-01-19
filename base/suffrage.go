package base

import "github.com/spikeekips/mitum/util"

type SuffrageInfo interface {
	util.HashByter
	util.Hasher // NOTE hash of suffrage chain
	util.IsValider
	Threshold() Threshold
	Nodes() []Address
}
