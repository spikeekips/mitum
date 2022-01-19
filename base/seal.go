package base

import (
	"github.com/spikeekips/mitum/util"
	"github.com/spikeekips/mitum/util/hint"
)

type Seal interface {
	hint.Hinter
	util.IsValider
	util.Hasher
	Signed() Signed
	Body() []SealBody
}

type SealBody interface {
	util.IsValider
	util.HashByter
}
