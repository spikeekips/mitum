package base

import (
	"github.com/spikeekips/mitum/util"
)

type Seal interface {
	util.IsValider
	util.Hasher
	Signed() Signed
	Body() []SealBody
}

type SealBody interface {
	util.IsValider
	util.HashByter
}
