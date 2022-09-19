package base

import (
	"github.com/spikeekips/mitum/util"
)

type Seal interface {
	util.IsValider
	util.Hasher
	Signs() Sign
	Body() []SealBody
}

type SealBody interface {
	util.IsValider
	util.HashByter
}
