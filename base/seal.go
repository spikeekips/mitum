package base

import (
	"github.com/spikeekips/mitum/util"
)

// FIXME remove

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
