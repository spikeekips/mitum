package base

import (
	"github.com/spikeekips/mitum/util"
	"github.com/spikeekips/mitum/util/hint"
	"github.com/spikeekips/mitum/util/valuehash"
)

type Seal interface {
	hint.Hinter
	util.Byter
	util.IsValider
	Sign
	valuehash.HashGenerator // geneate new hash of seal
	Hash() valuehash.Hash   // hash of seal
}
