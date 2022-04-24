package base

import (
	"github.com/spikeekips/mitum/util"
	"github.com/spikeekips/mitum/util/hint"
)

type NetworkPolicy interface {
	hint.Hinter
	util.IsValider
	util.HashByter
	MaxOperationsInProposal() uint64
}

type NodePolicy interface {
	hint.Hinter
	util.IsValider
	NetworkID() NetworkID
	Threshold() Threshold
}
