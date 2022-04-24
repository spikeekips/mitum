package base

import (
	"github.com/spikeekips/mitum/util"
	"github.com/spikeekips/mitum/util/hint"
)

type NodePolicy interface {
	hint.Hinter
	util.IsValider
	NetworkID() NetworkID
	Threshold() Threshold
}
