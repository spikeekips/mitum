package base

import "github.com/spikeekips/mitum/util"

type LocalParams interface {
	util.IsValider
	NetworkID() NetworkID
	Threshold() Threshold
}
