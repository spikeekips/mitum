package isaacstates

import (
	"github.com/spikeekips/mitum/base"
	"github.com/spikeekips/mitum/isaac"
)

type StoppedHandler struct {
	*baseHandler
}

func NewStoppedHandler(
	local base.LocalNode,
	policy isaac.NodePolicy,
) *StoppedHandler {
	return &StoppedHandler{
		baseHandler: newBaseHandler(StateStopped, local, policy, nil),
	}
}
