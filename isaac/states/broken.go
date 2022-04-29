package isaacstates

import (
	"github.com/spikeekips/mitum/base"
	"github.com/spikeekips/mitum/isaac"
)

type BrokenHandler struct {
	*baseHandler
}

func NewBrokenHandler(
	local base.LocalNode,
	policy isaac.NodePolicy,
) *BrokenHandler {
	return &BrokenHandler{
		baseHandler: newBaseHandler(StateBroken, local, policy, nil),
	}
}

func newBrokenSwitchContext(from StateType, err error) baseErrorSwitchContext {
	return newBaseErrorSwitchContext(from, StateBroken, err)
}
