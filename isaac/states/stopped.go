package isaacstates

import (
	"github.com/spikeekips/mitum/base"
	"github.com/spikeekips/mitum/isaac"
)

type StoppedHandler struct {
	*baseHandler
}

type NewStoppedHandlerType struct {
	*StoppedHandler
}

func NewNewStoppedHandlerType(
	local base.LocalNode,
	policy isaac.NodePolicy,
) *NewStoppedHandlerType {
	return &NewStoppedHandlerType{
		StoppedHandler: &StoppedHandler{
			baseHandler: newBaseHandler(StateStopped, local, policy, nil),
		},
	}
}

func (h *NewStoppedHandlerType) new() (handler, error) {
	return &StoppedHandler{
		baseHandler: h.baseHandler.new(),
	}, nil
}
