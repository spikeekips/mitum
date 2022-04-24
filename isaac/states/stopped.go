package isaacstates

import (
	"github.com/rs/zerolog"
	"github.com/spikeekips/mitum/isaac"
)

type StoppedHandler struct {
	*baseHandler
}

func NewStoppedHandler(
	local isaac.LocalNode,
	policy isaac.NodePolicy,
) *StoppedHandler {
	return &StoppedHandler{
		baseHandler: newBaseHandler(StateStopped, local, policy, nil),
	}
}

type stoppedSwitchContext struct {
	baseSwitchContext
	err error
}

func newStoppedSwitchContext(from StateType, err error) stoppedSwitchContext {
	return stoppedSwitchContext{
		baseSwitchContext: newBaseSwitchContext(from, StateStopped),
		err:               err,
	}
}

func (s stoppedSwitchContext) Error() string {
	if s.err != nil {
		return s.err.Error()
	}

	return ""
}

func (s stoppedSwitchContext) Unwrap() error {
	return s.err
}

func (s stoppedSwitchContext) MarshalZerologObject(e *zerolog.Event) {
	s.baseSwitchContext.MarshalZerologObject(e)

	if s.err != nil {
		e.Err(s.err)
	}
}
