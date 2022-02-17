package isaac

import (
	"github.com/rs/zerolog"
)

type StoppedHandler struct {
	*baseStateHandler
}

func NewStoppedHandler(
	local *LocalNode,
	policy Policy,
) *StoppedHandler {
	return &StoppedHandler{
		baseStateHandler: newBaseStateHandler(StateStopped, local, policy),
	}
}

type stoppedSwitchContext struct {
	baseStateSwitchContext
	err error
}

func newStoppedSwitchContext(from StateType, err error) stoppedSwitchContext {
	return stoppedSwitchContext{
		baseStateSwitchContext: newBaseStateSwitchContext(from, StateStopped),
		err:                    err,
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
	s.baseStateSwitchContext.MarshalZerologObject(e)

	if s.err != nil {
		e.Err(s.err)
	}
}
