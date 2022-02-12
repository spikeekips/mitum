package isaac

import "github.com/rs/zerolog"

type brokenSwitchContext struct {
	baseStateSwitchContext
	err error
}

func newBrokenSwitchContext(from StateType, err error) brokenSwitchContext {
	return brokenSwitchContext{
		baseStateSwitchContext: newBaseStateSwitchContext(from, StateBroken),
		err:                    err,
	}
}

func (s brokenSwitchContext) Error() string {
	if s.err != nil {
		return s.err.Error()
	}

	return ""
}

func (s brokenSwitchContext) Unwrap() error {
	return s.err
}

func (s brokenSwitchContext) MarshalZerologObject(e *zerolog.Event) {
	s.baseStateSwitchContext.MarshalZerologObject(e)

	if s.err != nil {
		e.Err(s.err)
	}
}
