package isaacstates

import "github.com/rs/zerolog"

type brokenSwitchContext struct {
	baseSwitchContext
	err error
}

func newBrokenSwitchContext(from StateType, err error) brokenSwitchContext {
	return brokenSwitchContext{
		baseSwitchContext: newBaseSwitchContext(from, StateBroken),
		err:               err,
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
	s.baseSwitchContext.MarshalZerologObject(e)

	if s.err != nil {
		e.Err(s.err)
	}
}
