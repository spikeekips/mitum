package states

import (
	"github.com/rs/zerolog"
	"github.com/spikeekips/mitum/base"
)

type syncingSwitchContext struct {
	baseStateSwitchContext
	height base.Height
}

func newSyncingSwitchContext(from StateType, height base.Height) syncingSwitchContext {
	return syncingSwitchContext{
		baseStateSwitchContext: newBaseStateSwitchContext(from, StateSyncing),
		height:                 height,
	}
}

func (s syncingSwitchContext) Error() string {
	return ""
}

func (s syncingSwitchContext) MarshalZerologObject(e *zerolog.Event) {
	s.baseStateSwitchContext.MarshalZerologObject(e)

	e.Int64("height", s.height.Int64())
}
