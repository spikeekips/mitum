package isaacstates

import (
	"github.com/pkg/errors"
	"github.com/spikeekips/mitum/base"
	"github.com/spikeekips/mitum/isaac"
	"github.com/spikeekips/mitum/util"
)

var ErrUnpromising = util.NewMError("unpromising broken error")

type BrokenHandler struct {
	*baseHandler
}

type NewBrokenHandlerType struct {
	*BrokenHandler
}

func NewNewBrokenHandlerType(
	local base.LocalNode,
	params *isaac.LocalParams,
) *NewBrokenHandlerType {
	return &NewBrokenHandlerType{
		BrokenHandler: &BrokenHandler{
			baseHandler: newBaseHandler(StateBroken, local, params),
		},
	}
}

func (h *NewBrokenHandlerType) new() (handler, error) {
	return &BrokenHandler{
		baseHandler: h.baseHandler.new(),
	}, nil
}

func (st *BrokenHandler) enter(from StateType, i switchContext) (func(), error) {
	e := util.StringErrorFunc("failed to enter broken state")

	deferred, err := st.baseHandler.enter(from, i)
	if err != nil {
		return nil, e(err, "")
	}

	sctx, ok := i.(baseErrorSwitchContext)
	if !ok {
		return nil, e(nil, "invalid stateSwitchContext, not for broken state; %T", i)
	}

	switch err := sctx.Unwrap(); {
	case err == nil:
	case errors.Is(err, ErrUnpromising):
		return nil, e(err, "")
	}

	return func() {
		deferred()

		if err := st.timers.StopTimersAll(); err != nil {
			st.Log().Error().Err(err).Msg("failed to stop all timers")
		}
	}, nil
}

func newBrokenSwitchContextFromEmpty(err error) baseErrorSwitchContext {
	return newBaseErrorSwitchContext(StateBroken, err, switchContextOKFuncNil)
}

func newBrokenSwitchContext(from StateType, err error) baseErrorSwitchContext {
	return newBaseErrorSwitchContext(StateBroken, err, switchContextOKFuncCheckFrom(from))
}
