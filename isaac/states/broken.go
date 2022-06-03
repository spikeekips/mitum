package isaacstates

import (
	"github.com/pkg/errors"
	"github.com/spikeekips/mitum/base"
	"github.com/spikeekips/mitum/isaac"
	"github.com/spikeekips/mitum/util"
)

var ErrUnpromising = util.NewError("unpromising broken error")

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

func (st *BrokenHandler) enter(i switchContext) (func(), error) {
	e := util.StringErrorFunc("failed to enter broken state")

	deferred, err := st.baseHandler.enter(i)
	if err != nil {
		return nil, e(err, "")
	}

	ectx, ok := i.(baseErrorSwitchContext)
	if !ok {
		return nil, e(nil, "invalid stateSwitchContext, not for broken state; %T", i)
	}

	switch err := ectx.Unwrap(); {
	case err == nil:
	case errors.Is(err, ErrUnpromising):
		return nil, e(err, "")
	}

	return func() {
		deferred()
	}, nil
}

func newBrokenSwitchContext(from StateType, err error) baseErrorSwitchContext {
	return newBaseErrorSwitchContext(from, StateBroken, err)
}
