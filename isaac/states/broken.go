package isaacstates

import (
	"github.com/pkg/errors"
	"github.com/spikeekips/mitum/base"
	"github.com/spikeekips/mitum/util"
)

var ErrUnpromising = util.NewIDError("unpromising broken error")

type BrokenHandlerArgs struct {
	LeaveMemberlistFunc func() error
	Exit                bool
}

func NewBrokenHandlerArgs() *BrokenHandlerArgs {
	return &BrokenHandlerArgs{
		LeaveMemberlistFunc: func() error {
			return util.ErrNotImplemented.Errorf("LeaveMemberlistFunc")
		},
	}
}

type BrokenHandler struct {
	*baseHandler
	args *BrokenHandlerArgs
}

type NewBrokenHandlerType struct {
	*BrokenHandler
}

func NewNewBrokenHandlerType(
	networkID base.NetworkID,
	local base.LocalNode,
	args *BrokenHandlerArgs,
) *NewBrokenHandlerType {
	return &NewBrokenHandlerType{
		BrokenHandler: &BrokenHandler{
			baseHandler: newBaseHandlerType(StateBroken, networkID, local),
			args:        args,
		},
	}
}

func (h *NewBrokenHandlerType) new() (handler, error) {
	return &BrokenHandler{
		baseHandler: h.baseHandler.new(),
		args:        h.args,
	}, nil
}

func (st *BrokenHandler) enter(from StateType, i switchContext) (func(), error) {
	e := util.StringError("enter broken state")

	deferred, err := st.baseHandler.enter(from, i)
	if err != nil {
		return nil, e.Wrap(err)
	}

	var sctx baseErrorSwitchContext
	if err := util.SetInterfaceValue(i, &sctx); err != nil {
		return nil, e.Wrap(err)
	}

	switch err := sctx.Unwrap(); {
	case err == nil:
	case errors.Is(err, ErrUnpromising), st.args.Exit:
		return nil, e.Wrap(err)
	}

	if st.args.Exit {
		return nil, e.Errorf("exit")
	}

	return func() {
		deferred()

		if lerr := st.args.LeaveMemberlistFunc(); lerr != nil {
			st.Log().Error().Err(lerr).Msg("failed to leave memberilst; ignored")
		}

		if err := st.bbt.StopTimers(); err != nil {
			st.Log().Error().Err(err).Msg("failed to stop all timers")
		}
	}, nil
}

func newBrokenSwitchContext(from StateType, err error) baseErrorSwitchContext {
	return newBaseErrorSwitchContext(from, StateBroken, err)
}
