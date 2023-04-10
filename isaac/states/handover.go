package isaacstates

import (
	"github.com/spikeekips/mitum/base"
)

type handoverSwitchContext struct {
	vp base.Voteproof
	baseSwitchContext
}

func newHandoverSwitchContext(from StateType, vp base.Voteproof) handoverSwitchContext {
	return handoverSwitchContext{
		baseSwitchContext: newBaseSwitchContext(StateHandover, switchContextOKFuncCheckFrom(from)),
		vp:                vp,
	}
}

func newHandoverSwitchContextFromOther(sctx switchContext) handoverSwitchContext { // FIXME set from
	var vp base.Voteproof

	switch t := sctx.(type) {
	case consensusSwitchContext:
		vp = t.vp
	case joiningSwitchContext:
		vp = t.vp
	}

	return handoverSwitchContext{
		baseSwitchContext: newBaseSwitchContext(StateHandover, func(StateType) bool { return true }),
		vp:                vp,
	}
}
