package base

import (
	"context"

	"github.com/spikeekips/mitum/util/hint"
)

type (
	PreProcessor func(StatePool) (bool, error)
	Processor    func(StatePool) error
)

type Operation interface {
	hint.Hinter
	SealBody
	SignedFact
}

type ProcessableOperation interface {
	PreProcess(context.Context, StatePool) (bool, error)
	Process(context.Context, StatePool) ([]State, error)
}

type OperationProcessor interface {
	PreProcess(context.Context, Operation, StatePool) (bool, error)
	Process(context.Context, Operation, StatePool) ([]State, error)
}
