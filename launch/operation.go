package launch

import (
	"context"

	"github.com/spikeekips/mitum/base"
	"github.com/spikeekips/mitum/util/hint"
)

func OperationPreProcess(
	oprs *hint.CompatibleSet,
	op base.Operation,
	height base.Height,
) (
	preprocess func(context.Context, base.GetStateFunc) (base.OperationProcessReasonError, error),
	cancelf func() error,
	_ error,
) {
	v := oprs.Find(op.Hint())
	if v == nil {
		return op.PreProcess, func() error { return nil }, nil
	}

	f := v.(func(height base.Height) (base.OperationProcessor, error)) //nolint:forcetypeassert //...

	switch opp, err := f(height); {
	case err != nil:
		return nil, nil, err
	default:
		return func(ctx context.Context, getStateFunc base.GetStateFunc) (base.OperationProcessReasonError, error) {
			return opp.PreProcess(ctx, op, getStateFunc)
		}, opp.Close, nil
	}
}
