package main

import (
	"context"

	"github.com/spikeekips/mitum/base"
	"github.com/spikeekips/mitum/util"
)

// FIXME add failed operations to unmerged
// FIXME add empty merged states to unmerged

func newStateMergeValueExtractorGetOperationFunc(
	f stateMergeValueExtractorGetOperationFunc,
	networkID base.NetworkID,
) stateMergeValueExtractorGetOperationFunc {
	return func(ctx context.Context, opindex uint64, oph, fact util.Hash) (Operation, bool, error) {
		// NOTE check fact hash is correct

		var op Operation

		switch o, found, err := f(ctx, opindex, oph, fact); {
		case err != nil, !found:
			return nil, found, err
		default:
			op = o
		}

		if err := op.IsValid(networkID); err != nil {
			return nil, true, err
		}

		switch {
		case !op.Hash().Equal(oph):
			return nil, true, util.ErrInvalid.Errorf("operation hash not match")
		case !op.Fact().Hash().Equal(fact):
			return nil, true, util.ErrInvalid.Errorf("fact hash not match")
		}

		return op, true, nil
	}
}
