package main

import (
	"context"

	"github.com/spikeekips/mitum/base"
	"github.com/spikeekips/mitum/util"
	"github.com/spikeekips/mitum/util/encoder"
	"github.com/spikeekips/mitum/util/fixedtree"
)

type blockItemWriterNewWriteOperationsTreeFunc func(_ context.Context, createID string, _ fixedtree.Tree) (base.BlockMapItem, error)

func newBlockItemOperationsTreeWriterFunc(
	enc encoder.Encoder,
	createWriter blockItemWriterCreateWriterFunc,
) blockItemWriterNewWriteOperationsTreeFunc {
	w := newBlockItemTreeWriterFunc(base.BlockItemOperationsTree, enc, createWriter)

	return func(ctx context.Context, createID string, tr fixedtree.Tree) (base.BlockMapItem, error) {
		if tr.Hint().Type() != base.OperationFixedtreeHint.Type() {
			return nil, util.ErrInvalid.Errorf("wrong operation fixed tree hint, %q", tr.Hint())
		}

		return w(ctx, createID, tr)
	}
}
