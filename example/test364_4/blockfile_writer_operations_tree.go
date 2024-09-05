package main

import (
	"context"

	"github.com/spikeekips/mitum/base"
	"github.com/spikeekips/mitum/util"
	"github.com/spikeekips/mitum/util/encoder"
	"github.com/spikeekips/mitum/util/fixedtree"
)

func newFuncBlockFileOperationsTreeWriter(
	enc encoder.Encoder,
	createWriter blockFileWriterCreateWriterFunc,
	chunkItemsf,
	maxSemSizef func() uint64,
) blockFileWriterTreeFunc {
	w := newFuncBlockFileTreeWriter(base.BlockItemOperationsTree, enc, createWriter, chunkItemsf, maxSemSizef)

	return func(ctx context.Context, createID string, tr fixedtree.Tree) ([]blockFileChunk, error) {
		if tr.Len() < 1 {
			return nil, nil
		}

		if tr.Hint().Type() != base.OperationFixedtreeHint.Type() {
			return nil, util.ErrInvalid.Errorf("wrong operation fixed tree hint, %q", tr.Hint())
		}

		return w(ctx, createID, tr)
	}
}
