package main

import (
	"context"

	"github.com/spikeekips/mitum/base"
	"github.com/spikeekips/mitum/util"
	"github.com/spikeekips/mitum/util/encoder"
	"github.com/spikeekips/mitum/util/fixedtree"
)

func newFuncBlockFileStatesTreeWriter(
	enc encoder.Encoder,
	createWriter blockFileWriterCreateWriterFunc,
	chunkItemsf,
	maxSemSizef func() uint64,
) blockFileWriterTreeFunc {
	w := newFuncBlockFileTreeWriter(base.BlockItemStatesTree, enc, createWriter, chunkItemsf, maxSemSizef)

	return func(ctx context.Context, workID string, tr fixedtree.Tree) ([]blockFileChunk, error) {
		if tr.Len() < 1 {
			return nil, nil
		}

		if tr.Hint().Type() != base.StateFixedtreeHint.Type() {
			return nil, util.ErrInvalid.Errorf("wrong state fixed tree hint, %q", tr.Hint())
		}

		return w(ctx, workID, tr)
	}
}
