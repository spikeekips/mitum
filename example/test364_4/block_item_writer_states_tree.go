package main

import (
	"context"

	"github.com/spikeekips/mitum/base"
	"github.com/spikeekips/mitum/util"
	"github.com/spikeekips/mitum/util/encoder"
	"github.com/spikeekips/mitum/util/fixedtree"
)

type blockItemWriterNewWriteStatesTreeFunc func(_ context.Context, createID string, _ fixedtree.Tree) (base.BlockMapItem, error)

func newBlockItemStatesTreeWriterFunc(
	enc encoder.Encoder,
	createWriter blockItemWriterCreateWriterFunc,
) blockItemWriterNewWriteStatesTreeFunc {
	w := newBlockItemTreeWriterFunc(base.BlockItemStatesTree, enc, createWriter)

	return func(ctx context.Context, createID string, tr fixedtree.Tree) (base.BlockMapItem, error) {
		if tr.Hint().Type() != base.StateFixedtreeHint.Type() {
			return nil, util.ErrInvalid.Errorf("wrong state fixed tree hint, %q", tr.Hint())
		}

		return w(ctx, createID, tr)
	}
}
