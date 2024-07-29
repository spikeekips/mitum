package main

import (
	"context"
	"slices"
	"strings"

	"github.com/pkg/errors"
	"github.com/spikeekips/mitum/base"
	"github.com/spikeekips/mitum/util"
	"github.com/spikeekips/mitum/util/encoder"
	"github.com/spikeekips/mitum/util/fixedtree"
)

type (
	blockItemWriterNewWriteStatesTreeAddFunc  func(stateKey string) error
	blockItemWriterNewWriteStatesTreeDoneFunc func(error) (base.BlockMapItem, error)
	blockItemWriterNewWriteStatesTreeFunc     func(_ context.Context, total uint64) (blockItemWriterNewWriteStatesTreeAddFunc, blockItemWriterNewWriteStatesTreeDoneFunc, error)
)

func newBlockItemStatesTreeWriterNewWriteFunc(
	enc encoder.Encoder,
	createWriter blockItemWriterCreateWriterFunc,
) func(string) (blockItemWriterNewWriteStatesTreeFunc, error) {
	w := newBlockItemTreeWriterNewWriteFunc(
		enc,
		createWriter,
		base.StateFixedtreeHint,
		encodeWithTreeIndex[fixedtree.Node],
	)

	return func(createID string) (blockItemWriterNewWriteStatesTreeFunc, error) {
		var c blockItemWriterNewWriteTreeFunc

		switch i, err := w(createID); {
		case err != nil:
			return nil, err
		default:
			c = i
		}

		return func(ctx context.Context, maxTotal uint64) (
			blockItemWriterNewWriteStatesTreeAddFunc,
			blockItemWriterNewWriteStatesTreeDoneFunc,
			error,
		) {
			var add util.CompactAppendSliceAddFunc[fixedtree.Node]
			done := func(func([]fixedtree.Node) error) error { return nil }

			if maxTotal > 0 {
				switch a, d, err := util.CompactAppendSliceV1[fixedtree.Node](maxTotal); {
				case err != nil:
					return nil, nil, err
				default:
					add = a
					done = d
				}
			}

			wctx, wctxCancel := context.WithCancelCause(ctx)

			return func(stateKey string) error {
					if err := context.Cause(wctx); err != nil {
						return errors.WithStack(err)
					}

					if maxTotal < 1 {
						return util.ErrFound.Errorf("zero max total, but tried to write")
					}

					if a, _ := add(fixedtree.NewBaseNode(stateKey)); a < 1 {
						err := errors.Errorf("add state key; out of range")

						wctxCancel(err)

						return err
					}

					return nil
				},
				func(err error) (bm base.BlockMapItem, _ error) {
					defer func() {
						done(nil)
						wctxCancel(nil)
					}()

					if err := context.Cause(wctx); err != nil {
						return nil, errors.WithStack(err)
					}

					if err != nil {
						return nil, err
					}

					if err := done(func(s []fixedtree.Node) error {
						if len(s) < 1 {
							return nil
						}

						slices.SortFunc(s, func(a, b fixedtree.Node) int {
							return strings.Compare(a.Key(), b.Key())
						})

						switch tw, err := fixedtree.NewWriterFromNodes(base.StateFixedtreeHint, s); {
						case err != nil:
							return err
						default:
							_, treeDone := c(wctx, uint64(len(s)))

							switch i, err := treeDone(err, tw); {
							case err != nil:
								return err
							default:
								bm = i

								return nil
							}
						}
					}); err != nil {
						return nil, err
					}

					return bm, nil
				},
				nil
		}, nil
	}
}
