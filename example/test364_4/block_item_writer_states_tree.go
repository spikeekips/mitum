package main

import (
	"context"
	"slices"
	"strings"
	"sync"

	"github.com/pkg/errors"
	"github.com/spikeekips/mitum/base"
	"github.com/spikeekips/mitum/util"
	"github.com/spikeekips/mitum/util/encoder"
	"github.com/spikeekips/mitum/util/fixedtree"
)

type (
	blockItemWriterNewWriteStatesTreeAddFunc  func(stateKey string) error
	blockItemWriterNewWriteStatesTreeDoneFunc func(error) (base.BlockMapItem, error)
	blockItemWriterNewWriteStatesTreeFunc     func(_ context.Context, size uint64, grow ...uint64) (blockItemWriterNewWriteStatesTreeAddFunc, blockItemWriterNewWriteStatesTreeDoneFunc, error)
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

		var lock sync.Mutex
		m := map[string]struct{}{}

		checkUniq := func(n string) bool {
			lock.Lock()
			defer lock.Unlock()

			_, found := m[n]
			if !found {
				m[n] = struct{}{}
			}

			return !found
		}

		return func(ctx context.Context, size uint64, grow ...uint64) (
			blockItemWriterNewWriteStatesTreeAddFunc,
			blockItemWriterNewWriteStatesTreeDoneFunc,
			error,
		) {
			var add func(fixedtree.Node) error
			done := func(func([]fixedtree.Node) error) error { return nil }

			if size > 0 {
				switch a, d, err := util.CompactAppendSliceV1[fixedtree.Node](size, grow...); {
				case err != nil:
					return nil, nil, err
				default:
					add = func(n fixedtree.Node) error {
						if !checkUniq(n.Key()) {
							return nil
						}

						_, _ = a(n)

						return nil
					}

					done = d
				}
			}

			wctx, wctxCancel := ContextWithCancelCause(ctx)

			return func(stateKey string) error {
					if err := context.Cause(wctx); err != nil {
						return errors.WithStack(err)
					}

					if size < 1 {
						return wctxCancel(errors.Errorf("zero size, but tried to write"))
					}

					if err := add(fixedtree.NewBaseNode(stateKey)); err != nil {
						return wctxCancel(err)
					}

					return nil
				},
				func(err error) (bm base.BlockMapItem, _ error) {
					defer func() {
						done(nil)
						clear(m)
						m = nil

						wctxCancel(nil)
					}()

					if err := context.Cause(wctx); err != nil {
						return nil, errors.WithStack(err)
					}

					if err != nil {
						return nil, wctxCancel(err)
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
						return nil, wctxCancel(err)
					}

					return bm, nil
				},
				nil
		}, nil
	}
}
