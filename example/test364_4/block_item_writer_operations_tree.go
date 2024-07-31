package main

import (
	"bytes"
	"context"
	"strconv"
	"sync"

	"github.com/pkg/errors"
	"github.com/spikeekips/mitum/base"
	"github.com/spikeekips/mitum/util"
	"github.com/spikeekips/mitum/util/encoder"
	"github.com/spikeekips/mitum/util/fixedtree"
	"github.com/spikeekips/mitum/util/hint"
)

type (
	blockItemWriterNewWriteTreeAddFunc            func(index uint64, _ func() (fixedtree.Node, error)) error
	blockItemWriterNewWriteTreeDoneFunc           func(error, *fixedtree.Writer) (base.BlockMapItem, error)
	blockItemWriterNewWriteTreeFunc               func(_ context.Context, total uint64) (blockItemWriterNewWriteTreeAddFunc, blockItemWriterNewWriteTreeDoneFunc)
	blockItemWriterNewWriteOperationsTreeAddFunc  func(opindex uint64, fact util.Hash, inState bool) error
	blockItemWriterNewWriteOperationsTreeDoneFunc func(error) (base.BlockMapItem, error)
	blockItemWriterNewWriteOperationsTreeFunc     func(_ context.Context, total uint64) (blockItemWriterNewWriteOperationsTreeAddFunc, blockItemWriterNewWriteOperationsTreeDoneFunc)
)

func newBlockItemOperationsTreeWriterNewWriteFunc(
	enc encoder.Encoder,
	createWriter blockItemWriterCreateWriterFunc,
) func(string) (blockItemWriterNewWriteOperationsTreeFunc, error) {
	w := newBlockItemTreeWriterNewWriteFunc(
		enc,
		createWriter,
		base.OperationFixedtreeHint,
		encodeWithTreeIndex[fixedtree.Node],
	)

	return func(createID string) (blockItemWriterNewWriteOperationsTreeFunc, error) {
		var c blockItemWriterNewWriteTreeFunc

		switch i, err := w(createID); {
		case err != nil:
			return nil, err
		default:
			c = i
		}

		var treeAdd blockItemWriterNewWriteTreeAddFunc
		var treeDone blockItemWriterNewWriteTreeDoneFunc

		var once sync.Once

		return func(ctx context.Context, total uint64) (blockItemWriterNewWriteOperationsTreeAddFunc, blockItemWriterNewWriteOperationsTreeDoneFunc) {
				return func(opindex uint64, fact util.Hash, inState bool) error {
						once.Do(func() {
							treeAdd, treeDone = c(ctx, total)
						})

						return treeAdd(opindex, func() (fixedtree.Node, error) {
							switch {
							case inState:
								return base.NewInStateOperationFixedtreeNode(fact, ""), nil
							default:
								return base.NewNotInStateOperationFixedtreeNode(fact, ""), nil
							}
						})
					},
					func(err error) (base.BlockMapItem, error) {
						if treeDone != nil {
							return treeDone(err, nil)
						}

						return nil, err
					}
			},
			nil
	}
}

func newBlockItemTreeWriterNewWriteFunc(
	enc encoder.Encoder,
	createWriter blockItemWriterCreateWriterFunc,
	treehint hint.Hint,
	encode func(encoder.Encoder, uint64, fixedtree.Node) ([]byte, error),
) func(string) (blockItemWriterNewWriteTreeFunc, error) {
	w := newBaseBlockItemWriterNewWriteFunc(base.BlockItemOperationsTree, createWriter)

	return func(createID string) (blockItemWriterNewWriteTreeFunc, error) {
		var c blockItemWriterBaseNewWriteFunc

		switch i, err := w(createID); {
		case err != nil:
			return nil, err
		default:
			c = i
		}

		var tw *fixedtree.Writer

		once := OnceFuncCallback(func(total uint64) error {
			switch i, err := fixedtree.NewWriter(treehint, total); {
			case err != nil:
				return err
			default:
				tw = i

				return nil
			}
		})

		return func(ctx context.Context, total uint64) (blockItemWriterNewWriteTreeAddFunc, blockItemWriterNewWriteTreeDoneFunc) {
				wctx, wctxCancel := ContextWithCancelCause(ctx)

				return func(index uint64, f func() (fixedtree.Node, error)) error {
						if total < 1 {
							return util.ErrFound.Errorf("zero total, but tried to write")
						}

						if err := once(total); err != nil {
							return wctxCancel(err)
						}

						switch n, err := f(); {
						case err != nil:
							return err
						default:
							if err := tw.Add(index, n); err != nil {
								return wctxCancel(err)
							}

							return nil
						}
					},
					func(err error, w *fixedtree.Writer) (base.BlockMapItem, error) {
						if err := context.Cause(wctx); err != nil {
							return nil, errors.WithStack(err)
						}

						defer wctxCancel(nil)

						if w != nil {
							tw = w
						}

						if err != nil || tw == nil || tw.Len() < 1 {
							return nil, wctxCancel(err)
						}

						defer tw.Close()

						switch bm, err := c(newTreeItemsHeader(enc.Hint(), uint64(tw.Len()), tw.Hint()), func(write ioWriteFunc) error {
							var jw JobWorker

							switch i, err := NewBaseJobWorker(wctx, int64(tw.Len())); {
							case err != nil:
								return err
							default:
								defer i.Close()

								jw = i
							}

							if err := tw.Write(func(index uint64, n fixedtree.Node) error {
								return jw.NewJob(func(context.Context) error {
									switch b, err := encode(enc, index, n); {
									case err != nil:
										return err
									default:
										_, err := write(b)

										return err
									}
								})
							}); err != nil {
								return err
							}

							jw.JobDone()

							return jw.Wait()
						}); {
						case err != nil:
							return nil, wctxCancel(err)
						default:
							return bm, nil
						}
					}
			},
			nil
	}
}

func encodeWithIndex[T any](enc encoder.Encoder, index uint64, indexName string, i T) ([]byte, error) {
	switch b, err := enc.Marshal(i); {
	case err != nil:
		return nil, err
	default:
		buf := bytes.NewBuffer(nil)
		defer buf.Reset()

		_, _ = buf.Write([]byte("# "))
		_, _ = buf.Write([]byte(indexName))
		_, _ = buf.Write([]byte("="))
		_, _ = buf.Write([]byte(strconv.FormatUint(index, 10)))
		_, _ = buf.Write([]byte("\n"))
		_, _ = buf.Write(b)
		_, _ = buf.Write([]byte("\n"))

		return buf.Bytes(), nil
	}
}

func encodeWithTreeIndex[T any](enc encoder.Encoder, index uint64, i T) ([]byte, error) {
	return encodeWithIndex(enc, index, "index", i)
}
