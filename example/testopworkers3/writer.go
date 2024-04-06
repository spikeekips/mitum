package testopsworkers

import (
	"bytes"
	"cmp"
	"context"
	"io"
	"os"
	"slices"
	"strconv"
	"sync"

	"github.com/pkg/errors"
	"github.com/spikeekips/mitum/isaac"
	"github.com/spikeekips/mitum/util"
	"github.com/spikeekips/mitum/util/encoder"
	"github.com/spikeekips/mitum/util/fixedtree"
	"github.com/spikeekips/mitum/util/hint"
)

type baseBlockItemWriter struct {
	ctx          func() context.Context
	ctxCancel    func(error)
	encode       func(interface{}) ([]byte, error)
	newJobWorker func(context.Context) (util.JobWorker, error)
}

func newBaseBlockItemWriter(
	ctx context.Context,
	enc encoder.Encoder,
	newJobWorker func(context.Context) (util.JobWorker, error),
) *baseBlockItemWriter {
	wctx, wctxCancel := context.WithCancelCause(ctx)

	return &baseBlockItemWriter{
		ctx: func() context.Context { return wctx },
		ctxCancel: func(err error) {
			wctxCancel(err)
		},
		encode:       enc.Marshal,
		newJobWorker: newJobWorker,
	}
}

func (w *baseBlockItemWriter) Cancel() {
	w.ctxCancel(nil)
}

type (
	treeWriterSaveNodeFunc      func(uint64, fixedtree.Node) error
	treeWriterCloseSaveNodeFunc func(bool) error
)

type baseTreeWriter[K cmp.Ordered] struct {
	*baseBlockItemWriter
	add      func(k K, node fixedtree.Node) error
	treeRoot func() (util.Hash, error)
	saveDone func() error
}

func NewBaseTreeWriter[K cmp.Ordered](
	ctx context.Context,
	enc encoder.Encoder,
	newJobWorker func(context.Context) (util.JobWorker, error),
	ht hint.Hint,
	saveNodeFuncs func(context.Context) (treeWriterSaveNodeFunc, treeWriterCloseSaveNodeFunc),
	minNodesSize uint64,
) (*baseTreeWriter[K], error) {
	w := &baseTreeWriter[K]{
		baseBlockItemWriter: newBaseBlockItemWriter(ctx, enc, newJobWorker),
	}

	var sew util.JobWorker
	var treeDone func() error
	var saveNodeFunc func(uint64, fixedtree.Node) error

	switch i, j, k, err := w.newTreeFuncs(
		ht,
		minNodesSize,
		func(index uint64, node fixedtree.Node) error {
			return sew.NewJob(func(context.Context, uint64) error {
				return saveNodeFunc(index, node)
			})
		},
	); {
	case err != nil:
		w.ctxCancel(err)

		return nil, err
	default:
		treeDone = k
		w.add = i
		w.treeRoot = j
	}

	switch i, j, k, err := w.newSaveFuncs(saveNodeFuncs, treeDone); {
	case err != nil:
		w.ctxCancel(err)

		return nil, err
	default:
		sew = i
		saveNodeFunc = j
		w.saveDone = k
	}

	return w, nil
}

func (w *baseTreeWriter[K]) Add(k K, node fixedtree.Node) error {
	if err := w.add(k, node); err != nil {
		w.ctxCancel(err)

		return err
	}

	return nil
}

func (w *baseTreeWriter[K]) Tree() (util.Hash, error) {
	switch i, err := w.treeRoot(); {
	case err != nil:
		w.ctxCancel(err)

		return nil, err
	default:
		return i, nil
	}
}

func (w *baseTreeWriter[K]) Save() error {
	if err := w.saveDone(); err != nil {
		w.ctxCancel(err)

		return err
	}

	return nil
}

func (w *baseTreeWriter[K]) buildTree(
	ht hint.Hint,
	nodes []fixedtree.Node,
	saveNodeFunc func(uint64, fixedtree.Node) error,
) (tr fixedtree.Tree, _ error) {
	if len(nodes) < 1 {
		return tr, nil
	}

	slices.SortFunc(nodes, func(a, b fixedtree.Node) int {
		return cmp.Compare(
			a.(orderedTreeNode[K]).K,
			b.(orderedTreeNode[K]).K,
		)
	})

	var tw *fixedtree.Writer

	switch i, err := fixedtree.NewWriterFromNodes(ht, nodes); {
	case err != nil:
		return tr, err
	default:
		tw = i
	}

	if err := tw.Write(saveNodeFunc); err != nil {
		return tr, err
	}

	return tw.Tree()
}

func (w *baseTreeWriter[K]) newTreeFuncs(
	ht hint.Hint,
	minNodesSize uint64,
	saveNodeFunc func(uint64, fixedtree.Node) error,
) (
	add func(K, fixedtree.Node) error,
	treeRoot func() (util.Hash, error),
	treeDone func() error,
	_ error,
) {
	var tew util.JobWorker

	var addNodes util.CompactAppendSliceAddFunc[fixedtree.Node]
	var doneNodes util.CompactAppendSliceDoneFunc[fixedtree.Node]
	var root util.Hash

	tctx, treeDone, _ := NewContextDoneFunc(
		w.ctx(),
		func(context.Context) error {
			defer func() {
				tew.Close()

				if doneNodes != nil {
					doneNodes(nil)
				}
			}()

			tew.Done()

			if err := tew.Wait(); err != nil {
				return err
			}

			var nodes []fixedtree.Node

			if doneNodes != nil {
				_ = doneNodes(func(s []fixedtree.Node) error {
					nodes = slices.Clone(s)

					return nil
				})
			}

			if len(nodes) < 1 {
				return nil
			}

			switch tr, err := w.buildTree(
				ht,
				nodes,
				saveNodeFunc,
			); {
			case err != nil:
				return err
			default:
				defer tr.Close()

				root = tr.Root()

				// FIXME save tree

				return nil
			}
		},
	)

	switch i, err := w.newJobWorker(tctx); {
	case err != nil:
		return nil, nil, nil, err
	default:
		tew = i
	}

	var addNodesOnce sync.Once

	return func(k K, node fixedtree.Node) error {
			var gerr error

			addNodesOnce.Do(func() {
				switch i, j, err := util.CompactAppendSliceV1[fixedtree.Node](minNodesSize, 1<<7); {
				case err != nil:
					gerr = err
				default:
					addNodes = i
					doneNodes = j
				}
			})

			if gerr != nil {
				return gerr
			}

			return tew.NewJob(func(context.Context, uint64) error {
				_, _ = addNodes(newOrderedTreeNode(k, node))

				return nil
			})
		},
		func() (util.Hash, error) {
			if err := treeDone(); err != nil {
				return nil, err
			}

			return root, nil
		},
		treeDone,
		nil
}

func (w *baseTreeWriter[K]) newSaveFuncs(
	saveNodeFuncs func(context.Context) (treeWriterSaveNodeFunc, treeWriterCloseSaveNodeFunc),
	treeDone func() error,
) (
	sew util.JobWorker,
	saveNodeFunc func(uint64, fixedtree.Node) error,
	saveDone func() error,
	_ error,
) {
	var closeSaveNodeFunc func(bool) error

	sctx, sDone, _ := NewContextDoneFunc(
		w.ctx(),
		func(ctx context.Context) error {
			if err := func() error {
				defer sew.Close()

				if err := treeDone(); err != nil {
					return err
				}

				sew.Done()

				return sew.Wait()
			}(); err != nil {
				_ = closeSaveNodeFunc(false)

				return err
			}

			return closeSaveNodeFunc(true)
		},
	)

	switch i, err := w.newJobWorker(sctx); {
	case err != nil:
		return nil, nil, nil, err
	default:
		sew = i
	}

	saveNodeFunc, closeSaveNodeFunc = saveNodeFuncs(sctx)

	return sew,
		saveNodeFunc,
		sDone,
		nil
}

type orderedTreeNode[K cmp.Ordered] struct {
	fixedtree.Node
	K K
}

func newOrderedTreeNode[K cmp.Ordered](k K, node fixedtree.Node) orderedTreeNode[K] {
	return orderedTreeNode[K]{
		Node: node,
		K:    k,
	}
}

func DecodeIndexedTreeNode(enc encoder.Encoder, b []byte, ht hint.Hint) (index uint64, _ fixedtree.Node, _ error) {
	bf := bytes.NewBuffer(b)
	defer bf.Reset()

	switch i, err := bf.ReadBytes(','); {
	case err != nil:
		return 0, nil, errors.WithStack(err)
	case len(i) < 2:
		return 0, nil, errors.Errorf("find index string")
	default:
		j, err := strconv.ParseUint(string(i[:len(i)-1]), 10, 64)
		if err != nil {
			return 0, nil, errors.WithStack(err)
		}

		index = j
	}

	left, err := io.ReadAll(bf)
	if err != nil {
		return 0, nil, errors.WithStack(err)
	}

	switch i, err := enc.DecodeWithHint(left, ht); {
	case err != nil:
		return 0, nil, err
	default:
		return index, i.(fixedtree.Node), nil
	}
}

func OpenCompressedFile(f string) (*util.CompressedReader, error) {
	switch i, err := os.Open(f); {
	case err != nil:
		return nil, err
	default:
		return util.NewCompressedReader(i, "gz", nil)
	}
}

func LoadTree(
	enc encoder.Encoder,
	ht hint.Hint,
	r io.Reader,
) (tr fixedtree.Tree, _ error) {
	var (
		add  util.CompactAppendSliceAddFunc[fixedtree.Node]
		done util.CompactAppendSliceDoneFunc[fixedtree.Node]
	)

	switch i, j, err := util.CompactAppendSliceV1[fixedtree.Node](1<<9, 1<<9); {
	case err != nil:
		return tr, err
	default:
		add = i
		done = j
	}

	if _, err := isaac.BlockItemDecodeLineItems(
		r,
		func(b []byte) (interface{}, error) {
			switch index, n, err := DecodeIndexedTreeNode(enc, b, ht); {
			case err != nil:
				return nil, err
			default:
				_, _ = add(newOrderedTreeNode(index, n))
			}

			return nil, nil
		},
		func(uint64, interface{}) error { return nil },
	); err != nil {
		return tr, err
	}

	var nodes []fixedtree.Node

	if err := done(func(s []fixedtree.Node) error {
		nodes = slices.Clone(s)

		return nil
	}); err != nil {
		return tr, err
	}

	slices.SortFunc(nodes, func(a, b fixedtree.Node) int {
		return cmp.Compare(
			a.(orderedTreeNode[uint64]).K,
			b.(orderedTreeNode[uint64]).K,
		)
	})

	return fixedtree.NewTree(ht, nodes)
}

func LoadTreeFromFile(
	enc encoder.Encoder,
	ht hint.Hint,
	f string,
) (tr fixedtree.Tree, _ error) {
	var r io.Reader

	switch i, err := OpenCompressedFile(f); {
	case err != nil:
		return tr, err
	default:
		j, err := i.Decompress()
		if err != nil {
			return tr, err
		}

		r = j
	}

	return LoadTree(enc, ht, r)
}

func NewContextDoneFunc(
	ctx context.Context,
	f func(context.Context) error,
) (
	_ context.Context,
	done func() error,
	cancel func(error),
) {
	errDone := errors.New(util.UUID().String())
	cctx, cctxCancel := context.WithCancelCause(ctx)

	var once sync.Once

	return cctx,
		func() error {
			once.Do(func() {
				go func() {
					if err := f(cctx); err != nil {
						cctxCancel(err)

						return
					}

					cctxCancel(errDone)
				}()
			})

			<-cctx.Done()

			switch err := context.Cause(cctx); {
			case errors.Is(err, errDone):
				return nil
			default:
				return err
			}
		},
		cctxCancel
}
