package main

import (
	"bytes"
	"cmp"
	"compress/gzip"
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"sync"

	"github.com/pkg/errors"
	"github.com/rs/zerolog"
	"github.com/spikeekips/mitum/base"
	"github.com/spikeekips/mitum/isaac"
	"github.com/spikeekips/mitum/util"
	"github.com/spikeekips/mitum/util/encoder"
	"github.com/spikeekips/mitum/util/fixedtree"
	"github.com/spikeekips/mitum/util/hint"
	"github.com/spikeekips/mitum/util/logging"
)

type BaseBlockItemWriter struct {
	ctx          func() context.Context
	ctxCancel    func(error)
	newJobWorker func(context.Context) (JobWorker, error)
}

func NewBaseBlockItemWriter(
	ctx context.Context,
	newJobWorker func(context.Context) (JobWorker, error),
) *BaseBlockItemWriter {
	wctx, wctxCancel := context.WithCancelCause(ctx)

	return &BaseBlockItemWriter{
		ctx: func() context.Context { return wctx },
		ctxCancel: func(err error) {
			wctxCancel(err)
		},
		newJobWorker: newJobWorker,
	}
}

func (w *BaseBlockItemWriter) Cancel() {
	w.ctxCancel(nil)
}

type (
	writerSaveBlockItemFunc  func(interface{}) error
	writerSaveBlockItemFuncs func(context.Context, base.BlockItemType) (writerSaveBlockItemFunc, writerCloseSaveFunc, error)
)

func NewSingleBlockItemWriter(
	ctx context.Context,
	t base.BlockItemType,
	i interface{},
	savef writerSaveBlockItemFuncs,
) (string, error) {
	var save writerSaveBlockItemFunc
	var saveDone writerCloseSaveFunc

	switch i, j, err := savef(ctx, t); {
	case err != nil:
		return "", err
	default:
		save = i
		saveDone = j
	}

	if err := save(i); err != nil {
		_, _ = saveDone(false)

		return "", err
	}

	return saveDone(true)
}

type VoteproofWriter struct {
	*BaseBlockItemWriter
	SetACCEPTVoteproof func(base.ACCEPTVoteproof) (string, error)
}

func NewVoteproofWriter(
	ctx context.Context,
	ivp base.INITVoteproof,
	savef writerSaveBlockItemFuncs,
) (*VoteproofWriter, error) {
	w := &VoteproofWriter{
		BaseBlockItemWriter: NewBaseBlockItemWriter(ctx, nil),
	}

	var save writerSaveBlockItemFunc
	var saveDone writerCloseSaveFunc

	switch i, j, err := savef(w.ctx(), base.BlockItemVoteproofs); {
	case err != nil:
		return nil, err
	default:
		save = i
		saveDone = j
	}

	_, doneINIT, _ := NewContextDoneFunc(
		w.ctx(),
		func(context.Context) error {
			if err := save(ivp); err != nil {
				w.ctxCancel(err)

				return err
			}

			return nil
		},
	)

	avpch := make(chan base.ACCEPTVoteproof, 1)

	var chunk string

	saveACCEPT := func(ctx context.Context) error {
		if err := doneINIT(); err != nil {
			return err
		}

		select {
		case <-ctx.Done():
			return ctx.Err()
		case vp := <-avpch:
			if err := save(vp); err != nil {
				return err
			}

			switch i, err := saveDone(true); {
			case err != nil:
				return err
			default:
				chunk = i

				return nil
			}
		}
	}

	_, doneACCEPT, _ := NewContextDoneFunc(
		w.ctx(),
		func(ctx context.Context) error {
			if err := saveACCEPT(ctx); err != nil {
				_, _ = saveDone(false)

				return err
			}

			return nil
		},
	)

	w.SetACCEPTVoteproof = func(vp base.ACCEPTVoteproof) (string, error) {
		avpch <- vp

		if err := doneACCEPT(); err != nil {
			return "", err
		}

		return chunk, nil
	}

	return w, nil
}

type (
	writerSaveTreeNodeFunc  func(uint64, fixedtree.Node) error
	writerCloseSaveFunc     func(bool) (string, error)
	writerSaveTreeNodeFuncs func(context.Context) (writerSaveTreeNodeFunc, writerCloseSaveFunc, error)
)

type baseFixedTreeWriter[K cmp.Ordered] struct {
	*BaseBlockItemWriter
	*logging.Logging
	add      func(k K, node fixedtree.Node) error
	treeRoot func() (util.Hash, error)
	saveDone func() (string, error)
}

func NewBaseFixedTreeWriter[K cmp.Ordered](
	ctx context.Context,
	newJobWorker func(context.Context) (JobWorker, error),
	ht hint.Hint,
	saveNodeFuncs writerSaveTreeNodeFuncs,
	minNodesSize uint64,
) (*baseFixedTreeWriter[K], error) {
	w := &baseFixedTreeWriter[K]{
		Logging: logging.NewLogging(func(zctx zerolog.Context) zerolog.Context {
			return zctx.Str("module", "fixedtree-writer").Stringer("hint", ht)
		}),
		BaseBlockItemWriter: NewBaseBlockItemWriter(ctx, newJobWorker),
	}

	var treeDone func() error
	var saveNodeFunc func(uint64, fixedtree.Node) error

	switch i, j, k, err := w.newTreeFuncs(
		ht,
		minNodesSize,
		func() func(uint64, fixedtree.Node) error { return saveNodeFunc },
	); {
	case err != nil:
		w.ctxCancel(err)

		return nil, err
	default:
		treeDone = k
		w.add = i
		w.treeRoot = j
	}

	switch i, j, err := w.newSaveFuncs(saveNodeFuncs, treeDone); {
	case err != nil:
		w.ctxCancel(err)

		return nil, err
	default:
		saveNodeFunc = i
		w.saveDone = j
	}

	return w, nil
}

func (w *baseFixedTreeWriter[K]) Add(k K, node fixedtree.Node) error {
	if err := w.add(k, node); err != nil {
		w.ctxCancel(err)

		return err
	}

	return nil
}

func (w *baseFixedTreeWriter[K]) Tree() (util.Hash, error) {
	switch i, err := w.treeRoot(); {
	case err != nil:
		w.ctxCancel(err)

		return nil, err
	default:
		return i, nil
	}
}

func (w *baseFixedTreeWriter[K]) Save() (chunk string, _ error) {
	switch i, err := w.saveDone(); {
	case err != nil:
		w.ctxCancel(err)

		return "", err
	default:
		chunk = i
	}

	return chunk, nil
}

func (w *baseFixedTreeWriter[K]) buildTree(
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

func (w *baseFixedTreeWriter[K]) newTreeFuncs(
	ht hint.Hint,
	minNodesSize uint64,
	saveNodeFunc func() func(uint64, fixedtree.Node) error,
) (
	add func(K, fixedtree.Node) error,
	treeRoot func() (util.Hash, error),
	treeDone func() error,
	_ error,
) {
	var tew JobWorker
	elapsed := func(*zerolog.Event, string) {}

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

				elapsed(w.Log().Info(), "generated")
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
				saveNodeFunc(),
			); {
			case err != nil:
				return err
			default:
				defer tr.Close()

				root = tr.Root()

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
	var gerr error

	return func(k K, node fixedtree.Node) error {
			addNodesOnce.Do(func() {
				elapsed = logging.TimeElapsed()

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

			return tew.NewJob(func(context.Context) error {
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

func (w *baseFixedTreeWriter[K]) newSaveFuncs(
	saveNodeFuncs writerSaveTreeNodeFuncs,
	treeDone func() error,
) (
	saveNodeFunc func(uint64, fixedtree.Node) error,
	saveDone func() (string, error),
	_ error,
) {
	var closeSaveNodeFunc func(bool) (string, error)
	var chunk string

	var add util.CompactAppendSliceAddFunc[[2]interface{}]
	var done util.CompactAppendSliceDoneFunc[[2]interface{}]

	switch i, j, err := util.CompactAppendSliceV1[[2]interface{}](1<<9, 1<<9); {
	case err != nil:
		return nil, nil, err
	default:
		add, done = i, j
	}

	var sew JobWorker
	var save func(uint64, fixedtree.Node) error

	sctx, sDone, _ := NewContextDoneFunc(
		w.ctx(),
		func(ctx context.Context) error {
			defer logging.TimeElapsed()(w.Log().Info(), "saved")

			if err := func() error {
				defer sew.Close()

				if err := treeDone(); err != nil {
					return err
				}

				if err := done(func(s [][2]interface{}) error {
					return util.TraverseSliceV1(s, func(_ int, i [2]interface{}) (bool, error) {
						return true, sew.NewJob(func(context.Context) error {
							return save(
								i[0].(uint64),
								i[1].(fixedtree.Node),
							)
						})
					})
				}); err != nil {
					return err
				}

				sew.Done()

				return sew.Wait()
			}(); err != nil {
				_, _ = closeSaveNodeFunc(false)

				return err
			}

			switch i, err := closeSaveNodeFunc(true); {
			case err != nil:
				return err
			default:
				chunk = i

				return nil
			}
		},
	)

	switch i, err := w.newJobWorker(sctx); {
	case err != nil:
		return nil, nil, err
	default:
		sew = i
	}

	switch i, j, err := saveNodeFuncs(sctx); {
	case err != nil:
		return nil, nil, err
	default:
		save = i
		closeSaveNodeFunc = j
	}

	return func(index uint64, node fixedtree.Node) error {
			add([2]interface{}{index, node})

			return nil
		},
		func() (string, error) {
			if err := sDone(); err != nil {
				return "", err
			}

			return chunk, nil
		},
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

func NewCompressedFile(fpath string) (io.WriteCloser, error) {
	f, err := os.OpenFile(fpath, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0o600)
	if err != nil {
		return nil, err
	}

	g, err := util.NewGzipWriter(f, gzip.BestSpeed)
	if err != nil {
		_ = f.Close()

		return nil, err
	}

	return g, nil
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

func NewWriterSaveBlockItemFuncs(
	db isaac.BlockWriteDatabase,
	root string,
	enc encoder.Encoder,
) writerSaveBlockItemFuncs {
	return func(ctx context.Context, t base.BlockItemType) (writerSaveBlockItemFunc, writerCloseSaveFunc, error) {
		switch t {
		case base.BlockItemProposal:
			return newWriterSaveBlockItemFileFuncs(root, enc.Marshal, "proposal.json.gz")(ctx, t)
		case base.BlockItemVoteproofs:
			return newWriterSaveBlockItemFileFuncs(root, enc.Marshal, "voteproofs.json.gz")(ctx, t)
		case base.BlockItemStatesTree:
			return newWriterSaveBlockItemFileFuncs(root, enc.Marshal, "states_tree.json.gz")(ctx, t)
		case base.BlockItemMap:
			switch i, j, err := newWriterSaveBlockItemFileFuncs(root, enc.Marshal, "map.json")(ctx, t); {
			case err != nil:
				return nil, nil, err
			default:
				return func(v interface{}) error {
					if err := i(v); err != nil {
						return err
					}

					return db.SetBlockMap(v.(base.BlockMap))
				}, j, nil
			}
		default:
			return nil, nil, util.ErrNotFound.Errorf("unknown item type, %q", t)
		}
	}
}

func newWriterSaveBlockItemFileFuncs(
	root string,
	encode func(interface{}) ([]byte, error),
	fname string,
) writerSaveBlockItemFuncs {
	fpath := filepath.Join(filepath.Clean(root), fname)

	return func(ctx context.Context, _ base.BlockItemType) (writerSaveBlockItemFunc, writerCloseSaveFunc, error) {
		var fonce sync.Once

		var f io.WriteCloser
		var gerr error

		return func(i interface{}) error {
				fonce.Do(func() {
					switch i, err := NewCompressedFile(fpath); {
					case err != nil:
						gerr = err
						return
					default:
						f = i
					}
				})

				if gerr != nil {
					return gerr
				}

				switch b, err := encode(i); {
				case err != nil:
					return err
				default:
					if _, err := f.Write(append(b, []byte("\n")...)); err != nil {
						return err
					}

					return nil
				}
			},
			func(ok bool) (string, error) {
				if f == nil {
					return "", nil
				}

				if err := f.Close(); err != nil {
					return "", err
				}

				if !ok {
					if err := os.Remove(fpath); err != nil {
						return "", err
					}
				}

				return fpath, nil
			},
			nil
	}
}

var blockItemMapTree base.BlockItemType = "tree"

func newWriterSaveTreeFuncs(
	root string,
	enc encoder.Encoder,
	t base.BlockItemType,
) (writerSaveTreeNodeFuncs, error) {
	var fname string

	switch t {
	case base.BlockItemStatesTree:
		fname = "states_tree.json.gz"
	case base.BlockItemOperationsTree:
		fname = "operations_tree.json.gz"
	default:
		return nil, errors.Errorf("unknown tree type, %q", t)
	}

	return func(ctx context.Context) (writerSaveTreeNodeFunc, writerCloseSaveFunc, error) {
		var fsave writerSaveBlockItemFunc
		var fclose writerCloseSaveFunc

		nodeEncode := func(i interface{}) ([]byte, error) {
			j := i.([2]interface{})
			index, node := j[0].(uint64), j[1].(fixedtree.Node)

			switch b, err := enc.Marshal(node); {
			case err != nil:
				return nil, err
			default:
				return util.ConcatBytesSlice(
					[]byte(fmt.Sprintf("%d,", index)),
					b,
				), nil
			}
		}

		switch i, j, err := newWriterSaveBlockItemFileFuncs(root, nodeEncode, fname)(ctx, blockItemMapTree); {
		case err != nil:
			return nil, nil, err
		default:
			fsave = i
			fclose = j
		}

		return func(index uint64, node fixedtree.Node) error {
				return fsave([2]interface{}{index, node})
			},
			fclose,
			nil
	}, nil
}

func newWriterSaveStatesFuncs(
	db isaac.BlockWriteDatabase,
	root string,
	enc encoder.Encoder,
	fname string,
) saveStateFuncs {
	return func(ctx context.Context) (processWorkerSaveStateFunc, writerCloseSaveFunc, error) {
		var fsave writerSaveBlockItemFunc
		var fclose writerCloseSaveFunc

		switch i, j, err := newWriterSaveBlockItemFileFuncs(root, enc.Marshal, fname)(ctx, base.BlockItemStates); {
		case err != nil:
			return nil, nil, err
		default:
			fsave = i
			fclose = j
		}

		return func(st base.State) error {
				if err := fsave(st); err != nil {
					return err
				}

				return db.SetStates([]base.State{st})
			},
			fclose,
			nil
	}
}

func newWriterSaveOperationsFuncs(
	db isaac.BlockWriteDatabase,
	root string,
	enc encoder.Encoder,
	fname string,
) saveOperationFuncs {
	return func(ctx context.Context) (processWorkerSaveOperationFunc, writerCloseSaveFunc, error) {
		nodeEncode := func(i interface{}) ([]byte, error) {
			j := i.([2]interface{})
			index, op := j[0].(uint64), j[1].(Operation)

			switch b, err := enc.Marshal(op); {
			case err != nil:
				return nil, err
			default:
				return util.ConcatBytesSlice([]byte(fmt.Sprintf("%d,", index)), b), nil
			}
		}

		var fsave writerSaveBlockItemFunc
		var fclose writerCloseSaveFunc

		switch i, j, err := newWriterSaveBlockItemFileFuncs(root, nodeEncode, fname)(ctx, base.BlockItemOperations); {
		case err != nil:
			return nil, nil, err
		default:
			fsave = i
			fclose = j
		}

		return func(index uint64, op Operation) error {
				if err := fsave([2]interface{}{index, op}); err != nil {
					return err
				}

				return db.SetOperations([]util.Hash{op.Hash()})
			},
			fclose,
			nil
	}
}
