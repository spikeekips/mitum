package fixedtree

import (
	"context"
	"iter"
	"sync"
	"sync/atomic"

	"github.com/pkg/errors"
	dutil "github.com/spikeekips/mitum/example/test364_5/util"
	"github.com/spikeekips/mitum/util"
)

var (
	errNoParent   = util.NewIDError("no parent")
	errNoChildren = util.NewIDError("no children")
)

type (
	NodeWriteFunc         = func(uint64, Node) error
	GenerateNodesHashFunc = func(context.Context, []Node, NodeWriteFunc) error
)

func WriteTree(ctx context.Context, semSize uint64, tr *Tree, w NodeWriteFunc, g GenerateNodesHashFunc) error {
	switch {
	case tr.Len() < 1:
		return nil
	case tr.Root() == nil:
		return g(ctx, tr.Nodes(), w)
	default:
		return baseWriteNodes(ctx, semSize, tr.Nodes(), w)
	}
}

func WriteNodes(ctx context.Context, semSize uint64, nodes []Node, w NodeWriteFunc, g GenerateNodesHashFunc) error {
	switch {
	case len(nodes) < 1:
		return nil
	case nodes[0] == nil:
		return errors.Errorf("nil root")
	case nodes[0].Hash() == nil:
		if g == nil {
			return nil
		}

		return g(ctx, nodes, w)
	default:
		return baseWriteNodes(ctx, semSize, nodes, w)
	}
}

func baseWriteNodes(ctx context.Context, semSize uint64, nodes []Node, w NodeWriteFunc) error {
	if w == nil {
		return nil
	}

	wctx, cancel := dutil.ContextWithCancelCause(ctx)
	defer cancel(nil)

	var newJob dutil.JobWorkerNewJobFunc
	var jobDone func()
	var jobWait dutil.InErrorFunc[error]

	switch n, d, w, err := dutil.NewFuncJobWorker(wctx, semSize); {
	case err != nil:
		return err
	default:
		newJob, jobDone, jobWait = n, d, w
	}

	go func() {
		defer jobDone()

		_ = dutil.TraverseSlice(nodes, func(i int, n Node) (bool, error) {
			if n == nil {
				return false, cancel(errors.Errorf("nil node at %d", i))
			}

			return true, newJob(func(context.Context) error {
				return w(uint64(i), n)
			})
		})
	}()

	return jobWait(nil)
}

func GenerateNodeHash(index uint64, nodes []Node, w NodeWriteFunc) error {
	n := nodes[index]
	if n == nil {
		return errors.Errorf("nil node at %d", index)
	}

	var children [2]Node

	switch c, err := childrenNodes(index, nodes); {
	case errors.Is(err, errNoChildren):
	case err == nil:
		children = c
	}

	switch _, c, err := n.SetHash(children[0], children[1]); {
	case err != nil:
		return err
	default:
		if err := c(); err != nil {
			return err
		}
	}

	if w != nil {
		return w(index, n)
	}

	return nil
}

func SyncronousGenerateNodesHash(_ context.Context, nodes []Node, w NodeWriteFunc) error {
	size := uint64(len(nodes))

	for i := range size {
		index := size - 1 - i

		if err := GenerateNodeHash(index, nodes, w); err != nil {
			return err
		}
	}

	return nil
}

func asyncGenerateNodeHash(
	index uint64,
	nodes []Node,
	cc []uint32,
	w NodeWriteFunc,
) (uint64, error) {
	if err := GenerateNodeHash(index, nodes, w); err != nil {
		return index, err
	}

	if index < 1 {
		return index, nil
	}

	var p uint64

	switch i, err := parent(index); {
	case err != nil:
		return index, err
	default:
		p = i
	}

	switch {
	case index == uint64(len(nodes))-1 && index%2 == 1, // NOTE last odd index; no sibling
		!atomic.CompareAndSwapUint32(&cc[p], 0, 1):
		return p, nil
	default:
		return index, nil
	}
}

func AsyncGenerateNodesHashByFollowingParent(
	ctx context.Context,
	nodes []Node,
	semSize uint64,
	w NodeWriteFunc,
) error {
	switch {
	case len(nodes) < 1:
		return nil
	case len(nodes) < 2:
		_, err := asyncGenerateNodeHash(0, nodes, nil, w)

		return err
	}

	nnodes := uint64(len(nodes))

	last := indexHeight(nnodes - 1)
	if last < 2 {
		return SyncronousGenerateNodesHash(ctx, nodes, w)
	}

	var cc []uint32
	var ncc uint64

	{
		ln := lenNodesInHeight(nnodes, last)
		ncc = nnodes - ln - lenNodesInHeight(nnodes, last-1)

		switch {
		case ln%2 == 1:
			ncc += ((ln - 1) / 2) + 1
		default:
			ncc += (ln) / 2
		}

		cc = make([]uint32, ncc)
		defer dutil.ClearSlice(cc)
	}

	wctx, cancel := dutil.ContextWithCancelCause(ctx)
	defer cancel(nil)

	var newJob dutil.JobWorkerNewJobFunc
	var jobDone func()
	var jobWait dutil.InErrorFunc[error]

	switch n, d, w, err := dutil.NewFuncJobWorker(wctx, semSize); {
	case err != nil:
		return err
	default:
		defer func() {
			_ = jobWait(nil)
		}()

		newJob, jobDone, jobWait = n, d, w
	}

	var gen func(index uint64) error
	gen = func(index uint64) error {
		if err := newJob(func(context.Context) error {
			switch p, err := asyncGenerateNodeHash(index, nodes, cc, w); {
			case err != nil:
				return err
			case p < index:
				return gen(p)
			default:
				return nil
			}
		}); err != nil {
			return err
		}

		if index == 0 {
			jobDone()
		}

		return nil
	}

	gen = func(index uint64) error {
		if index == 0 {
			defer jobDone()
		}

		return newJob(func(context.Context) error {
			switch p, err := asyncGenerateNodeHash(index, nodes, cc, w); {
			case err != nil:
				return err
			case p < index:
				go func() {
					if err := gen(p); err != nil {
						cancel(err)
					}
				}()
			}

			return nil
		})
	}

	for i := range nnodes {
		index := nnodes - i - 1
		if index < uint64(len(cc)) {
			break
		}

		if err := gen(index); err != nil {
			return err
		}
	}

	return jobWait(nil)
}

func AsyncGenerateNodesHashByHeight(
	ctx context.Context,
	nodes []Node,
	w NodeWriteFunc,
) error {
	switch {
	case len(nodes) < 1:
		return nil
	case len(nodes) < 2:
		_, err := asyncGenerateNodeHash(0, nodes, nil, w)

		return err
	}

	nnodes := uint64(len(nodes))

	if last := indexHeight(nnodes - 1); last < 2 {
		return SyncronousGenerateNodesHash(ctx, nodes, w)
	}

	var next func() (uint64, iter.Seq[uint64], bool)
	var stop func()

	switch i, err := reverseByHeight(nnodes); {
	case err != nil:
		return err
	default:
		next, stop = iter.Pull2(i)
		defer stop()
	}

end:
	for {
		switch _, is, ok := next(); {
		case !ok:
			break end
		default:
			if err := generateNodesHash(ctx, nodes, w, is); err != nil {
				return err
			}
		}
	}

	return nil
}

func generateNodesHash(ctx context.Context, nodes []Node, w NodeWriteFunc, is iter.Seq[uint64]) error {
	next, stop := iter.Pull(is)
	defer stop()

	var newJob dutil.JobWorkerNewJobFunc
	var jobDone func()
	var jobWait dutil.InErrorFunc[error]

	var once sync.Once

end:
	for {
		switch i, ok := next(); {
		case !ok:
			break end
		default:
			var gerr error

			once.Do(func() {
				switch n, d, w, err := dutil.NewFuncJobWorker(ctx, 1<<9); {
				case err != nil:
					gerr = err
				default:
					newJob, jobDone, jobWait = n, d, w
				}
			})
			if gerr != nil {
				return gerr
			}

			if err := newJob(func(context.Context) error {
				return GenerateNodeHash(i, nodes, w)
			}); err != nil {
				return err
			}
		}
	}

	if jobDone == nil {
		return nil
	}

	jobDone()

	return jobWait(nil)
}

func NewFuncAsyncGenerateNodesHashByFollowingParent(semSize uint64) GenerateNodesHashFunc {
	return func(ctx context.Context, nodes []Node, w NodeWriteFunc) error {
		return AsyncGenerateNodesHashByFollowingParent(
			ctx,
			nodes,
			semSize,
			w,
		)
	}
}
