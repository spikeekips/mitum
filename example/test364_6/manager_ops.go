package dop6

import (
	"context"
	"iter"
	"maps"
	"slices"

	"github.com/pkg/errors"
	dop5 "github.com/spikeekips/mitum/example/test364_5"
	"github.com/spikeekips/mitum/example/test364_5/fixedtree"
	dutil "github.com/spikeekips/mitum/example/test364_5/util"
	"github.com/spikeekips/mitum/util"
)

func newManagerDistributeOps(
	ctx context.Context,
	workers []string,
	share uint64,
	ophs [][2]util.Hash,
) ([]string, dutil.In2ErrorFunc[newManagerDistributeOpsWorkerFunc, dutil.In2ErrorFunc[string, error]], error) {
	switch {
	case len(workers) < 1:
		return nil, nil, errors.New("empty workers")
	case share < 1:
		return nil, nil, errors.New("zero share")
	case len(ophs) < 1:
		return nil, nil, errors.New("empty ophs")
	}

	var newJob dutil.JobWorkerNewJobFunc
	var jobDone func()
	var jobWait dutil.InErrorFunc[error]

	switch n, j, w, err := dutil.NewFuncJobWorker(ctx, int64(len(workers))); {
	case err != nil:
		return nil, nil, err
	default:
		newJob, jobDone, jobWait = n, j, w
	}

	opsWorkers, seq := newOpsWorkersItems(workers, share, uint64(len(ophs)))

	var checkWorker dop5.CheckFailedWorkersFunc

	switch w, err := dop5.NewFuncCheckFailedWorkers(uint64(len(opsWorkers)), share, func(string) bool { return true }); {
	case err != nil:
		return nil, nil, err
	default:
		checkWorker = w
	}

	return opsWorkers, func(
			distribute newManagerDistributeOpsWorkerFunc,
			workerErr dutil.In2ErrorFunc[string, error],
		) error {
			for worker, q := range seq {
				if err := newJob(func(ctx context.Context) error {
					err := managerDistributeOpsToWorkers(ctx, ophs, distribute, worker, q)

					if _, _, failed := checkWorker(worker, err); failed != nil {
						return failed
					}

					return workerErr(worker, err)
				}); err != nil {
					return err
				}
			}

			jobDone()

			return jobWait(nil)
		},
		nil
}

func managerDistributeOpsToWorkers(
	ctx context.Context,
	ophs [][2]util.Hash,
	distribute newManagerDistributeOpsWorkerFunc,
	worker string,
	seqf func() (uint64, iter.Seq[uint64]),
) error {
	var f managerDistributeOpsWorkerFunc

	switch i, err := distribute(worker); {
	case err != nil:
		return err
	default:
		f = i
	}

	items, seq := seqf()

	return f(items, func(of opindexOpHashesFunc) error {
		for opindex := range seq {
			if err := of(opindex, ophs[opindex][0], ophs[opindex][1]); err != nil {
				return err
			}
		}

		return nil
	})
}

func newManagerUnmergedOps(
	ctx context.Context,
	ophs [][2]util.Hash,
	checkWorker dop5.CheckFailedWorkersFunc,
) (
	dutil.In2ErrorFunc[string, map[uint64]string],
	managerFixedTreeWaitFunc,
	error,
) {
	wctx, wctxCancel := dutil.ContextWithCancelCause(ctx)

	opsSize := uint64(len(ophs))
	var pool dutil.LockedMap[uint64, fixedtree.Node]

	switch m, err := dutil.NewLockedMap[uint64, fixedtree.Node](nil, opsSize); {
	case err != nil:
		return nil, nil, err
	default:
		pool = m
	}

	var newJob dutil.JobWorkerNewJobFunc
	var jobDone func()
	var jobWait dutil.InErrorFunc[error]

	switch n, d, w, err := dutil.NewFuncJobWorker(wctx, dutil.SafeUint64ToInt64(opsSize)); {
	case err != nil:
		return nil, nil, err
	default:
		newJob, jobDone, jobWait = n, d, w
	}

	return func(worker string, unmerged map[uint64]string) error {
			switch ok, enough, failed := checkWorker(worker, nil); {
			case !ok:
			case failed != nil:
				return wctxCancel(failed)
			default:
				for opindex, reason := range maps.All(unmerged) {
					switch {
					case opindex >= opsSize:
						return errors.Errorf("unknown opindex, %d", opindex)
					case reason == "":
						return errors.Errorf("empty reason, %d", opindex)
					}

					switch err := newJob(func(context.Context) error {
						hs := ophs[opindex]

						switch n, err := dop5.NewOperationTreeNode(hs[0], hs[1], reason); {
						case err != nil:
							return err
						default:
							_ = pool.SetValue(opindex, n)

							return nil
						}
					}); {
					case err == nil:
					case !errors.Is(err, dutil.ErrJobWorkerDone):
						return err
					}
				}

				if enough {
					jobDone()
				}
			}

			return nil
		},
		func(g fixedtree.GenerateNodesHashFunc) (*fixedtree.Tree, error) {
			defer func() {
				pool.Close()
				wctxCancel(nil)
			}()

			if err := jobWait(nil); err != nil {
				return nil, err
			}

			nodes := make([]fixedtree.Node, len(ophs))

			for i, hs := range slices.All(ophs) {
				opindex := uint64(i)

				n, found := pool.Value(opindex)
				if !found {
					switch j, err := dop5.NewOperationTreeNode(hs[0], hs[1], ""); {
					case err != nil:
						return nil, err
					default:
						n = j
					}
				}

				nodes[i] = n
			}

			if err := g(ctx, nodes, nil); err != nil {
				return nil, err
			}

			return dop5.NewStatesTree(nodes), nil
		},
		nil
}
