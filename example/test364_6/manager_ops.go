package dop6

import (
	"context"
	"iter"
	"maps"

	"github.com/pkg/errors"
	dop5 "github.com/spikeekips/mitum/example/test364_5"
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

func newManagerCollectUnmergedOps(
	ctx context.Context,
	opsSize uint64,
	checkWorker dop5.CheckFailedWorkersFunc,
) (
	dutil.In2ErrorFunc[string, map[uint64]string],
	dutil.InErrorFunc[dutil.InErrorFunc[iter.Seq2[uint64, string]]],
	error,
) {
	var pool dutil.LockedMap[uint64, string]

	switch m, err := dutil.NewLockedMap[uint64, string](nil, opsSize); {
	case err != nil:
		return nil, nil, err
	default:
		pool = m
	}

	done, wait := dutil.NewContextDone(ctx)

	return func(worker string, unmerged map[uint64]string) error {
			switch ok, enough, failed := checkWorker(worker, nil); {
			case !ok:
			case failed != nil:
				return done(failed)
			default:
				for opindex, reason := range maps.All(unmerged) {
					_ = pool.SetValue(opindex, reason)
				}

				if enough {
					_ = done(nil)
				}
			}

			return nil
		},
		func(f dutil.InErrorFunc[iter.Seq2[uint64, string]]) error {
			defer pool.Close()

			if err := wait(); err != nil {
				return err
			}

			return f(func(yield func(uint64, string) bool) {
				for i := range opsSize {
					opindex := uint64(i)

					reason, _ := pool.Value(opindex)
					if !yield(opindex, reason) {
						return
					}
				}
			})
		},
		nil
}
