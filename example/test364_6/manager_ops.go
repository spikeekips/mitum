package dop6

import (
	"context"
	"iter"

	"github.com/pkg/errors"
	dutil "github.com/spikeekips/mitum/example/test364_5/util"
	"github.com/spikeekips/mitum/util"
)

func newManagerDistributeOps(
	ctx context.Context,
	workers []string,
	share uint64,
	ophs [][2]util.Hash,
	distribute newManagerDistributeOpsWorkerFunc,
) ([]string, dutil.ErrorFunc, error) {
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

	return opsWorkers, func() error {
			for worker, q := range seq {
				if err := newJob(func(ctx context.Context) error {
					return managerDistributeOpsToWorkers(ctx, ophs, distribute, worker, q)
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
