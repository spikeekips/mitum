package dop6

import (
	"context"
	"slices"

	dop5 "github.com/spikeekips/mitum/example/test364_5"
	dutil "github.com/spikeekips/mitum/example/test364_5/util"
	"github.com/spikeekips/mitum/util"
)

type (
	managerDistributeOpsWorkerFunc    = dutil.In2ErrorFunc[uint64 /* items */, dutil.InErrorFunc[opindexOpHashesFunc]]
	newManagerDistributeOpsWorkerFunc = func(worker string) (managerDistributeOpsWorkerFunc, error)
)

type managerRequestStartWorkerFunc func(
	_ context.Context,
	worker string,
	opsShare, statesShare uint64,
	opsWorkers []string,
	maxOpsSize uint64,
) error

var managerName = "manager"

func NewManager(
	ctx context.Context,
	workers []string,
	opsShare, statesShare uint64,
	ophs [][2]util.Hash,
) error {
	wctx, wctxCancel := dutil.ContextWithCancelCause(ctx)

	var opsWorkers []string
	var opsDistribute dutil.InErrorFunc[newManagerDistributeOpsWorkerFunc]
	switch o, d, err := newManagerDistributeOps(
		wctx,
		workers,
		opsShare,
		ophs,
	); {
	case err != nil:
		return err
	default:
		opsWorkers, opsDistribute = o, d
	}

	_, _ = func() (
		func(string, []string, []string) error,
		func(string, map[uint64]string) error,
		error,
	) {
		// FIXME receive worker errors
		// FIXME receive state owners
		// FIXME receive unmerged ops
		// FIXME receive states

		return nil, nil, nil
	},
		func(
			requestStartWorker managerRequestStartWorkerFunc,
			workerDistribute newManagerDistributeOpsWorkerFunc,
		) error {
			defer func() {
				_ = wctxCancel(nil)
			}()

			if err := newManagerStartWorkers(
				wctx,
				requestStartWorker,
				workers,
				opsWorkers,
				opsShare, statesShare,
				uint64(len(ophs)),
			); err != nil {
				return err
			}

			if err := opsDistribute(workerDistribute); err != nil {
				return err
			}

			// FIXME generate states tree and ops tree
			// FIXME generate manifest

			return nil
		}

	return nil
}

func newManagerStartWorkers(
	ctx context.Context,
	requestStartWorker managerRequestStartWorkerFunc,
	workers, opsWorkers []string,
	opsShare, statesShare uint64,
	maxOpsSize uint64,
) error {
	var newJob dutil.JobWorkerNewJobFunc
	var jobDone func()
	var jobWait dutil.InErrorFunc[error]

	switch n, j, w, err := dutil.NewFuncJobWorker(ctx, int64(len(workers))); {
	case err != nil:
		return err
	default:
		newJob, jobDone, jobWait = n, j, w
	}

	var checkWorker dop5.CheckFailedWorkersFunc

	switch w, err := dop5.NewFuncCheckFailedWorkers(uint64(len(opsWorkers)), opsShare, func(string) bool { return true }); {
	case err != nil:
		return err
	default:
		checkWorker = w
	}

	for worker := range slices.Values(workers) {
		if err := newJob(func(ctx context.Context) error {
			err := requestStartWorker(ctx, worker, opsShare, statesShare, opsWorkers, maxOpsSize)
			if slices.Index(opsWorkers, worker) >= 0 {
				_, _, failed := checkWorker(worker, err)

				return failed
			}

			return nil
		}); err != nil {
			return err
		}
	}

	jobDone()

	return jobWait(nil)
}
