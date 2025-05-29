package dop6

import (
	"context"
	"fmt"
	"iter"
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
	opsSize := uint64(len(ophs))

	wctx, wctxCancel := dutil.ContextWithCancelCause(ctx)

	var opsWorkers []string
	var opsDistribute dutil.In2ErrorFunc[newManagerDistributeOpsWorkerFunc, dutil.In2ErrorFunc[string, error]]
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

	var workerOwners func(string, []string, []string) error
	var checkFailedSubs dop5.CheckFailedWorkersFunc
	var checkFailedOwners dop5.CheckFailedWorkersFunc
	var waitOwners func() ([]string, []string, error)

	switch w, cs, co, o, err := newManagerStateOwners(
		wctx,
		opsWorkers,
		opsShare, statesShare,
		uint64(len(workers)),
	); {
	case err != nil:
		return err
	default:
		workerOwners, checkFailedSubs, checkFailedOwners, waitOwners = w, cs, co, o
		_, _, _, _ = workerOwners, checkFailedSubs, checkFailedOwners, waitOwners // FIXME remove
	}

	var workerUnmerged dutil.In2ErrorFunc[string, map[uint64]string]
	var workerUnmergedWait dutil.InErrorFunc[dutil.InErrorFunc[iter.Seq2[uint64, string]]]

	switch u, w, err := newManagerCollectUnmergedOps(wctx, opsSize, checkFailedSubs); {
	case err != nil:
		return err
	default:
		workerUnmerged, workerUnmergedWait = u, w
		_, _ = workerUnmerged, workerUnmergedWait
	}

	_, _, _ = func(worker string, err error) error { // FIXME receive worker errors
		if err == nil {
			panic(fmt.Sprintf("worker=%q nil error", worker))
		}

		if ok, _, failed := checkFailedOwners(worker, err); ok && failed != nil {
			_ = wctxCancel(failed)
		}

		return nil
	}, func(
		requestStartWorker managerRequestStartWorkerFunc,
		workerDistribute newManagerDistributeOpsWorkerFunc,
	) (
		func(newManagerDistributeOpsWorkerFunc) error, // FIXME distribute ops
		func(string, []string, []string) error, // FIXME receive state owners
		func(string, map[uint64]string) error, // FIXME receive unmerged ops
		func() error, // FIXME receive states
		error,
	) {
		if err := newManagerStartWorkers(
			wctx,
			requestStartWorker,
			workers,
			opsWorkers,
			opsShare, statesShare,
			opsSize,
		); err != nil {
			return nil, nil, nil, nil, wctxCancel(err)
		}

		return func(workerDistribute newManagerDistributeOpsWorkerFunc) error {
				if err := opsDistribute(
					workerDistribute,
					func(worker string, err error) error {
						if err != nil {
							if ok, _, failed := checkFailedSubs(worker, err); ok && failed != nil {
								return failed
							}

							return nil
						}

						return nil
					},
				); err != nil {
					return wctxCancel(err)
				}

				return nil
			},
			workerOwners,
			workerUnmerged,
			nil,
			nil
	}, func() error { // FIXME wait
		defer func() {
			_ = wctxCancel(nil)
		}()

		if _, _, err := waitOwners(); err != nil {
			return wctxCancel(err)
		}

		if err := workerUnmergedWait(func(seq iter.Seq2[uint64, string]) error {
			// FIXME generate states tree and ops tree

			return nil
		}); err != nil {
			return wctxCancel(err)
		}

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
