package dop6

import (
	"context"
	"fmt"
	"slices"

	dop5 "github.com/spikeekips/mitum/example/test364_5"
	"github.com/spikeekips/mitum/example/test364_5/fixedtree"
	dutil "github.com/spikeekips/mitum/example/test364_5/util"
	"github.com/spikeekips/mitum/util"
)

type (
	managerDistributeOpsWorkerFunc    = dutil.In2ErrorFunc[uint64 /* items */, dutil.InErrorFunc[opindexOpHashesFunc]]
	newManagerDistributeOpsWorkerFunc = func(worker string) (managerDistributeOpsWorkerFunc, error)
	managerStartWorkerFunc            = func(managerRequestStartWorkerFunc) (
		dutil.InErrorFunc[newManagerDistributeOpsWorkerFunc], // NOTE distribute ops
		dutil.In3ErrorFunc[string /* worker */, []string /* subs */, []string /* affected */], // NOTE receive state owners
		managerWorkerUnmergedFunc, // NOTE receive unmerged ops
		managerWorkerStatesFunc, // NOTE receive states
		error,
	)
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
) (
	dutil.In2ErrorFunc[string, error],
	managerStartWorkerFunc,
	dutil.InErrorFunc[fixedtree.GenerateNodesHashFunc],
	error,
) {
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
		return nil, nil, nil, err
	default:
		opsWorkers, opsDistribute = o, d
	}

	var workerOwners dutil.In3ErrorFunc[string, []string, []string]
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
		return nil, nil, nil, err
	default:
		workerOwners, checkFailedSubs, checkFailedOwners, waitOwners = w, cs, co, o
	}

	var workerUnmerged managerWorkerUnmergedFunc
	var workerOpsTreeWait managerFixedTreeWaitFunc

	switch u, w, err := newManagerUnmergedOps(wctx, ophs, checkFailedSubs); {
	case err != nil:
		return nil, nil, nil, err
	default:
		workerUnmerged, workerOpsTreeWait = u, w
	}

	var workerStates managerWorkerStatesFunc
	var workerStatesTreeWait managerFixedTreeWaitFunc

	switch s, w, err := newManagerStates(wctx, opsSize*2, checkFailedOwners); {
	case err != nil:
		return nil, nil, nil, err
	default:
		workerStates, workerStatesTreeWait = s, w

	}

	return func(worker string, err error) error { // NOTE receive worker errors
			if err == nil {
				panic(fmt.Sprintf("worker=%q nil error", worker))
			}

			if ok, _, failed := checkFailedOwners(worker, err); ok && failed != nil {
				_ = wctxCancel(failed)
			}

			return nil
		},
		func(request managerRequestStartWorkerFunc) (
			func(newManagerDistributeOpsWorkerFunc) error,
			func(string, []string, []string) error,
			managerWorkerUnmergedFunc,
			managerWorkerStatesFunc,
			error,
		) {
			if err := newManagerStartWorkers(
				wctx,
				request,
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
				workerStates,
				nil
		},
		func(g fixedtree.GenerateNodesHashFunc) error {
			defer func() {
				_ = wctxCancel(nil)
			}()

			if _, _, err := waitOwners(); err != nil {
				return wctxCancel(err)
			}

			var opsTree, statesTree *fixedtree.Tree

			if err := dutil.FuncsJobWorker(
				wctx,
				func(context.Context) error {
					switch t, err := workerOpsTreeWait(g); {
					case err != nil:
						return err
					default:
						opsTree = t

						return nil
					}
				},
				func(context.Context) error {
					switch t, err := workerStatesTreeWait(g); {
					case err != nil:
						return err
					default:
						statesTree = t

						return nil
					}
				},
			); err != nil {
				return wctxCancel(err)
			}

			// FIXME generate manifest
			_, _ = opsTree, statesTree

			return nil
		},
		nil
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

	switch w, err := dop5.NewFuncCheckFailedWorkers(
		uint64(len(opsWorkers)), opsShare, func(string) bool { return true }); {
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
