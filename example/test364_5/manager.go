package main

import (
	"context"
	"slices"

	"github.com/pkg/errors"
	"github.com/rs/zerolog"
	"github.com/spikeekips/mitum/base"
	"github.com/spikeekips/mitum/example/test364_5/fixedtree"
	dutil "github.com/spikeekips/mitum/example/test364_5/util"
	"github.com/spikeekips/mitum/util"
)

var managerName = "manager"

type (
	managerStartWorkerFunc = func(
		_ context.Context,
		workID, worker string,
		workers, opsWorkers []string,
		opsShare, statesShare uint64,
		maxOpsSize uint64,
	) error
	managerCancelWorkerFunc    = func(_ context.Context, workID, worker string, err error) error
	managerDoneWorkerFunc      = func(_ context.Context, workID, worker string) error
	newManagerWorkerSendOpFunc = func(_ context.Context, workID, worker string) (
		opindexOpHashesFunc, dutil.InErrorFunc[error], error)
	newManagerManifestFunc = func(opsTree, statesTree *fixedtree.Tree) (base.Manifest, error)
	managerWaitFunc        = dutil.In3ErrorFunc[
		dutil.InContextErrorFunc[string],
		newManagerManifestFunc,
		managerDoneWorkerFunc,
	]
)

type managerOptions struct {
	JobWorkerMaxSemSize   uint64
	GenerateNodesHashFunc fixedtree.GenerateNodesHashFunc
	WorkersSizeMultiplier uint64
}

func newManagerOptions() *managerOptions {
	return &managerOptions{
		// WorkersSizeMultiplier estimates the acting workers size by operations
		// size. The acting workers size will be <workers size> *
		// <WorkerSizeMultiplier>. 0 will workers will be acting workers
		WorkersSizeMultiplier: 0,
		JobWorkerMaxSemSize:   1 << 9,
		GenerateNodesHashFunc: fixedtree.NewFuncAsyncGenerateNodesHashByFollowingParent(1 << 9),
	}
}

func newFuncManager(
	ctx context.Context,
	options *managerOptions,
	cancelWorkerf managerCancelWorkerFunc,
	workID string,
	workers []string,
	opsShare, statesShare uint64,
	ophs [][2]util.Hash,
) (
	_ dutil.In2ErrorFunc[managerStartWorkerFunc, newManagerWorkerSendOpFunc],
	workerOwners dutil.In2ErrorFunc[string, []string],
	_ managerInsertWorkerResultFunc,
	wait managerWaitFunc,
	_ error,
) {
	wctx, wctxCancel := dutil.ContextWithCancelCause(ctx)

	var actingWorkers []string

	switch a, o, s, err := isValidManagerArgs(options, workID, workers, opsShare, statesShare, ophs); {
	case err != nil:
		return nil, nil, nil, nil, err
	default:
		actingWorkers, opsShare, statesShare = a, o, s
	}

	log := dutil.NewModuleLoggerFromContext(wctx, "manager", func(zctx zerolog.Context) zerolog.Context {
		return zctx.Str("work_id", workID)
	})

	log.Debug().
		Interface("options", options).
		Strs("workers", workers).
		Strs("acting_workers", actingWorkers).
		Uint64("ops_share", opsShare).
		Uint64("states_share", statesShare).
		Msg("new")

	newJobWorker := dutil.NewFuncNewJobWorkerFunc(options.JobWorkerMaxSemSize)

	var sendOps dutil.In2ErrorFunc[dutil.InErrorFunc[[]string], newWorkerSendOpFunc]
	var workerResult managerInsertWorkerResultFunc
	var processWait managerProcessWaitFunc

	switch s, o, r, w, err := newFuncManagerProcess(
		wctx,
		newJobWorker,
		actingWorkers,
		opsShare, statesShare,
		ophs,
	); {
	case err != nil:
		return nil, nil, nil, nil, err
	default:
		sendOps, workerOwners, workerResult, processWait = s, o, r, w
	}

	cancelWork := func(err error) error {
		if err != nil {
			if cancelWorkerf != nil {
				err = managerCancelWorkers(wctx, newJobWorker, workID, workers, cancelWorkerf, err)
			}

			return wctxCancel(err)
		}

		return wctxCancel(err)
	}

	var opsWorkers []string

	return func(
			startWorker managerStartWorkerFunc,
			f newManagerWorkerSendOpFunc,
		) error {
			return dutil.NewIfErrErrorInFunc(cancelWork)(sendOps(
				func(w []string) error {
					opsWorkers = w

					return managerStartWorkers(
						wctx,
						newJobWorker,
						workID,
						actingWorkers, w,
						opsShare, statesShare,
						uint64(len(ophs)),
						startWorker,
					)
				},
				func(ctx context.Context, worker string) (opindexOpHashesFunc, dutil.InErrorFunc[error], error) {
					return f(ctx, workID, worker)
				},
			))
		},
		workerOwners,
		workerResult,
		func(
			workerInsertDone dutil.InContextErrorFunc[string],
			newManifest newManagerManifestFunc,
			doneWorkerf managerDoneWorkerFunc,
		) error {
			defer func() {
				_ = wctxCancel(nil)
			}()

			var statesOwners []string
			var opsTree, statesTree *fixedtree.Tree

			switch otr, str, err := processWait(
				options.GenerateNodesHashFunc,
				func(owners []string) error {
					statesOwners = owners

					// NOTE manager done to workers outside owners
					return newFuncManagerWorkerInsertDone(
						wctx,
						newJobWorker,
						actingWorkers,
						owners,
						workerInsertDone,
					)
				},
			); {
			case err != nil:
				return cancelWork(err)
			default:
				opsTree, statesTree = otr, str
			}

			switch manifest, err := newManifest(opsTree, statesTree); {
			case err != nil:
				return cancelWork(err)
			default:
				_ = manifest
			}

			// FIXME save,
			// - block map
			// - trees

			if doneWorkerf != nil {
				// NOTE if failed done workers, canceling workers is
				// meaningless. Some workers finish, but others not. If it
				// happened, manager cancels work.
				return managerDoneWorkers(
					wctx,
					newJobWorker,
					workID,
					workers, opsWorkers, statesOwners,
					opsShare, statesShare,
					doneWorkerf,
				)
			}

			return nil
		},
		nil
}

func isValidManagerArgs(
	options *managerOptions,
	workID string,
	workers []string,
	opsShare, statesShare uint64,
	ophs [][2]util.Hash,
) ([]string, uint64, uint64, error) {
	nworkers := uint64(len(workers))

	var err error

	switch {
	case workID == "":
		err = errors.Errorf("empty work id")
	case len(ophs) < 1:
		err = errors.Errorf("empty ophs")
	case nworkers < 1:
		err = errors.Errorf("empty workers")
	case opsShare < 1:
		err = errors.Errorf("zero ops share")
	case statesShare < 1:
		err = errors.Errorf("zero states share")
	}

	if err != nil {
		return nil, 0, 0, err
	}

	if opsShare >= nworkers {
		opsShare = nworkers
	}

	if statesShare >= nworkers {
		statesShare = nworkers
	}

	actingWorkers := workers

	if options.WorkersSizeMultiplier > 1 {
		n := uint64(len(ophs)) * options.WorkersSizeMultiplier
		if n < nworkers {
			actingWorkers = workers[:n]
		}
	}

	return actingWorkers, opsShare, statesShare, nil
}

func newFuncManagerWorkerInsertDone(
	ctx context.Context,
	newJobWorker dutil.NewJobWorkerFunc,
	workers, owners []string,
	workerDone dutil.InContextErrorFunc[string],
) error {
	var newJob dutil.JobWorkerNewJobFunc
	var jobDone func()
	var jobWait dutil.InErrorFunc[error]

	switch n, d, w, err := newJobWorker(ctx, uint64(len(workers))); {
	case err != nil:
		return err
	default:
		newJob, jobDone, jobWait = n, d, w
	}

	log := dutil.LoadContextValueLogger(ctx)

	for worker := range dutil.FilterSlice(slices.Values(workers), func(i string) bool {
		return slices.Index(owners, i) < 0
	}) {
		if err := newJob(func(ctx context.Context) error {
			err := workerDone(ctx, worker) // ignore worker error
			log.Debug().Err(err).Str("worker", worker).Msg("worker done; non-owners")

			return nil
		}); err != nil {
			return jobWait(err)
		}
	}

	jobDone()

	return jobWait(nil)
}

func managerStartWorkers(
	ctx context.Context,
	newJobWorker dutil.NewJobWorkerFunc,
	workID string,
	workers, opsWorkers []string,
	opsShare, statesShare uint64,
	maxOpsSize uint64,
	f managerStartWorkerFunc,
) error {
	var checkWorker checkFailedWorkersFunc

	switch w, err := newFuncCheckFailedWorkers(uint64(len(workers)), opsShare, nil); {
	case err != nil:
		return err
	default:
		checkWorker = w
	}

	var newJob dutil.JobWorkerNewJobFunc
	var jobDone func()
	var jobWait dutil.InErrorFunc[error]

	switch n, d, w, err := newJobWorker(ctx, uint64(len(workers))); {
	case err != nil:
		return err
	default:
		newJob, jobDone, jobWait = n, d, w
	}

	check := func(worker string, err error) error {
		var gerr error

		// NOTE check failed ops workers
		if err != nil && slices.Index(opsWorkers, worker) >= 0 {
			gerr = err
		}

		if _, _, failed := checkWorker(worker, gerr); failed != nil {
			return failed
		}

		return nil
	}

	for worker := range slices.Values(workers) {
		if err := newJob(func(ctx context.Context) error {
			return check(worker, f(
				ctx,
				workID,
				worker,
				workers, opsWorkers,
				opsShare, statesShare,
				maxOpsSize,
			))
		}); err != nil {
			return jobWait(err)
		}
	}

	jobDone()

	return jobWait(nil)
}

func managerCancelWorkers(
	ctx context.Context,
	newJobWorker dutil.NewJobWorkerFunc,
	workID string,
	workers []string,
	f managerCancelWorkerFunc,
	gerr error,
) error {
	log := dutil.LoadContextValueLogger(ctx)
	log.Debug().Msg("cancel workers")

	var newJob dutil.JobWorkerNewJobFunc
	var jobDone func()
	var jobWait dutil.InErrorFunc[error]

	switch n, d, w, err := newJobWorker(ctx, uint64(len(workers))); {
	case err != nil:
		return err
	default:
		newJob, jobDone, jobWait = n, d, w
	}

	for worker := range slices.Values(workers) {
		if err := newJob(func(ctx context.Context) error {
			err := f(ctx, workID, worker, gerr)
			log.Debug().Err(err).Str("worker", worker).Msg("cancel worker")

			return nil
		}); err != nil {
			return err
		}
	}

	jobDone()

	if err := jobWait(nil); err != nil {
		return err
	}

	return gerr
}

func managerDoneWorkers(
	ctx context.Context,
	newJobWorker dutil.NewJobWorkerFunc,
	workID string,
	workers, opsWorkers, statesOwners []string,
	opsShare, statesShare uint64,
	f managerDoneWorkerFunc,
) error {
	log := dutil.LoadContextValueLogger(ctx)

	var checkWorkerOps, checkWorkerStates checkFailedWorkersFunc

	switch w, err := newFuncCheckFailedWorkers(uint64(len(opsWorkers)), opsShare, nil); {
	case err != nil:
		return err
	default:
		checkWorkerOps = w
	}

	switch w, err := newFuncCheckFailedWorkers(uint64(len(statesOwners)), statesShare, nil); {
	case err != nil:
		return err
	default:
		checkWorkerStates = w
	}

	var newJob dutil.JobWorkerNewJobFunc
	var jobDone func()
	var jobWait dutil.InErrorFunc[error]

	switch n, d, w, err := newJobWorker(ctx, uint64(len(workers))); {
	case err != nil:
		return err
	default:
		newJob, jobDone, jobWait = n, d, w
	}

	for worker := range slices.Values(workers) {
		if err := newJob(func(ctx context.Context) error {
			err := f(ctx, workID, worker)

			l := log.With().Str("worker", worker).Err(err).Logger()

			if slices.Index(opsWorkers, worker) >= 0 {
				if _, _, failed := checkWorkerOps(worker, err); failed != nil {
					l.Error().Msg("ops worker done failed")

					return failed
				}
			}

			if slices.Index(statesOwners, worker) >= 0 {
				if _, _, failed := checkWorkerStates(worker, err); failed != nil {
					l.Error().Msg("state owner done failed")

					return failed
				}
			}

			l.Debug().Msg("worker done")

			return nil
		}); err != nil {
			return err
		}
	}

	jobDone()

	return jobWait(nil)
}
