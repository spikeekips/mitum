package dop5

import (
	"context"
	"iter"
	"slices"

	"github.com/pkg/errors"
	"github.com/spikeekips/mitum/base"
	"github.com/spikeekips/mitum/example/test364_5/fixedtree"
	dutil "github.com/spikeekips/mitum/example/test364_5/util"
	"github.com/spikeekips/mitum/util"
)

var managerName = "manager"

type (
	ManagerStartWorkerFunc = func(
		_ context.Context,
		worker string,
		workers, opsWorkers []string,
		opsShare, statesShare uint64,
		maxOpsSize uint64,
	) error
	ManagerCancelWorkerFunc    = func(_ context.Context, worker string, err error) error
	managerCloseWorkerFunc     = func(_ context.Context, worker string) error
	NewManagerWorkerSendOpFunc = func(_ context.Context, worker string, maxItems uint64) (
		OpindexOpHashesFunc, dutil.InErrorFunc[error], error)
	newManagerManifestFunc = func(opsTree, statesTree *fixedtree.Tree) (base.Manifest, error)
	ManagerWaitFunc        = dutil.In3ErrorFunc[
		dutil.InContextErrorFunc[string],
		newManagerManifestFunc,
		managerCloseWorkerFunc,
	]
)

type ManagerOptions struct {
	JobWorkerMaxSemSize   uint64
	GenerateNodesHashFunc fixedtree.GenerateNodesHashFunc
	WorkersSizeMultiplier uint64
}

func NewManagerOptions() *ManagerOptions {
	return &ManagerOptions{
		// WorkersSizeMultiplier estimates the acting workers size by operations
		// size. The acting workers size will be <workers size> *
		// <WorkerSizeMultiplier>. 0 will workers will be acting workers
		WorkersSizeMultiplier: 0,
		JobWorkerMaxSemSize:   1 << 9,
		GenerateNodesHashFunc: fixedtree.NewFuncAsyncGenerateNodesHashByFollowingParent(1 << 9),
	}
}

func NewFuncManager(
	ctx context.Context,
	options *ManagerOptions,
	cancelWorkerf ManagerCancelWorkerFunc,
	workers []string,
	opsShare, statesShare uint64,
	ophs [][2]util.Hash,
) (
	_ dutil.In2ErrorFunc[ManagerStartWorkerFunc, NewManagerWorkerSendOpFunc],
	workerOwners dutil.In2ErrorFunc[string, []string],
	_ ManagerInsertWorkerResultFunc,
	workerErr dutil.In2ErrorFunc[string, error],
	wait ManagerWaitFunc,
	_ error,
) {
	wctx, wctxCancel := dutil.ContextWithCancelCause(ctx)

	var actingWorkers []string

	switch a, o, s, err := isValidManagerArgs(options, workers, opsShare, statesShare, ophs); {
	case err != nil:
		return nil, nil, nil, nil, nil, err
	default:
		actingWorkers, opsShare, statesShare = a, o, s
	}

	log := dutil.NewModuleLoggerFromContext(wctx, "manager", nil)

	log.Debug().
		Interface("options", options).
		Strs("workers", workers).
		Strs("acting_workers", actingWorkers).
		Uint64("ops_share", opsShare).
		Uint64("states_share", statesShare).
		Msg("new")

	newJobWorker := dutil.NewFuncNewJobWorkerFunc(options.JobWorkerMaxSemSize)

	var workerPool *failedWorkerPool
	var sendOps dutil.InErrorFunc[newWorkerSendOpFunc]
	var workerResult ManagerInsertWorkerResultFunc
	var processWait managerProcessWaitFunc

	switch p, s, o, r, w, err := newFuncManagerProcess(
		wctx,
		newJobWorker,
		actingWorkers,
		opsShare, statesShare,
		ophs,
	); {
	case err != nil:
		return nil, nil, nil, nil, nil, err
	default:
		workerPool, sendOps, workerOwners, workerResult, processWait = p, s, o, r, w
	}

	cancelWork := func(err error) error {
		if err != nil {
			if cancelWorkerf != nil {
				_ = managerCancelWorkers(wctx, newJobWorker, workerPool, cancelWorkerf, err)
			}

			return wctxCancel(err)
		}

		return wctxCancel(err)
	}

	_, bgJobDone, _, bgJobWait := dutil.NewContextWait(wctx)

	return func(
			startWorker ManagerStartWorkerFunc,
			f NewManagerWorkerSendOpFunc,
		) error {
			if err := managerStartWorkers(
				wctx,
				newJobWorker,
				workerPool,
				opsShare, statesShare,
				uint64(len(ophs)),
				startWorker,
			); err != nil {
				return cancelWork(err)
			}

			return dutil.NewIfErrErrorInFunc(cancelWork)(sendOps(f))
		},
		workerOwners,
		workerResult,
		func(worker string, gerr error) error {
			switch _, failed, err := workerPool.workerError(worker, gerr); {
			case err != nil:
				return err
			case failed:
				return cancelWork(errWorkersFailed.Errorf("worker error; worker=%q err=%v", worker, gerr))
			default:
				return nil
			}
		},
		func(
			workerInsertDone dutil.InContextErrorFunc[string],
			newManifest newManagerManifestFunc,
			closeWorkerf managerCloseWorkerFunc,
		) error {
			defer func() {
				_ = wctxCancel(nil)
				_ = bgJobDone(nil)
				workerPool.Close()
			}()

			var statesOwners []string
			var opsTree, statesTree *fixedtree.Tree

			switch otr, str, err := processWait(
				options.GenerateNodesHashFunc,
				func(owners []string) error {
					statesOwners = owners

					switch failed, err := workerPool.statesOwnersReady(statesOwners); {
					case err != nil:
						return cancelWork(err)
					case failed:
						return cancelWork(errWorkersFailed.Errorf("process wait"))
					}

					// NOTE manager done to workers outside owners
					switch wait, err := newFuncManagerWorkerInsertDone(
						wctx,
						newJobWorker,
						workerPool,
						workerInsertDone,
					); {
					case err != nil:
						return err
					default:
						go func() {
							_ = bgJobDone(wait(nil))
						}()

						return nil
					}
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

			if closeWorkerf != nil {
				// NOTE if done workers fails, canceling workers is meaningless.
				// some workers ok, but others not. If it happened, manager
				// cancels work.
				if err := managerCloseWorkers(
					wctx,
					newJobWorker,
					workerPool,
					opsShare, statesShare,
					closeWorkerf,
				); err != nil {
					return err
				}
			}

			_ = bgJobWait() // NOTE wait error of background job worker ignored

			return nil
		},
		nil
}

func isValidManagerArgs(
	options *ManagerOptions,
	workers []string,
	opsShare, statesShare uint64,
	ophs [][2]util.Hash,
) ([]string, uint64, uint64, error) {
	nworkers := uint64(len(workers))

	var err error

	switch {
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
	workerPool *failedWorkerPool,
	workerDone dutil.InContextErrorFunc[string],
) (dutil.InErrorFunc[error], error) {
	var newJob dutil.JobWorkerNewJobFunc
	var jobDone func()
	var jobWait dutil.InErrorFunc[error]

	switch n, d, w, err := newJobWorker(ctx, uint64(len(workerPool.workers()))); {
	case err != nil:
		return nil, err
	default:
		newJob, jobDone, jobWait = n, d, w
	}

	log := dutil.LoadContextValueLogger(ctx)

	go func() {
		defer jobDone()

		var gerr error

		switch failed, err := workerPool.activeNonStatesOwners(func(seq iter.Seq[string]) {
			for worker := range seq {
				if err := newJob(func(ctx context.Context) error {
					err := workerDone(ctx, worker) // ignore worker error
					log.Debug().Err(err).Str("worker", worker).Msg("worker done; non-owners")

					return nil
				}); err != nil {
					gerr = err

					break
				}
			}
		}); {
		case err != nil:
			jobWait(err)
		case failed, gerr != nil:
			jobWait(errWorkersFailed.WithMessage(gerr, "insert done"))
		}
	}()

	return jobWait, nil
}

func managerStartWorkers(
	ctx context.Context,
	newJobWorker dutil.NewJobWorkerFunc,
	workerPool *failedWorkerPool,
	opsShare, statesShare uint64,
	maxOpsSize uint64,
	f ManagerStartWorkerFunc,
) error {
	workers := workerPool.workers()
	opsWorkers := workerPool.opsWorkers()

	var checkWorker checkFailedWorkersFunc

	switch w, err := newFuncCheckFailedWorkers(uint64(len(workerPool.opsWorkers())), opsShare, nil); {
	case err != nil:
		return err
	default:
		checkWorker = w
	}

	var newJob dutil.JobWorkerNewJobFunc
	var jobDone func()
	var jobWait dutil.InErrorFunc[error]

	switch n, d, w, err := newJobWorker(ctx, uint64(len(workerPool.workers()))); {
	case err != nil:
		return err
	default:
		newJob, jobDone, jobWait = n, d, w
	}

	switch failed, seq := workerPool.activeWorkers(); {
	case failed:
		return jobWait(errWorkersFailed.Errorf("start workers"))
	default:
		for worker := range seq {
			if err := newJob(func(ctx context.Context) error {
				switch err := f(
					ctx,
					worker,
					workers, opsWorkers,
					opsShare, statesShare,
					maxOpsSize,
				); {
				case slices.Index(opsWorkers, worker) >= 0:
					if _, _, failed := checkWorker(worker, err); failed != nil {
						return failed
					}
				}

				return nil
			}); err != nil {
				break
			}
		}
	}

	jobDone()

	return jobWait(nil)
}

func managerCancelWorkers(
	ctx context.Context,
	newJobWorker dutil.NewJobWorkerFunc,
	workerPool *failedWorkerPool,
	f ManagerCancelWorkerFunc,
	gerr error,
) error {
	log := dutil.LoadContextValueLogger(ctx)
	log.Debug().Msg("cancel workers")

	var newJob dutil.JobWorkerNewJobFunc
	var jobDone func()
	var jobWait dutil.InErrorFunc[error]

	switch n, d, w, err := newJobWorker(ctx, uint64(len(workerPool.workers()))); {
	case err != nil:
		return err
	default:
		newJob, jobDone, jobWait = n, d, w
	}

	_, seq := workerPool.activeWorkers()

	for worker := range seq {
		if err := newJob(func(ctx context.Context) error {
			err := f(ctx, worker, gerr)
			log.Debug().Err(err).Str("worker", worker).Msg("cancel worker")

			return nil
		}); err != nil {
			return err
		}
	}

	jobDone()

	return jobWait(nil)
}

func managerCloseWorkers(
	ctx context.Context,
	newJobWorker dutil.NewJobWorkerFunc,
	workerPool *failedWorkerPool,
	opsShare, statesShare uint64,
	f managerCloseWorkerFunc,
) error {
	log := dutil.LoadContextValueLogger(ctx)

	opsWorkers := workerPool.opsWorkers()

	var statesOwners []string

	switch i, err := workerPool.statesOwners(); {
	case err != nil:
		return err
	default:
		statesOwners = i
	}

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

	switch n, d, w, err := newJobWorker(ctx, uint64(len(workerPool.workers()))); {
	case err != nil:
		return err
	default:
		newJob, jobDone, jobWait = n, d, w
	}

	switch failed, seq := workerPool.activeWorkers(); {
	case failed:
		return jobWait(errWorkersFailed.Errorf("manager done workers"))
	default:
		for worker := range seq {
			if err := newJob(func(ctx context.Context) error {
				err := f(ctx, worker)

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
				break
			}
		}
	}

	jobDone()

	return jobWait(nil)
}
