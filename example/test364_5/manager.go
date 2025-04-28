package dop5

import (
	"context"
	stderrors "errors"
	"iter"
	"slices"

	"github.com/pkg/errors"
	"github.com/spikeekips/mitum/base"
	"github.com/spikeekips/mitum/example/test364_5/fixedtree"
	dutil "github.com/spikeekips/mitum/example/test364_5/util"
	"github.com/spikeekips/mitum/util"
)

var ManagerName = "manager"

type (
	ManagerStartWorkerFunc = func(
		_ context.Context,
		worker string,
		workers, opsWorkers []string,
		opsShare, statesShare uint64,
		maxOpsSize uint64,
	) error
	ManagerCancelWorkFunc      = func(_ context.Context, worker string, err error) error
	managerCloseWorkFunc       = func(_ context.Context, worker string) error
	NewManagerWorkerSendOpFunc = func(_ context.Context, worker string, maxItems uint64) (
		OpindexOpHashesFunc, dutil.InErrorFunc[error], error)
	newManagerManifestFunc = func(opsTree, statesTree *fixedtree.Tree) (base.Manifest, error)
	ManagerWaitFunc        = dutil.In3ErrorFunc[
		dutil.InContextErrorFunc[string],
		newManagerManifestFunc,
		managerCloseWorkFunc,
	]
)

type ManagerOptions struct {
	GenerateNodesHashFunc fixedtree.GenerateNodesHashFunc
	JobWorkerMaxSemSize   uint64
	WorkersSizeMultiplier uint64
}

func NewManagerOptions() *ManagerOptions {
	return &ManagerOptions{
		// WorkersSizeMultiplier estimates the acting workers size by operations
		// size. The acting workers size will be <workers size> *
		// <WorkerSizeMultiplier>. 0 will workers will be acting workers
		WorkersSizeMultiplier: 0,
		JobWorkerMaxSemSize:   1 << 9,                                                           //nolint:mnd
		GenerateNodesHashFunc: fixedtree.NewFuncAsyncGenerateNodesHashByFollowingParent(1 << 9), //nolint:mnd
	}
}

func NewFuncManager(
	ctx context.Context,
	options *ManagerOptions,
	cancelWorkf ManagerCancelWorkFunc,
	workers []string,
	opsShare, statesShare uint64,
	ophs [][2]util.Hash,
) (
	dutil.In2ErrorFunc[ManagerStartWorkerFunc, NewManagerWorkerSendOpFunc], /* start workers */
	dutil.In2ErrorFunc[string, []string], /* workerOwners */
	ManagerInsertWorkerResultFunc,
	dutil.In2ErrorFunc[string, error], /* workerErr */
	ManagerWaitFunc,
	error,
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

	newJobWorker := dutil.NewFuncNewJobWorkerFunc(dutil.SafeUint64ToInt64(options.JobWorkerMaxSemSize))

	var workerPool *failedWorkerPool
	var sendOps dutil.InErrorFunc[newWorkerSendOpFunc]
	var workerOwners dutil.In2ErrorFunc[string, []string]
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
			if cancelWorkf != nil {
				_ = managerCancelWorks(wctx, newJobWorker, workerPool, cancelWorkf, err)
			}

			return wctxCancel(err)
		}

		return wctxCancel(err)
	}

	bgJobDone, bgJobWait := dutil.NewContextDone(wctx)

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
				return cancelWork(errors.Wrapf(errWorkersFailed, "worker error; worker=%q err=%v", worker, gerr))
			default:
				return nil
			}
		},
		func(
			workerInsertDone dutil.InContextErrorFunc[string],
			newManifest newManagerManifestFunc,
			closeWorkf managerCloseWorkFunc,
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
						return cancelWork(errors.Wrap(errWorkersFailed, "process wait"))
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

			if closeWorkf != nil {
				// NOTE if done workers fails, canceling workers is meaningless.
				// some workers ok, but others not. If it happened, manager
				// cancels work.
				if err := managerCloseWork(
					wctx,
					newJobWorker,
					workerPool,
					opsShare, statesShare,
					closeWorkf,
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
		err = errors.New("empty ophs")
	case nworkers < 1:
		err = errors.New("empty workers")
	case opsShare < 1:
		err = errors.New("zero ops share")
	case statesShare < 1:
		err = errors.New("zero states share")
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

	switch n, d, w, err := newJobWorker(ctx, int64(len(workerPool.workers()))); {
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
			_ = jobWait(err)
		case failed, gerr != nil:
			_ = jobWait(stderrors.Join(errWorkersFailed, errors.Wrap(gerr, "insert done")))
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

	var checkWorker CheckFailedWorkersFunc

	switch w, err := NewFuncCheckFailedWorkers(uint64(len(workerPool.opsWorkers())), opsShare, nil); {
	case err != nil:
		return err
	default:
		checkWorker = w
	}

	var newJob dutil.JobWorkerNewJobFunc
	var jobDone func()
	var jobWait dutil.InErrorFunc[error]

	switch n, d, w, err := newJobWorker(ctx, int64(len(workerPool.workers()))); {
	case err != nil:
		return err
	default:
		newJob, jobDone, jobWait = n, d, w
	}

	switch failed, seq := workerPool.activeWorkers(); {
	case failed:
		return jobWait(errors.Wrap(errWorkersFailed, "start workers"))
	default:
		for worker := range seq {
			if err := newJob(func(ctx context.Context) error {
				err := f(
					ctx,
					worker,
					workers, opsWorkers,
					opsShare, statesShare,
					maxOpsSize,
				)

				if slices.Index(opsWorkers, worker) >= 0 {
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

func managerCancelWorks(
	ctx context.Context,
	newJobWorker dutil.NewJobWorkerFunc,
	workerPool *failedWorkerPool,
	f ManagerCancelWorkFunc,
	gerr error,
) error {
	log := dutil.LoadContextValueLogger(ctx)
	log.Debug().Msg("cancel workers")

	var newJob dutil.JobWorkerNewJobFunc
	var jobDone func()
	var jobWait dutil.InErrorFunc[error]

	switch n, d, w, err := newJobWorker(ctx, int64(len(workerPool.workers()))); {
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

func managerCloseWork(
	ctx context.Context,
	newJobWorker dutil.NewJobWorkerFunc,
	workerPool *failedWorkerPool,
	opsShare, statesShare uint64,
	f managerCloseWorkFunc,
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

	var checkWorkerOps, checkWorkerStates CheckFailedWorkersFunc

	switch w, err := NewFuncCheckFailedWorkers(uint64(len(opsWorkers)), opsShare, nil); {
	case err != nil:
		return err
	default:
		checkWorkerOps = w
	}

	switch w, err := NewFuncCheckFailedWorkers(uint64(len(statesOwners)), statesShare, nil); {
	case err != nil:
		return err
	default:
		checkWorkerStates = w
	}

	var newJob dutil.JobWorkerNewJobFunc
	var jobDone func()
	var jobWait dutil.InErrorFunc[error]

	switch n, d, w, err := newJobWorker(ctx, int64(len(workerPool.workers()))); {
	case err != nil:
		return err
	default:
		newJob, jobDone, jobWait = n, d, w
	}

	switch failed, seq := workerPool.activeWorkers(); {
	case failed:
		return jobWait(errors.Wrap(errWorkersFailed, "manager done workers"))
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
