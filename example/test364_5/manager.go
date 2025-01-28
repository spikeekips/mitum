package main

import (
	"context"
	"fmt"
	"iter"
	"slices"
	"sync"

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
	managerCloseWorkerFunc     = func(_ context.Context, workID, worker string) error
	newManagerWorkerSendOpFunc = func(_ context.Context, workID, worker string) (
		opindexOpHashesFunc, dutil.InErrorFunc[error], error)
	newManagerManifestFunc = func(opsTree, statesTree *fixedtree.Tree) (base.Manifest, error)
	managerWaitFunc        = dutil.In3ErrorFunc[
		dutil.InContextErrorFunc[string],
		newManagerManifestFunc,
		managerCloseWorkerFunc,
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
	workerErr dutil.In2ErrorFunc[string, error],
	wait managerWaitFunc,
	_ error,
) {
	wctx, wctxCancel := dutil.ContextWithCancelCause(ctx)

	var actingWorkers []string

	switch a, o, s, err := isValidManagerArgs(options, workID, workers, opsShare, statesShare, ophs); {
	case err != nil:
		return nil, nil, nil, nil, nil, err
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

	var workerPool *failedWorkerPool
	var sendOps dutil.InErrorFunc[newWorkerSendOpFunc]
	var workerResult managerInsertWorkerResultFunc
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
				err = managerCancelWorkers(wctx, newJobWorker, workID, workerPool, cancelWorkerf, err)
			}

			return wctxCancel(err)
		}

		return wctxCancel(err)
	}

	return func(
			startWorker managerStartWorkerFunc,
			f newManagerWorkerSendOpFunc,
		) error {
			if err := managerStartWorkers(
				wctx,
				newJobWorker,
				workID,
				workerPool,
				opsShare, statesShare,
				uint64(len(ophs)),
				startWorker,
			); err != nil {
				return cancelWork(err)
			}

			return dutil.NewIfErrErrorInFunc(cancelWork)(sendOps(
				func(ctx context.Context, worker string) (opindexOpHashesFunc, dutil.InErrorFunc[error], error) {
					return f(ctx, workID, worker)
				},
			))
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
						return cancelWork(errWorkersFailed.Errorf("process weait"))
					}

					// NOTE manager done to workers outside owners
					return newFuncManagerWorkerInsertDone(
						wctx,
						newJobWorker,
						workerPool,
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

			if closeWorkerf != nil {
				// NOTE if done workers fails, canceling workers is meaningless.
				// some workers ok, but others not. If it happened, manager
				// cancels work.
				return managerCloseWorkers(
					wctx,
					newJobWorker,
					workID,
					workerPool,
					opsShare, statesShare,
					closeWorkerf,
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
	workerPool *failedWorkerPool,
	workerDone dutil.InContextErrorFunc[string],
) error {
	var newJob dutil.JobWorkerNewJobFunc
	var jobDone func()
	var jobWait dutil.InErrorFunc[error]

	switch n, d, w, err := newJobWorker(ctx, uint64(len(workerPool.workers()))); {
	case err != nil:
		return err
	default:
		newJob, jobDone, jobWait = n, d, w
	}

	log := dutil.LoadContextValueLogger(ctx)

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
		return err
	case failed:
		return errWorkersFailed.Errorf("insert done")
	}

	if gerr != nil {
		return jobWait(gerr)
	}

	jobDone()

	return jobWait(nil)
}

func managerStartWorkers(
	ctx context.Context,
	newJobWorker dutil.NewJobWorkerFunc,
	workID string,
	workerPool *failedWorkerPool,
	opsShare, statesShare uint64,
	maxOpsSize uint64,
	f managerStartWorkerFunc,
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
					workID,
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
	workID string,
	workerPool *failedWorkerPool,
	f managerCancelWorkerFunc,
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
	var werr error

	for worker := range seq {
		if werr = newJob(func(ctx context.Context) error {
			err := f(ctx, workID, worker, gerr)
			log.Debug().Err(err).Str("worker", worker).Msg("cancel worker")

			return nil
		}); werr != nil {
			break
		}
	}

	if werr != nil {
		return jobWait(gerr)
	}

	jobDone()

	if err := jobWait(nil); err != nil {
		return err
	}

	return gerr
}

func managerCloseWorkers(
	ctx context.Context,
	newJobWorker dutil.NewJobWorkerFunc,
	workID string,
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

	var gerr error

	switch failed, seq := workerPool.activeWorkers(); {
	case failed:
		return jobWait(errWorkersFailed.Errorf("manager done workers"))
	default:
		for worker := range seq {
			if gerr = newJob(func(ctx context.Context) error {
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
			}); gerr != nil {
				break
			}
		}

		if gerr != nil {
			return jobWait(gerr)
		}
	}

	jobDone()

	return jobWait(nil)
}

type failedWorkerPool struct {
	pool             map[string]error
	opsShare         uint64
	statesShare      uint64
	ws               []string
	opsws            []string
	sos              []string
	failedOpsWorkers uint64
	failedSos        uint64
	isSosReady       bool
	failed           bool
	l                sync.RWMutex
}

func newFailedWorkerPool(workers, opsWorkers []string, opsShare, statesShare uint64) *failedWorkerPool {
	pool := map[string]error{}

	for worker := range slices.Values(workers) {
		pool[worker] = nil
	}

	return &failedWorkerPool{
		pool:        pool,
		ws:          workers,
		opsws:       opsWorkers,
		opsShare:    opsShare,
		statesShare: statesShare,
	}
}

func (p *failedWorkerPool) Close() {
	p.l.Lock()
	defer p.l.Unlock()

	p.failed = true

	dutil.ClearMap(p.pool)
}

func (p *failedWorkerPool) workerError(worker string, err error) (added, failed bool, _ error) {
	if err == nil {
		return false, false, errors.Errorf("nil error")
	}

	fmt.Printf("worker error: worker=%q err=%+v\n", worker, err)

	p.l.Lock()
	defer p.l.Unlock()

	if p.failed {
		return false, true, nil
	}

	switch prev, found := p.pool[worker]; {
	case !found:
		return false, false, util.ErrNotFound.Errorf("unknown worker, %q", worker)
	case prev != nil:
		return false, false, nil
	}

	p.pool[worker] = err

	if slices.Index(p.opsws, worker) >= 0 {
		p.failedOpsWorkers++
		if p.failedOpsWorkers >= p.opsShare {
			p.failed = true
			dutil.ClearMap(p.pool)

			return true, true, nil
		}
	}

	if p.isSosReady {
		if p.subOwnerError(worker, err) {
			return true, true, nil
		}
	}

	return true, false, nil
}

func (p *failedWorkerPool) subOwnerError(worker string, err error) (failed bool) {
	if slices.Index(p.sos, worker) < 0 {
		return false
	}

	p.failedSos++

	if p.failedSos >= p.statesShare {
		p.failed = true
		dutil.ClearMap(p.pool)

		return true
	}

	return false
}

func (p *failedWorkerPool) statesOwnersReady(sos []string) (failed bool, _ error) {
	p.l.Lock()
	defer p.l.Unlock()

	if p.failed {
		return true, nil
	}

	for worker := range slices.Values(sos) {
		if _, found := p.pool[worker]; !found {
			return false, util.ErrNotFound.Errorf("unknown sub owners, %q", worker)
		}
	}

	p.sos = sos
	p.isSosReady = true

	for worker := range slices.Values(sos) {
		if err := p.pool[worker]; err != nil && p.subOwnerError(worker, err) {
			return true, nil
		}
	}

	return false, nil
}

func (p *failedWorkerPool) workers() []string {
	return p.ws
}

func (p *failedWorkerPool) opsWorkers() []string {
	return p.opsws
}

func (p *failedWorkerPool) statesOwners() ([]string, error) {
	p.l.RLock()
	defer p.l.RUnlock()

	if !p.isSosReady {
		return nil, errors.Errorf("not ready")
	}

	return p.sos, nil
}

func (p *failedWorkerPool) activeWorkers() (failed bool, _ iter.Seq[string]) {
	p.l.RLock()
	defer p.l.RUnlock()

	return p.failed,
		func(yield func(string) bool) {
			p.l.RLock()
			defer p.l.RUnlock()

			slices.Values(p.ws)(func(worker string) bool {
				if p.pool[worker] != nil {
					return true
				}

				return yield(worker)
			})
		}
}

func (p *failedWorkerPool) activeOpsWorkers(f func(iter.Seq[string])) (failed bool) {
	p.l.RLock()
	defer p.l.RUnlock()

	if p.failed {
		return true
	}

	f(func(yield func(string) bool) {
		slices.Values(p.opsws)(func(worker string) bool {
			if p.pool[worker] != nil {
				return true
			}

			return yield(worker)
		})
	})

	return false
}

func (p *failedWorkerPool) activeStatesOwners(f func(iter.Seq[string])) (failed bool, _ error) {
	p.l.RLock()
	defer p.l.RUnlock()

	if p.failed {
		return true, nil
	}

	if !p.isSosReady {
		return false, errors.Errorf("not ready")
	}

	f(func(yield func(string) bool) {
		slices.Values(p.sos)(func(worker string) bool {
			if p.pool[worker] != nil {
				return true
			}

			return yield(worker)
		})
	})

	return false, nil
}

func (p *failedWorkerPool) activeNonStatesOwners(f func(iter.Seq[string])) (failed bool, _ error) {
	p.l.RLock()
	defer p.l.RUnlock()

	if p.failed {
		return true, nil
	}

	if !p.isSosReady {
		return false, errors.Errorf("not ready")
	}

	f(func(yield func(string) bool) {
		slices.Values(p.ws)(func(worker string) bool {
			switch {
			case p.pool[worker] != nil,
				slices.Index(p.sos, worker) >= 0:
				return true
			default:
				return yield(worker)
			}
		})
	})

	return false, nil
}
