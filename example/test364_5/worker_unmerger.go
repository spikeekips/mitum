package main

import (
	"context"
	"sync"

	"github.com/pkg/errors"
	"github.com/spikeekips/mitum/util"
)

type (
	setMinEnoughMaxFailedFunc func(minEnough, maxFailed uint64) error
	workerUnmergedFunc        func(worker string, unmerged []uint64) error
)

// newFuncWorkerUnmerger finishes to merge operations after phase merger.
func newFuncWorkerUnmerger(
	ctx context.Context,
	ctxCancel inErrorFunc[error],
	local string,
	newJobWorker newJobWorkerFunc,
	xMergedOpsMapSize uint64,
	getStateMergeValues opindexGetStateMergeValuesFunc,
	getStateMergers mergerGetStateMergersFunc,
	newCheckWorkers newCheckEnoughFailedWorkersFunc, // NOTE newLocalCheckEnoughFailedWorkersFunc
) (
	workerUnmergedFunc, // NOTE from state owners, excluding local
	setMinEnoughMaxFailedFunc,
	inErrorFunc[unmergedFunc],
	error,
) {
	log := newModuleLoggerFromContext(ctx, "worker-unmerger", nil)

	log.Debug().Msg("newFuncWorkerUnmerger")

	unmergerUnmerge, unmergerDone, unmergerWait := newFuncUnmergerLocked(
		ctx,
		newJobWorker,
		xMergedOpsMapSize,
		getStateMergeValues,
		getStateMergers,
	)

	_, workerPhaseDone, workerPhaseDoneWait := newDoneContext(ctx)

	workerUnmerge, localReady, workerUnmergeClose := newFuncWorkerPhaseDonePool(
		local,
		newCheckWorkers,
		unmergerUnmerge,
	)

	return func(worker string, unmerged []uint64) error {
			if worker == local {
				return util.ErrFound.Errorf("unexpected local")
			}

			return contextCauseError(ctx, dummyErrorInFunc, func(context.Context) error {
				l := log.With().Str("worker", worker).Uints64("unmerged", unmerged).Logger()

				switch ok, enough, failed, err := workerUnmerge(worker, unmerged); {
				case failed:
					l.Error().Err(err).Msg("worker phase done; failed")

					return ctxCancel(workerPhaseDone(err))
				case err != nil: // NOTE ignore
					l.Debug().Err(err).Msg("worker phase done")

					return err
				case !ok:
					l.Debug().Msg("worker phase done; not ok")
				case enough:
					l.Debug().Msg("worker phase done; enough")

					return workerPhaseDone(nil)
				}

				return nil
			})
		},
		func(minEnough, maxFailed uint64) error {
			return contextCauseError(ctx, dummyErrorInFunc, func(context.Context) error {
				l := log.With().
					Uint64("min_enough", minEnough).
					Uint64("max_failed", maxFailed).
					Logger()

				switch ok, enough, failed, err := localReady(minEnough, maxFailed); {
				case failed:
					l.Error().Err(err).Msg("local phase done; failed")

					return ctxCancel(workerPhaseDone(err))
				case err != nil: // NOTE ignore
					l.Debug().Err(err).Msg("local phase done")

					return err
				case !ok:
					l.Debug().Msg("local phase done; not ok")
				case enough:
					l.Debug().Msg("local phase done; enough")

					return workerPhaseDone(nil)
				}

				return nil
			})
		},
		func(f unmergedFunc) error {
			defer func() {
				_ = unmergerWait(nil)
				workerUnmergeClose()
			}()

			return contextCauseError(ctx, ctxCancel, func(context.Context) error {
				if err := workerPhaseDoneWait(); err != nil {
					return err
				}

				unmergerDone()

				return unmergerWait(func(unmerged []uint64) error {
					log.Debug().Uints64("unmerged", unmerged).Msg("done")

					if f == nil {
						return nil
					}

					return f(unmerged)
				})
			})
		},
		nil
}

func newFuncWorkerPhaseDonePool(
	local string,
	newCheckWorkers newCheckEnoughFailedWorkersFunc,
	unmergeOp opindexErrorFunc,
) (
	func(worker string, unmerged []uint64) (ok, enough, failed bool, _ error),
	func(minEnough, maxFailed uint64) (ok, enough, failed bool, _ error),
	func(),
) {
	pool := map[ /* worker */ string] /* unmerged */ []uint64{}

	closePool := func() {
		clear(pool)
		pool = nil
	}

	var checkWorker checkEnoughFailedWorkerFunc
	var checkWorkerClose func()

	var l sync.RWMutex

	addPool := func(worker string, unmerged []uint64) (isReady bool) {
		l.Lock()
		defer l.Unlock()

		if _, found := pool[worker]; found {
			return false
		}

		if checkWorker != nil {
			return true
		}

		pool[worker] = unmerged

		return false
	}

	unmergeOps := func(worker string, unmerged []uint64) (_, _, _ bool, _ error) {
		gerr := traverseSlice(unmerged, func(_ int, opindex uint64) (bool, error) {
			return true, unmergeOp(opindex)
		})

		switch ok, enough, failed, err := checkWorker(worker, func() error { return gerr }); {
		case failed, err != nil:
			return ok, enough, failed, err
		default:
			return ok, enough, failed, gerr
		}
	}

	return func(worker string, unmerged []uint64) (_, _, _ bool, _ error) {
			if worker == local {
				return false, false, false, util.ErrFound.Errorf("unexpected local")
			}

			if !addPool(worker, unmerged) {
				return false, false, false, nil
			}

			return unmergeOps(worker, unmerged)
		},
		func(minEnough, maxFailed uint64) (_, _, _ bool, _ error) {
			l.Lock()
			defer l.Unlock()
			defer closePool()

			if minEnough < 1 {
				if len(pool) > 0 { // non-empty pool; failed
					return true, false, true, errors.Errorf("unmerged found in pool, but min enough is zero")
				}

				return true, true, false, nil
			}

			switch w, c, err := newCheckWorkers(minEnough, maxFailed); {
			case err != nil:
				return false, false, false, err
			default:
				checkWorker = w
				checkWorkerClose = c
			}

			if ok, enough, failed, err := unmergeOps(local, nil); failed || err != nil || enough {
				return ok, enough, failed, err
			}

			var ok, enough, failed bool
			var gerr error

			_ = traverseMap(pool, func(worker string, unmerged []uint64) (bool, error) {
				if o, e, f, err := unmergeOps(worker, unmerged); f || err != nil || e {
					ok, enough, failed, gerr = o, e, f, err

					return false, err
				}

				return true, nil
			})

			return ok, enough, failed, gerr
		},
		func() {
			l.Lock()
			defer l.Unlock()

			closePool()

			if checkWorkerClose != nil {
				checkWorkerClose()
			}
		}
}

func newFuncUnmergerLocked(
	ctx context.Context,
	newJobWorker newJobWorkerFunc,
	xMergedOpsMapSize uint64,
	getStateMergeValues opindexGetStateMergeValuesFunc,
	getMergers mergerGetStateMergersFunc,
) (
	opindexErrorFunc,
	func(),
	inErrorFunc[unmergedFunc],
) {
	doneLocked := util.EmptyLocked[func()]()
	waitLocked := util.EmptyLocked[inErrorFunc[unmergedFunc]]()

	var unmerge opindexErrorFunc

	once := sync.OnceValue(func() error {
		switch u, d, w, err := newFuncUnmerger(
			ctx,
			newJobWorker,
			xMergedOpsMapSize,
			getStateMergeValues,
			getMergers,
		); {
		case err != nil:
			return err
		default:
			unmerge = u
			_ = doneLocked.SetValue(d)
			_ = waitLocked.SetValue(w)

			return nil
		}
	})

	return func(opindex uint64) error {
			if err := once(); err != nil {
				return err
			}

			return unmerge(opindex)
		},
		func() {
			if i, isEmpty := doneLocked.Value(); !isEmpty {
				i()
			}
		},

		func(f unmergedFunc) error {
			defer func() {
				doneLocked.EmptyValue()
				waitLocked.EmptyValue()
			}()

			if i, isEmpty := waitLocked.Value(); !isEmpty {
				return i(f)
			}

			return nil
		}
}
