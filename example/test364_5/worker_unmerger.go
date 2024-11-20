package main

import (
	"context"
	"sync"

	"github.com/pkg/errors"
	"github.com/spikeekips/mitum/util"
)

type (
	workerUnmergerNewFuncUnmergerFunc func(_ context.Context, localUnmerged []uint64) (
		opindexErrorFunc,
		func(),
		inErrorFunc[unmergedFunc],
		error,
	)
	workerUnmergerLocalReadyFunc func(localUnmerged []uint64, minEnough, maxFailed uint64) error
	workerUnmergedFunc           func(worker string, unmerged []uint64) error
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
	workerUnmergerLocalReadyFunc,
	inErrorFunc[unmergedFunc],
	error,
) {
	log := newModuleLoggerFromContext(ctx, "worker-unmerger", nil)

	log.Debug().
		Str("local", local).
		Msg("newFuncWorkerUnmerger")

	localReady, workerUnmerge, unmergerDone, unmergerWait := newFuncWorkerUnmergerWorkerUnmergedPool(
		ctx,
		local,
		newCheckWorkers,
		func(ctx context.Context, localUnmerged []uint64) (opindexErrorFunc, func(), inErrorFunc[unmergedFunc], error) {
			return newFuncUnmerger(
				ctx,
				newJobWorker,
				xMergedOpsMapSize,
				getStateMergeValues,
				getStateMergers,
				localUnmerged,
			)
		},
	)

	_, workerPhaseDone, workerPhaseDoneWait := newDoneContext(ctx)

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
		func(localUnmerged []uint64, minEnough, maxFailed uint64) error {
			return contextCauseError(ctx, dummyErrorInFunc, func(context.Context) error {
				l := log.With().
					Uint64("min_enough", minEnough).
					Uint64("max_failed", maxFailed).
					Logger()

				switch ok, enough, failed, err := localReady(localUnmerged, minEnough, maxFailed); {
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
			}()

			return contextCauseError(ctx, ctxCancel, func(context.Context) error {
				if err := workerPhaseDoneWait(); err != nil {
					return err
				}

				unmergerDone()

				return unmergerWait(func(unmerged []uint64) error {
					log.Debug().Uints64("unmerged", unmerged).Msg("done")

					if f != nil {
						return f(unmerged)
					}

					return nil
				})
			})
		},
		nil
}

func newFuncWorkerUnmergerWorkerUnmergedPool(
	ctx context.Context,
	local string,
	newCheckWorkers newCheckEnoughFailedWorkersFunc,
	newFuncUnmergerFunc workerUnmergerNewFuncUnmergerFunc,
) (
	localReady func(localUnmerged []uint64, minEnough, maxFailed uint64) (ok, enough, failed bool, _ error),
	unmergerUnmerge func(worker string, unmerged []uint64) (ok, enough, failed bool, _ error),
	unmergerDone func(),
	unmergerWait inErrorFunc[unmergedFunc],
) {
	addPool, poolLen, traversePool := newFuncWorkerUnmergerUnmergedPool()

	var checkWorker checkEnoughFailedWorkerFunc
	var checkWorkerClose func()

	newUnmergerf, unmergef, donef, waitf := newFuncWorkerUnmergerNewUnmergerFunc(ctx, newFuncUnmergerFunc)

	var once sync.Once
	var newUnmergerErr error

	newUnmerger := func(localUnmerged []uint64, minEnough, maxFailed uint64) error {
		once.Do(func() {
			switch w, c, err := newCheckWorkers(minEnough, maxFailed); {
			case err != nil:
				newUnmergerErr = err
			default:
				checkWorker = w
				checkWorkerClose = c
			}

			newUnmergerErr = newUnmergerf(localUnmerged)
		})

		return newUnmergerErr
	}

	unmergeOps := func(worker string, unmerged []uint64, unmergeOp opindexErrorFunc) (_, _, _ bool, _ error) {
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

	return func(localUnmerged []uint64, minEnough, maxFailed uint64) (_, _, _ bool, _ error) {
			if minEnough < 1 {
				if poolLen() > 0 { // non-empty pool; failed
					return true, false, true, errors.Errorf("unmerged found in pool, but min enough is zero")
				}

				return true, true, false, nil
			}

			if err := newUnmerger(localUnmerged, minEnough, maxFailed); err != nil {
				return false, false, false, err
			}

			var unmergeOp opindexErrorFunc

			if err := unmergef(func(f opindexErrorFunc, isEmpty bool) error {
				if isEmpty {
					return errors.Errorf("empty unmerge of unmerger")
				}

				unmergeOp = f

				return nil
			}); err != nil {
				return false, false, false, err
			}

			if ok, enough, failed, err := unmergeOps(local, nil, unmergeOp); failed || err != nil || enough {
				return ok, enough, failed, err
			}

			var ok, enough, failed bool
			var gerr error

			_ = traversePool(func(worker string, unmerged []uint64) (bool, error) {
				if o, e, f, err := unmergeOps(worker, unmerged, unmergeOp); f || err != nil || e {
					ok, enough, failed, gerr = o, e, f, err

					return false, err
				}

				return true, nil
			})

			return ok, enough, failed, gerr
		},
		func(worker string, unmerged []uint64) (ok, enough, failed bool, _ error) {
			if worker == local {
				return false, false, false, util.ErrFound.Errorf("unexpected local")
			}

			return ok, enough, failed, unmergef(func(f opindexErrorFunc, isEmpty bool) error {
				if isEmpty {
					_ = addPool(worker, unmerged)

					return nil
				}

				var err error

				ok, enough, failed, err = unmergeOps(worker, unmerged, f)

				return err
			})
		},
		donef,
		func(f unmergedFunc) error {
			defer func() {
				if checkWorkerClose != nil {
					checkWorkerClose()
				}

				traversePool(nil)
			}()

			return waitf(f)
		}
}

func newFuncWorkerUnmergerUnmergedPool() (
	addPool func(worker string, unmerged []uint64) (added bool),
	poolLen func() int,
	traverse func(func(worker string, unmerged []uint64) (bool, error)) error,
) {
	pool := map[ /* worker */ string] /* unmerged */ []uint64{}

	var l sync.RWMutex

	return func(worker string, unmerged []uint64) bool {
			l.Lock()
			defer l.Unlock()

			if _, found := pool[worker]; found {
				return false
			}

			pool[worker] = unmerged

			return true
		},
		func() int {
			l.RLock()
			defer l.RUnlock()

			return len(pool)
		},
		func(f func(worker string, unmerged []uint64) (bool, error)) error {
			l.Lock()
			defer l.Unlock()

			defer clearMap(pool)

			if f != nil {
				return traverseMap(pool, f)
			}

			return nil
		}
}

func newFuncWorkerUnmergerNewUnmergerFunc(
	ctx context.Context,
	newFuncUnmergerFunc workerUnmergerNewFuncUnmergerFunc,
) (
	func(localUnmerged []uint64) error,
	func(func(_ opindexErrorFunc, isEmpty bool) error) error,
	func(),
	inErrorFunc[unmergedFunc],
) {
	unmergeLocked := util.EmptyLocked[opindexErrorFunc]()
	doneLocked := util.EmptyLocked[func()]()
	waitLocked := util.EmptyLocked[inErrorFunc[unmergedFunc]]()

	var waitOnce sync.Once
	var waitErr error

	return func(localUnmerged []uint64) error {
			switch u, d, w, err := newFuncUnmergerFunc(ctx, localUnmerged); {
			case err != nil:
				return err
			default:
				_ = unmergeLocked.SetValue(u)
				_ = doneLocked.SetValue(d)
				_ = waitLocked.SetValue(w)

				return nil
			}
		},
		unmergeLocked.Get,
		func() {
			if i, isEmpty := doneLocked.Value(); !isEmpty {
				i()
			}
		},
		func(f unmergedFunc) error {
			waitOnce.Do(func() {
				defer func() {
					unmergeLocked.EmptyValue()
					doneLocked.EmptyValue()
					waitLocked.EmptyValue()
				}()

				if f != nil {
					if i, isEmpty := waitLocked.Value(); !isEmpty {
						waitErr = i(f)

						return
					}

					waitErr = f(nil)
				}
			})

			return waitErr
		}
}
