package main

import (
	"context"
	"slices"
	"sync"

	"github.com/pkg/errors"
	"github.com/spikeekips/mitum/util"
)

type (
	workerUnmergerNewFuncUnmergerFunc = func(_ context.Context) (
		inErrorFunc[uint64],
		func(),
		inErrorFunc[unmergedFunc],
		error,
	)
	workerUnmergerLocalReadyFunc = func(subStateOwners []string, localUnmerged []uint64, minEnough, maxFailed uint64) error
	workerUnmergedFunc           = func(worker string, unmerged []uint64) error
	workerOpindexFilterFunc      = func(worker string) opindexFilterFunc
)

// newFuncWorkerUnmerger finishes to merge operations after phase merger.
func newFuncWorkerUnmerger(
	ctx context.Context,
	ctxCancel inErrorFunc[error],
	local string,
	newJobWorker newJobWorkerFunc,
	maxOpsSize uint64,
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
		maxOpsSize,
		getStateMergeValues,
		newCheckWorkers,
		func(ctx context.Context) (inErrorFunc[uint64], func(), inErrorFunc[unmergedFunc], error) {
			return newFuncUnmerger(
				ctx,
				newJobWorker,
				maxOpsSize,
				getStateMergeValues,
				getStateMergers,
			)
		},
	)

	_, workerPhaseDone, workerPhaseDoneWait := newContextDone(ctx)

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
		func(subws []string, localUnmerged []uint64, minEnough, maxFailed uint64) error {
			return contextCauseError(ctx, dummyErrorInFunc, func(context.Context) error {
				l := log.With().
					Strs("sub_states_owners", subws).
					Uint64("min_enough", minEnough).
					Uint64("max_failed", maxFailed).
					Logger()

				switch ok, enough, failed, err := localReady(subws, localUnmerged, minEnough, maxFailed); {
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
	maxOpsSize uint64,
	getStateMergeValues opindexGetStateMergeValuesFunc,
	newCheckWorkers newCheckEnoughFailedWorkersFunc,
	newFuncUnmergerFunc workerUnmergerNewFuncUnmergerFunc,
) (
	localReady func(subStateOwners []string, localUnmerged []uint64, minEnough, maxFailed uint64) (ok, enough, failed bool, _ error),
	unmergerUnmerge func(worker string, unmerged []uint64) (ok, enough, failed bool, _ error),
	unmergerDone func(),
	unmergerWait inErrorFunc[unmergedFunc],
) {
	addPool, poolIsNotEmpty, traversePool := newFuncWorkerUnmergerUnmergedPool()

	var checkWorker checkEnoughFailedWorkerFunc
	var checkWorkerClose func()

	newUnmergerf, unmergef, donef, waitf := newFuncWorkerUnmergerNewUnmergerFunc(ctx, newFuncUnmergerFunc)

	var once sync.Once
	var newUnmergerErr error

	newUnmerger := func(minEnough, maxFailed uint64) error {
		once.Do(func() {
			switch w, c, err := newCheckWorkers(minEnough, maxFailed); {
			case err != nil:
				newUnmergerErr = err
			default:
				checkWorker, checkWorkerClose = w, c
			}

			newUnmergerErr = newUnmergerf()
		})

		return newUnmergerErr
	}

	workerOpindexFilterLocalReady, workerOpindexFilter,
		newWorkerOpindexFilterClose := newFuncWorkerOpindexFilter(maxOpsSize, getStateMergeValues)

	unmergeOps := func(worker string, unmerged []uint64, unmergeOp inErrorFunc[uint64], filter opindexFilterFunc) (_, _, _ bool, _ error) {
		return checkWorker(worker, func() error {
			return traverseSlice(unmerged, func(_ int, opindex uint64) (bool, error) {
				if filter != nil {
					switch ok, err := filter(opindex); {
					case err != nil:
						return false, err
					case !ok:
						return true, nil
					}
				}

				return true, unmergeOp(opindex)
			})
		})
	}

	return func(subws []string, localUnmerged []uint64, minEnough, maxFailed uint64) (_, _, _ bool, _ error) {
			if err := workerOpindexFilterLocalReady(subws, localUnmerged); err != nil {
				return false, false, false, err
			}

			switch {
			case minEnough < 1: // NOTE zero min enough means, empty state merge values
				return true, true, false, nil
			case minEnough < 2:
				switch notEmpty, err := poolIsNotEmpty(workerOpindexFilter); { // non-empty pool; failed
				case err != nil:
					return false, false, false, err
				case notEmpty:
					return true, false, true, errors.Errorf("unmerged found in pool, but min enough is 1")
				}
			}

			if err := newUnmerger(minEnough, maxFailed); err != nil {
				return false, false, false, err
			}

			var unmergeOp inErrorFunc[uint64]

			if err := unmergef(func(f inErrorFunc[uint64]) error {
				if f == nil {
					return errors.Errorf("empty unmerge of unmerger")
				}

				unmergeOp = f

				return nil
			}); err != nil {
				return false, false, false, err
			}

			if ok, enough, failed, err := unmergeOps(local, nil, unmergeOp, nil); failed || err != nil || enough {
				return ok, enough, failed, err
			}

			var ok, enough, failed bool
			var gerr error

			_ = traversePool(func(worker string, unmerged []uint64) (bool, error) {
				filter := workerOpindexFilter(worker)
				if filter == nil {
					return true, nil
				}

				if o, e, f, err := unmergeOps(worker, unmerged, unmergeOp, filter); f || err != nil || e {
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

			return ok, enough, failed, unmergef(func(f inErrorFunc[uint64]) error {
				if f == nil {
					_ = addPool(worker, slices.Clone(unmerged))

					return nil
				}

				filter := workerOpindexFilter(worker)
				if filter == nil {
					return nil
				}

				var err error

				ok, enough, failed, err = unmergeOps(worker, unmerged, f, filter)

				return err
			})
		},
		donef,
		func(f unmergedFunc) error {
			defer func() {
				if checkWorkerClose != nil {
					checkWorkerClose()
				}

				newWorkerOpindexFilterClose()

				traversePool(nil)
			}()

			return waitf(f)
		}
}

func newFuncWorkerUnmergerUnmergedPool() (
	addPool func(worker string, unmerged []uint64) (added bool),
	isNotEmpty func(filter workerOpindexFilterFunc) (bool, error),
	traverse func(func(worker string, unmerged []uint64) (bool, error)) error,
) {
	pool := map[string] /* worker */ []uint64 /* unmerged */ {}

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
		func(filter workerOpindexFilterFunc) (found bool, _ error) {
			l.RLock()
			defer l.RUnlock()

			return found, traverseMap(pool, func(worker string, unmerged []uint64) (bool, error) {
				return true, traverseSlice(unmerged, func(_ int, opindex uint64) (bool, error) {
					switch f := filter(worker); {
					case f == nil:
						return true, nil
					default:
						switch ok, err := f(opindex); {
						case err != nil:
							return false, err
						case ok:
							found = true

							return false, nil
						default:
							return true, nil
						}
					}
				})
			})
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
	errorFunc,
	func(func(inErrorFunc[uint64]) error) error,
	func(),
	inErrorFunc[unmergedFunc],
) {
	var unmerge inErrorFunc[uint64]
	var done func()
	var wait inErrorFunc[unmergedFunc]

	var l sync.RWMutex
	var waitOnce sync.Once
	var waitErr error

	return func() error {
			l.Lock()
			defer l.Unlock()

			switch u, d, w, err := newFuncUnmergerFunc(ctx); {
			case err != nil:
				return err
			default:
				unmerge, done, wait = u, d, w

				return nil
			}
		},
		func(f func(inErrorFunc[uint64]) error) error {
			l.RLock()
			defer l.RUnlock()

			return f(unmerge)
		},
		func() {
			l.RLock()
			defer l.RUnlock()

			if done != nil {
				done()
			}
		},
		func(f unmergedFunc) error {
			waitOnce.Do(func() {
				if f != nil {
					l.RLock()
					defer l.RUnlock()

					switch {
					case wait != nil:
						waitErr = wait(f)
					default:
						waitErr = f(nil)
					}
				}
			})

			return waitErr
		}
}

func newFuncWorkerOpindexFilter(
	mapSize uint64,
	getStateMergeValues opindexGetStateMergeValuesFunc,
) (
	localReady func(subStateOwners []string, localUnmerged []uint64) error,
	_ workerOpindexFilterFunc,
	_ func(),
) {
	var once sync.Once
	var gerr error

	filter := func(string) opindexFilterFunc { return nil }
	closef := func() {}

	var l sync.RWMutex

	newf := func(subs []string, localUnmerged []uint64) error {
		once.Do(func() {
			if len(subs) < 1 {
				filter = func(string) opindexFilterFunc { return nil }

				return
			}

			var ml sync.Mutex

			switch f, c, err := newFuncOpindexFilter(mapSize, getStateMergeValues, localUnmerged); {
			case err != nil:
				gerr = err
			default:
				m := map[string]struct{}{}
				traverseSlice(subs, func(_ int, owner string) (bool, error) {
					m[owner] = struct{}{}

					return true, nil
				})

				filter = func(worker string) opindexFilterFunc {
					ml.Lock()
					defer ml.Unlock()

					if _, found := m[worker]; !found {
						return nil
					}

					m[worker] = struct{}{}

					return f
				}

				closef = func() {
					ml.Lock()
					defer ml.Unlock()

					clearMap(m)
					c()
				}
			}
		})

		return gerr
	}

	return func(subs []string, localUnmerged []uint64) error {
			return newf(subs, localUnmerged)
		},
		func(worker string) opindexFilterFunc {
			l.RLock()
			defer l.RUnlock()

			return filter(worker)
		},
		func() {
			l.RLock()
			defer l.RUnlock()

			closef()
		}
}
