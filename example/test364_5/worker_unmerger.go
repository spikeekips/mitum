package main

import (
	"context"
	"iter"
	"maps"
	"slices"
	"sync"

	"github.com/pkg/errors"
	dutil "github.com/spikeekips/mitum/example/test364_5/util"
	"github.com/spikeekips/mitum/util"
)

type (
	workerUnmergerNewFuncUnmergerFunc = func(_ context.Context) (
		dutil.InErrorFunc[uint64],
		func(),
		unmergedErrorFunc,
		error,
	)
	workerUnmergerLocalReadyFunc = func(subStatesOwners []string, localUnmerged []uint64) error
	workerUnmergedFunc           = func(worker string, unmerged []uint64) error
	workerOpindexFilterFunc      = func(worker string) (opindexFilterFunc, error)
	workerUnmergerWaitFunc       = unmergedErrorFunc
)

// newFuncWorkerUnmerger finishes to merge operations after phase merger.
func newFuncWorkerUnmerger(
	ctx context.Context,
	local string,
	ctxCancel dutil.InErrorFunc[error],
	newJobWorker dutil.NewJobWorkerFunc,
	statesShare, maxOpsSize uint64,
	getSMVs opindexGetSMVsFunc,
	getSVMs mergerGetSVMsFunc,
) (
	workerUnmergedFunc, // NOTE from states owners, excluding local
	workerUnmergerLocalReadyFunc,
	workerUnmergerWaitFunc,
	error,
) {
	log := dutil.NewModuleLoggerFromContext(ctx, "worker-unmerger", nil)

	log.Debug().
		Str("local", local).
		Uint64("states_share", statesShare).
		Msg("new")

	localReady, workerUnmerge, unmergerDone, unmergerWait := newFuncWorkerUnmergerWorkerUnmergedPool(
		ctx,
		local,
		maxOpsSize,
		getSMVs,
		func(ctx context.Context) (dutil.InErrorFunc[uint64], func(), unmergedErrorFunc, error) {
			return newFuncUnmerger(
				ctx,
				newJobWorker,
				maxOpsSize,
				getSMVs,
				getSVMs,
			)
		},
	)

	wctx, workerPhaseDone, _, workerPhaseDoneWait := dutil.NewContextWait(ctx)

	return func(worker string, unmerged []uint64) error {
			if worker == local {
				return util.ErrFound.Errorf("unexpected local")
			}

			return dutil.ContextCauseError(wctx, nil, func(context.Context) error {
				l := log.With().Str("worker", worker).Uints64("unmerged", unmerged).Logger()

				if worker == managerName {
					l.Debug().Msg("manager done")

					return workerPhaseDone(nil)
				}

				switch ok, enough, failed, err := workerUnmerge(worker, unmerged); {
				case failed != nil:
					l.Error().Err(failed).Msg("worker phase done; failed")

					return ctxCancel(workerPhaseDone(failed))
				case err != nil:
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
		func(subws []string, localUnmerged []uint64) error {
			return dutil.ContextCauseError(wctx, nil, func(context.Context) error {
				l := log.With().Strs("sub_states_owners", subws).Logger()

				switch ok, enough, failed, err := localReady(subws, localUnmerged, statesShare); {
				case failed != nil:
					l.Error().Err(failed).Msg("local phase done; failed")

					return ctxCancel(workerPhaseDone(failed))
				case err != nil:
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
		func() (unmerged []uint64, _ error) {
			defer func() {
				_, _ = unmergerWait()
				_ = workerPhaseDone(nil)
			}()

			return unmerged, dutil.ContextCauseError(ctx, ctxCancel, func(context.Context) error {
				if err := workerPhaseDoneWait(); err != nil {
					return err
				}

				unmergerDone()

				switch u, err := unmergerWait(); {
				case err != nil:
					return err
				default:
					log.Debug().Uints64("unmerged", u).Msg("done")

					unmerged = u

					return nil
				}
			})
		},
		nil
}

func newFuncWorkerUnmergerWorkerUnmergedPool(
	ctx context.Context,
	local string,
	maxOpsSize uint64,
	getSMVs opindexGetSMVsFunc,
	newFuncUnmergerFunc workerUnmergerNewFuncUnmergerFunc,
) (
	localReady func(subStatesOwners []string, localUnmerged []uint64, statesShare uint64) (ok, enough bool, failed error, _ error),
	unmergerUnmerge func(worker string, unmerged []uint64) (ok, enough bool, failed error, _ error),
	unmergerDone func(),
	_ workerUnmergerWaitFunc,
) {
	addPool, _, traversePool := newFuncWorkerUnmergerUnmergedPool()

	var checkWorker checkFailedWorkersFunc

	workerOpindexFilterLocalReady, workerOpindexFilter,
		newWorkerOpindexFilterClose := newFuncWorkerOpindexFilter(maxOpsSize, getSMVs)

	newUnmergerf, unmergef, donef, waitf := newFuncWorkerUnmergerNewUnmergerFunc(
		context.WithValue(ctx, "local", local),
		newFuncUnmergerFunc,
	)

	newUnmerger := func(subws []string, localUnmerged []uint64, statesShare uint64) error {
		if err := workerOpindexFilterLocalReady(subws, localUnmerged); err != nil {
			return err
		}

		nsubws := uint64(len(subws))
		if slices.Index(subws, local) < 0 {
			nsubws++
		}

		switch w, err := newFuncCheckFailedWorkers(nsubws, statesShare, func(worker string) bool {
			return worker == local
		}); {
		case err != nil:
			return err
		default:
			checkWorker = w
		}

		return newUnmergerf()
	}

	var l sync.Mutex

	unmergeOps := func(worker string, unmerged []uint64, unmergeOp dutil.InErrorFunc[uint64], filter opindexFilterFunc) (bool, bool, error) {
		l.Lock()
		defer l.Unlock()

		if filter == nil {
			filter = func(opindex uint64, f dutil.InErrorFunc[uint64]) (bool, error) {
				return true, f(opindex)
			}
		}

		var gerr error

		for opindex := range slices.Values(unmerged) {
			_, err := filter(opindex, unmergeOp)
			if err != nil {
				gerr = err

				break
			}
		}

		return checkWorker(worker, gerr)
	}

	return func(subws []string, localUnmerged []uint64, statesShare uint64) (bool, bool, error, error) {
			if err := newUnmerger(subws, localUnmerged, statesShare); err != nil {
				return false, false, nil, err
			}

			var unmergeOp dutil.InErrorFunc[uint64]

			if err := unmergef(func(f dutil.InErrorFunc[uint64]) error {
				if f == nil {
					return errors.Errorf("empty unmerge of unmerger")
				}

				unmergeOp = f

				return nil
			}); err != nil {
				return false, false, nil, err
			}

		end:
			for worker, unmerged := range traversePool {
				var filter opindexFilterFunc

				switch f, err := workerOpindexFilter(worker); {
				case err != nil || f == nil:
					continue end
				default:
					filter = f
				}

				if o, e, f := unmergeOps(worker, unmerged, unmergeOp, filter); f != nil {
					return o, e, f, nil
				}
			}

			o, e, f := unmergeOps(local, nil, unmergeOp, nil)

			return o, e, f, nil
		},
		func(worker string, unmerged []uint64) (ok, enough bool, failed error, _ error) {
			if worker == local {
				return false, false, nil, util.ErrFound.Errorf("unexpected local")
			}

			return ok, enough, failed, unmergef(func(f dutil.InErrorFunc[uint64]) error {
				if f == nil {
					_ = addPool(worker, unmerged)

					return nil
				}

				var filter opindexFilterFunc

				switch i, err := workerOpindexFilter(worker); {
				case err != nil || f == nil:
					return err
				default:
					filter = i
				}

				var err error

				ok, enough, failed = unmergeOps(worker, unmerged, f, filter)

				return err
			})
		},
		donef,
		func() ([]uint64, error) {
			defer func() {
				newWorkerOpindexFilterClose()

				traversePool(nil)
			}()

			return waitf()
		}
}

func newFuncWorkerUnmergerUnmergedPool() (
	addPool func(worker string, unmerged []uint64) (added bool),
	isNotEmpty func(filter workerOpindexFilterFunc) (bool, error),
	traverse iter.Seq2[string, []uint64],
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
		func(filter workerOpindexFilterFunc) (bool, error) {
			l.RLock()
			defer l.RUnlock()

			for worker, unmerged := range maps.All(pool) {
			end:
				for opindex := range slices.Values(unmerged) {
					switch f, err := filter(worker); {
					case err != nil, f == nil:
						continue end
					default:
						switch ok, err := f(opindex, nil); {
						case err != nil:
							return false, err
						case ok:
							return true, nil
						}
					}
				}
			}

			return false, nil
		},
		func(yield func(string, []uint64) bool) {
			l.Lock()
			defer l.Unlock()

			defer dutil.ClearMap(pool)

			if yield == nil {
				return
			}

			for worker, unmerged := range maps.All(pool) {
				if !yield(worker, unmerged) {
					return
				}
			}
		}
}

func newFuncWorkerUnmergerNewUnmergerFunc(
	ctx context.Context,
	newFuncUnmergerFunc workerUnmergerNewFuncUnmergerFunc,
) (
	dutil.ErrorFunc,
	func(func(dutil.InErrorFunc[uint64]) error) error,
	func(),
	unmergedErrorFunc,
) {
	var unmerge dutil.InErrorFunc[uint64]
	var done func()
	var wait unmergedErrorFunc

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
		func(f func(dutil.InErrorFunc[uint64]) error) error {
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
		func() (unmerged []uint64, _ error) {
			waitOnce.Do(func() {
				l.RLock()
				defer l.RUnlock()

				if wait != nil {
					unmerged, waitErr = wait()
				}
			})

			return unmerged, waitErr
		}
}

func newFuncWorkerOpindexFilter(
	maxOpsSize uint64,
	getSMVs opindexGetSMVsFunc,
) (
	localReady func(subStatesOwners []string, localUnmerged []uint64) error,
	_ workerOpindexFilterFunc,
	_ func(),
) {
	filterWorker := func(string) (opindexFilterFunc, error) { return nil, nil }
	closef := func() {}

	var l sync.RWMutex

	newf := func(subws []string, localUnmerged []uint64) error {
		if len(subws) < 1 {
			filterWorker = func(string) (opindexFilterFunc, error) { return nil, nil }

			return nil
		}

		var ml sync.RWMutex

		var filterOpindex opindexFilterFunc
		var filterOpindexClose func()

		switch f, c, err := newFuncOpindexFilter(maxOpsSize, getSMVs, localUnmerged); {
		case err != nil:
			return err
		default:
			filterOpindex, filterOpindexClose = f, c
		}

		msubws := map[string]struct{}{}
		for owner := range slices.Values(subws) {
			msubws[owner] = struct{}{}
		}

		filterWorker = func(worker string) (opindexFilterFunc, error) {
			ml.RLock()
			defer ml.RUnlock()

			if _, found := msubws[worker]; !found {
				return nil, util.ErrInvalid.Errorf("unknown sub state owner, %q", worker)
			}

			return filterOpindex, nil
		}

		closef = func() {
			ml.Lock()
			defer ml.Unlock()

			dutil.ClearMap(msubws)
			filterOpindexClose()
		}

		return nil
	}

	return func(subws []string, localUnmerged []uint64) error {
			l.Lock()
			defer l.Unlock()

			return newf(subws, localUnmerged)
		},
		func(worker string) (opindexFilterFunc, error) {
			l.RLock()
			defer l.RUnlock()

			return filterWorker(worker)
		},
		func() {
			l.RLock()
			defer l.RUnlock()

			closef()
		}
}
