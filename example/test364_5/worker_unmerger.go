package main

import (
	"context"
	"sync"

	"github.com/spikeekips/mitum/util"
)

type (
	workerUnmergedFunc     func(worker string, unmerged []uint64) error
	workerUnmergerWaitFunc func(unmergedFunc) error
)

// newFuncWorkerUnmerger finishes to merge operations after phase merger.
func newFuncWorkerUnmerger(
	ctx context.Context,
	local string,
	newJobWorker newJobWorkerFunc,
	xMergedOpsMapSize uint64,
	getStateMergeValues opindexGetStateMergeValuesFunc,
	getMergers mergerGetStateMergersFunc,
	newCheckWorkers newCheckEnoughFailedWorkersFunc, // NOTE newLocalCheckEnoughFailedWorkersFunc
	minEnough, maxFailed uint64,
) (
	workerUnmergedFunc,
	workerUnmergerWaitFunc,
	error,
) {
	log := newModuleLoggerFromContext(ctx, "worker-unmerger", nil)

	log.Debug().
		Uint64("min_enough", minEnough).
		Uint64("max_failed", maxFailed).
		Msg("newFuncWorkerUnmerger")

	wctx, wctxCancel := contextWithCancelCause(ctx)

	var checkWorker checkEnoughFailedWorkerFunc
	var checkWorkerClose func()

	switch w, c, err := newCheckWorkers(minEnough, maxFailed); {
	case err != nil:
		return nil, nil, err
	default:
		checkWorker = w
		checkWorkerClose = c
	}

	unmergerUnmerge, unmergerDone, unmergerWait := newFuncUnmergerLocked(
		wctx,
		newJobWorker,
		xMergedOpsMapSize,
		getStateMergeValues,
		getMergers,
	)

	workerPhaseDonePool, workerPhaseDonePoolClose := newFuncWorkerPhaseDonePool(
		local,
		func(worker string, unmerged []uint64) error {
			if len(unmerged) < 1 {
				return nil
			}

			return traverseSlice(unmerged, func(_ int, opindex uint64) (bool, error) {
				return true, unmergerUnmerge(opindex)
			})
		},
	)

	_, workerPhaseDone, workerPhaseDoneWait := newDoneContext(wctx)

	return func(worker string, unmerged []uint64) error {
			return contextCauseError(wctx, dummyErrorInFunc, func(context.Context) error {
				l := log.With().Str("worker", worker).Uints64("unmerged", unmerged).Logger()

				var gerr error

				switch {
				case worker == local:
					gerr = workerPhaseDonePool(worker, nil)
				default:
					gerr = workerPhaseDonePool(worker, unmerged)
				}

				switch ok, enough, failed, err := checkWorker(worker, func() error { return gerr }); {
				case failed:
					l.Error().Err(err).Msg("worker phase done; failed")

					return wctxCancel(workerPhaseDone(err))
				case err != nil: // NOTE ignore
					l.Debug().Err(err).Msg("worker phase done")

					return err
				case !ok:
					l.Debug().Msg("worker phase done; not ok")
				case enough:
					l.Debug().Msg("worker phase done; enough")

					return workerPhaseDone(nil)
				}

				return gerr
			})
		},
		func(f unmergedFunc) error {
			defer func() {
				_ = unmergerWait(nil)
				checkWorkerClose()
				workerPhaseDonePoolClose()
			}()

			return contextCauseError(wctx, wctxCancel, func(context.Context) error {
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
	whenReady func(worker string, unmerged []uint64) error,
) (
	_ workerUnmergedFunc,
	closef func(),
) {
	pool := map[ /* worker */ string] /* unmerged */ []uint64{}

	closePool := func() {
		clear(pool)
		pool = nil
	}

	var l sync.Mutex
	var isReady bool

	return func(worker string, unmerged []uint64) error {
			l.Lock()
			defer l.Unlock()

			if isReady {
				return whenReady(worker, unmerged)
			}

			switch _, found := pool[worker]; {
			case found:
				return nil
			case worker == local:
				isReady = true

				defer closePool()

				if err := traverseMap(pool, func(worker string, unmerged []uint64) (bool, error) {
					return true, whenReady(worker, unmerged)
				}); err != nil {
					return err
				}
			default:
				pool[worker] = unmerged
			}

			return nil
		},
		func() {
			l.Lock()
			defer l.Unlock()

			closePool()
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
	unmergerWaitFunc,
) {
	doneLocked := util.EmptyLocked[func()]()
	waitLocked := util.EmptyLocked[func(unmergedFunc) error]()

	var unmerge opindexErrorFunc

	once := onceFunc(func() error {
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
			if i, isEmpty := waitLocked.Value(); !isEmpty {
				return i(f)
			}

			defer func() {
				doneLocked.EmptyValue()
				waitLocked.EmptyValue()
			}()

			return nil
		}
}
