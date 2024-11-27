package main

import (
	"context"
	"maps"
	"slices"
)

type (
	addOwnersFunc                                   func(owners []string)
	insertWorkerStateMergeValuesWithStateOwnersFunc func(
		worker string,
		_ func(insertStateMergeValuesFunc, inErrorFunc[[]string]) error,
	) error
	workerMergerCallback                 func(unmerged []uint64, reasons map[uint64]string) error
	workerMergerLocalPhaseMergerDoneFunc func(unmerged []uint64, statesOwners []string) error
	workerMergerWaitFunc                 func(workerMergerLocalPhaseMergerDoneFunc, workerMergerCallback) error
)

// FIXME extractor reports the operation to manager, which has empty state merge
// values.

func newFuncWorkerMerger(
	ctx context.Context,
	local string,
	newJobWorker newJobWorkerFunc,
	getState createStateFunc,
	getStateMergers mergerGetStateMergersFunc,
	maxWorkerSize, xMergedOpsMapSize, stateMergeValuesPoolSize uint64,
	minEnoughOps, maxFailedOps, statesShare uint64,
) (
	insertWorkerStateMergeValuesWithStateOwnersFunc,
	workerUnmergedFunc,
	workerMergerWaitFunc,
	error,
) {
	log := newModuleLoggerFromContext(ctx, "worker-merger", nil)

	log.Debug().
		Uint64("min_enough_ops", minEnoughOps).
		Uint64("max_failed_ops", maxFailedOps).
		Msg("newFuncWorkerMerger")

	wctx, wctxCancel := contextWithCancelCause(ctx)

	var getStateMergeValues opindexGetStateMergeValuesFunc
	var setStateMergeValues opindexSetStateMergeValuesFunc
	var stateMergeValuesPoolClose func()

	switch g, s, c, err := newStateMergeValuesPool(stateMergeValuesPoolSize); {
	case err != nil:
		return nil, nil, nil, err
	default:
		getStateMergeValues = g
		setStateMergeValues = s
		stateMergeValuesPoolClose = c
	}

	newCheckWorkers := newFuncLocalCheckEnoughFailedWorkers(local)

	var workerPhaseMergerWorkerInsert insertWorkerStateMergeValuesFunc
	var workerPhaseMergerWait inErrorFunc[workerPhaseMergerWaitCallbackFunc]

	switch i, w, err := newFuncWorkerPhaseMerger(
		wctx,
		ifErrErrorInFunc(wctxCancel),
		getState,
		getStateMergeValues,
		getStateMergers,
		newCheckWorkers,
		newFuncPhaseMergerFunc(newJobWorker, xMergedOpsMapSize),
		minEnoughOps, maxFailedOps,
	); {
	case err != nil:
		return nil, nil, nil, err
	default:
		workerPhaseMergerWorkerInsert = i
		workerPhaseMergerWait = w
	}

	var workerUnmergerUnmerged workerUnmergedFunc
	var workerUnmergerLocalReady workerUnmergerLocalReadyFunc
	var workerUnmergerWait inErrorFunc[unmergedFunc]

	switch u, s, w, err := newFuncWorkerUnmerger(
		wctx,
		ifErrErrorInFunc(wctxCancel),
		local,
		newJobWorker,
		xMergedOpsMapSize,
		getStateMergeValues,
		getStateMergers,
		newCheckWorkers,
	); {
	case err != nil:
		return nil, nil, nil, err
	default:
		workerUnmergerUnmerged = u
		workerUnmergerLocalReady = s
		workerUnmergerWait = w
	}

	var addStateOwners addOwnersFunc
	var doneStateOwners compactAppendSliceDoneFunc[string]

	switch a, d, err := newFuncCollectStateOwners(maxWorkerSize); {
	case err != nil:
		return nil, nil, nil, err
	default:
		addStateOwners = a
		doneStateOwners = d
	}

	insert := newFuncWorkerMergerInsert(
		wctx,
		workerPhaseMergerWorkerInsert,
		setStateMergeValues,
		addStateOwners,
	)

	wait := newFuncWorkerMergerWait(
		wctx,
		statesShare,
		workerPhaseMergerWait,
		doneStateOwners,
		workerUnmergerLocalReady,
		workerUnmergerWait,
	)

	return insert,
		func(worker string, unmerged []uint64) error {
			return contextCauseError(wctx, dummyErrorInFunc, func(context.Context) error {
				return workerUnmergerUnmerged(worker, unmerged)
			})
		},
		func(localDone workerMergerLocalPhaseMergerDoneFunc, f workerMergerCallback) error {
			defer func() {
				stateMergeValuesPoolClose()
			}()

			return contextCauseError(wctx, wctxCancel, func(context.Context) error {
				return wait(localDone, f)
			})
		},
		nil
}

func newFuncWorkerMergerInsert(
	ctx context.Context,
	workerPhaseMergerWorkerInsert insertWorkerStateMergeValuesFunc,
	setStateMergeValues opindexSetStateMergeValuesFunc,
	addStateOwners addOwnersFunc,
) insertWorkerStateMergeValuesWithStateOwnersFunc {
	return func(
		worker string,
		insert func(insertStateMergeValuesFunc, inErrorFunc[[]string]) error,
	) error {
		return workerPhaseMergerWorkerInsert(worker, func(f insertStateMergeValuesFunc) error {
			return insert(
				func(opindex uint64, values []StateMergeValue) error {
					switch added, err := setStateMergeValues(opindex, values); {
					case err != nil:
						return err
					case !added:
						return nil
					default:
						return f(opindex, values)
					}
				},
				func(stateOwners []string) error {
					addStateOwners(stateOwners)

					return nil
				},
			)
		})
	}
}

func newFuncWorkerMergerWait(
	ctx context.Context,
	statesShare uint64,
	workerPhaseMergerWait inErrorFunc[workerPhaseMergerWaitCallbackFunc],
	doneStateOwners compactAppendSliceDoneFunc[string],
	workerUnmergerLocalReady workerUnmergerLocalReadyFunc,
	workerUnmergerWait inErrorFunc[unmergedFunc],
) workerMergerWaitFunc {
	return func(localPhaseDone workerMergerLocalPhaseMergerDoneFunc, f workerMergerCallback) error {
		var phaseDoneReasons map[uint64]string
		defer clearMap(phaseDoneReasons)

		var localUnmerged []uint64
		defer clear(localUnmerged)

		if err := workerPhaseMergerWait(func(_ context.Context, unmerged []uint64, reasons map[uint64]string) error {
			if err := doneStateOwners(func(statesOwners []string) error {
				if localPhaseDone != nil {
					if err := localPhaseDone(unmerged, statesOwners); err != nil {
						// FIXME broadcast unmerged to other active state owners
						return err
					}
				}

				var minEnough, maxFailed uint64

				if s := len(statesOwners); s > 0 {
					switch e, f, err := minEnoughMaxFailed(uint64(s), statesShare); {
					case err != nil:
						return err
					default:
						minEnough, maxFailed = e, f
					}
				}

				return workerUnmergerLocalReady(statesOwners, unmerged, minEnough, maxFailed)
			}); err != nil {
				return err
			}

			if len(unmerged) > 0 {
				localUnmerged = slices.Clone(unmerged)
			}

			if len(reasons) > 0 {
				phaseDoneReasons = maps.Clone(reasons)
			}

			return nil
		}); err != nil {
			return err
		}

		return workerUnmergerWait(func(unmerged []uint64) error {
			if f != nil {
				return mergeUnmergeds(localUnmerged, unmerged)(func(m []uint64) error {
					n := filterReasons(phaseDoneReasons, m)
					defer clearMap(n)

					return f(m, n)
				})
			}

			return nil
		})
	}
}

func newFuncCollectStateOwners(maxSize uint64) (
	addOwnersFunc,
	compactAppendSliceDoneFunc[string],
	error,
) {
	var (
		add  compactAppendSliceAddFunc[string]
		done compactAppendSliceDoneFunc[string]
	)

	if err := rUniqueCompactAppendSlice[string](
		maxSize, 1<<5, &add, &done); err != nil {
		return nil, nil, err
	}

	return func(owners []string) {
			if len(owners) < 1 {
				return
			}

			add(owners[0], owners[1:]...)
		},
		done,
		nil
}

func mergeUnmergeds(a, b []uint64) func(func([]uint64) error) error {
	switch {
	case len(a) < 1:
		return func(f func([]uint64) error) error { return f(b) }
	case len(b) < 1:
		return func(f func([]uint64) error) error { return f(a) }
	}

	as, bs := a, b

	if len(b) > len(a) {
		as = b
		bs = a
	}

	var add compactAppendSliceAddFunc[uint64]
	var done compactAppendSliceDoneFunc[uint64]

	switch a, d, err := compactAppendSlice[uint64](uint64(len(as)+len(bs)), 0); {
	case err != nil:
		return func(func([]uint64) error) error {
			return err
		}
	default:
		defer d(nil)

		add = a
		done = d
	}

	_ = traverseSlice(bs, func(_ int, opindex uint64) (bool, error) {
		if slices.Index(as, opindex) >= 0 {
			add(opindex)
		}

		return true, nil
	})

	return func(f func([]uint64) error) error {
		return done(func(s []uint64) error {
			if len(s) < 1 {
				return f(as)
			}

			m := make([]uint64, len(as)+len(s))
			defer clear(m)

			copy(m, as)
			copy(m[len(as):], s)

			return f(m)
		})
	}
}
