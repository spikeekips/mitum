package main

import (
	"context"
	"maps"
	"slices"

	"github.com/spikeekips/mitum/util"
)

type (
	addOwnersFunc                                   func(owners []string)
	insertWorkerStateMergeValuesWithStateOwnersFunc func(
		worker string,
		_ func(insertStateMergeValuesFunc, inErrorFunc[[]string]) error,
	) error
	workerMergerCallback                 func(unmerged []uint64, reasons map[uint64]string) error
	workerMergerLocalPhaseMergerDoneFunc func(unmerged []uint64, subStatesOwners []string) error
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
	maxWorkerSize, maxOpsSize, stateMergeValuesPoolSize uint64,
	opsWorkers []string, opsShare, statesShare uint64,
) (
	insertWorkerStateMergeValuesWithStateOwnersFunc,
	workerUnmergedFunc,
	workerMergerWaitFunc,
	error,
) {
	log := newModuleLoggerFromContext(ctx, "worker-merger", nil)

	localInOpsWorkers := true
	var mOpsWorkers util.LockedMap[string, struct{}]
	var minEnoughOps, maxFailedOps uint64

	switch opsWorkersSize := uint64(len(opsWorkers)); {
	case maxWorkerSize < opsWorkersSize:
		maxWorkerSize = opsWorkersSize

		fallthrough
	default:
		switch e, f, err := minEnoughMaxFailed(opsWorkersSize, opsShare); {
		case err != nil:
			return nil, nil, nil, err
		default:
			minEnoughOps, maxFailedOps = e, f
		}

		switch i, err := util.NewLockedMap[string, struct{}](opsWorkersSize, nil); {
		case err != nil:
			return nil, nil, nil, err
		default:
			mOpsWorkers = i

			_ = traverseSlice(opsWorkers, func(_ int, worker string) (bool, error) {
				mOpsWorkers.SetValue(worker, struct{}{})

				return true, nil
			})
		}

		if !mOpsWorkers.Exists(local) {
			localInOpsWorkers = false

			// NOTE if local is not in ops workers, to finish phase merger,
			// increase min enough
			minEnoughOps++
		}
	}

	log.Debug().
		Bool("local_in_ops_workers", localInOpsWorkers).
		Strs("ops_workers", opsWorkers).
		Uint64("ops_share", opsShare).
		Uint64("states_share", statesShare).
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
		newFuncPhaseMergerFunc(newJobWorker, maxOpsSize),
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
		maxOpsSize,
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
		func(worker string) bool {
			if worker == local {
				return true
			}

			return mOpsWorkers.Exists(worker)
		},
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

	if !localInOpsWorkers {
		if err := insert(local, func(insertStateMergeValuesFunc, inErrorFunc[[]string]) error {
			return nil
		}); err != nil {
			return nil, nil, nil, err
		}
	}

	return insert,
		func(worker string, unmerged []uint64) error {
			return contextCauseError(wctx, dummyErrorInFunc, func(context.Context) error {
				return workerUnmergerUnmerged(worker, unmerged)
			})
		},
		func(localDone workerMergerLocalPhaseMergerDoneFunc, f workerMergerCallback) error {
			defer func() {
				stateMergeValuesPoolClose()
				mOpsWorkers.Close()
			}()

			return contextCauseError(wctx, wctxCancel, func(context.Context) error {
				return wait(localDone, f)
			})
		},
		nil
}

func newFuncWorkerMergerInsert(
	ctx context.Context,
	isOpsWorker func(worker string) bool,
	workerInsert insertWorkerStateMergeValuesFunc,
	setStateMergeValues opindexSetStateMergeValuesFunc,
	addStateOwners addOwnersFunc,
) insertWorkerStateMergeValuesWithStateOwnersFunc {
	return func(
		worker string,
		insert func(insertStateMergeValuesFunc, inErrorFunc[[]string]) error,
	) error {
		if !isOpsWorker(worker) {
			return nil
		}

		return workerInsert(worker, func(f insertStateMergeValuesFunc) error {
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
	log := loadContextValueLogger(ctx)

	return func(localPhaseDone workerMergerLocalPhaseMergerDoneFunc, f workerMergerCallback) error {
		var phaseDoneReasons map[uint64]string
		defer clearMap(phaseDoneReasons)

		var localUnmerged []uint64
		defer clear(localUnmerged)

		if err := workerPhaseMergerWait(func(_ context.Context, unmerged []uint64, reasons map[uint64]string) error {
			if err := doneStateOwners(func(subStatesOwners []string) error {
				log.Debug().
					Uints64("unmerged", unmerged).
					Interface("reasons", reasons).
					Strs("sub_states_owners", subStatesOwners).
					Msg("phase merger wait done")

				if localPhaseDone != nil {
					if err := localPhaseDone(unmerged, subStatesOwners); err != nil {
						// FIXME broadcast unmerged to other active state owners
						return err
					}
				}

				var minEnough, maxFailed uint64

				if s := len(subStatesOwners); s > 0 {
					switch e, f, err := minEnoughMaxFailed(uint64(s), statesShare); {
					case err != nil:
						return err
					default:
						minEnough, maxFailed = e, f
					}
				}

				return workerUnmergerLocalReady(subStatesOwners, unmerged, minEnough, maxFailed)
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
			log.Debug().
				Uints64("unmerged", unmerged).
				Uints64("local_unmerged", localUnmerged).
				Msg("unmerger wait done")

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
