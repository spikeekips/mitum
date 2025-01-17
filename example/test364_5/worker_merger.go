package main

import (
	"context"
	"slices"
	"sync"

	"github.com/pkg/errors"
	dutil "github.com/spikeekips/mitum/example/test364_5/util"
	"github.com/spikeekips/mitum/util"
)

type (
	insertStateMergeValuesFunc             = func(opindex uint64, _ []StateMergeValue) error
	workerMergerInsertStateMergeValuesFunc = func(worker string) (
		insertStateMergeValuesFunc,
		dutil.InErrorFunc[error],
		error,
	)
	workerMergerLocalPhaseMergerDoneFunc = func(unmerged []uint64) (subStateOwners []string, _ error)
	workerMergerWaitFunc                 = func(workerMergerLocalPhaseMergerDoneFunc) (map[uint64]string, error)
)

func newFuncWorkerMerger(
	ctx context.Context,
	local string,
	newJobWorker dutil.NewJobWorkerFunc,
	getState getStateFunc,
	getStateValueMergers mergerGetStateValueMergersFunc,
	workersSize, maxOpsSize, stateMergeValuesPoolSize uint64,
	opsWorkers []string, opsShare, statesShare uint64,
) (
	workerMergerInsertStateMergeValuesFunc,
	workerUnmergedFunc,
	workerMergerWaitFunc,
	error,
) {
	log := dutil.NewModuleLoggerFromContext(ctx, "worker-merger", nil)

	localInOpsWorkers := true
	var mOpsWorkers util.LockedMap[string, struct{}]
	var minEnoughOps, maxFailedOps uint64

	switch opsWorkersSize := uint64(len(opsWorkers)); {
	case workersSize < opsWorkersSize:
		return nil, nil, nil, errors.Errorf("invalid workers size; < ops workers")
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

			for worker := range slices.Values(opsWorkers) {
				mOpsWorkers.SetValue(worker, struct{}{})
			}
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
		Msg("new")

	wctx, wctxCancel := dutil.ContextWithCancelCause(ctx)

	var getStateMergeValues opindexGetStateMergeValuesFunc
	var setStateMergeValues opindexSetStateMergeValuesFunc
	var stateMergeValuesPoolClose func()

	switch g, s, c, err := newStateMergeValuesPool(stateMergeValuesPoolSize); {
	case err != nil:
		return nil, nil, nil, err
	default:
		getStateMergeValues, setStateMergeValues, stateMergeValuesPoolClose = g, s, c
	}

	newCheckWorkers := newFuncLocalCheckEnoughFailedWorkers(local)

	var workerPhaseMergerWorkerInsert insertWorkerStateMergeValuesFunc
	var workerPhaseMergerWait workerPhaseMergerWaitFunc

	switch i, w, err := newFuncWorkerPhaseMerger(
		wctx,
		dutil.NewIfErrErrorInFunc(wctxCancel),
		getStateMergeValues,
		getStateValueMergers,
		newCheckWorkers,
		newFuncPhaseMergerFunc(newJobWorker, maxOpsSize),
		minEnoughOps, maxFailedOps,
	); {
	case err != nil:
		return nil, nil, nil, err
	default:
		workerPhaseMergerWorkerInsert, workerPhaseMergerWait = i, w
	}

	var workerUnmergerUnmerged workerUnmergedFunc
	var workerUnmergerLocalReady workerUnmergerLocalReadyFunc
	var workerUnmergerWait workerUnmergerWaitFunc

	switch u, s, w, err := newFuncWorkerUnmerger(
		wctx,
		dutil.NewIfErrErrorInFunc(wctxCancel),
		local,
		newJobWorker,
		maxOpsSize,
		getStateMergeValues,
		getStateValueMergers,
		newCheckWorkers,
	); {
	case err != nil:
		return nil, nil, nil, err
	default:
		workerUnmergerUnmerged, workerUnmergerLocalReady, workerUnmergerWait = u, s, w
	}

	unmerge := func(worker string, unmerged []uint64) error {
		return dutil.ContextCauseError(wctx, nil, func(context.Context) error {
			return workerUnmergerUnmerged(worker, unmerged)
		})
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
		unmerge,
	)

	wait := newFuncWorkerMergerWait(
		wctx,
		local,
		workersSize,
		statesShare,
		workerPhaseMergerWait,
		workerUnmergerLocalReady,
		workerUnmergerWait,
	)

	if !localInOpsWorkers {
		switch _, d, err := insert(local); {
		case err != nil:
			return nil, nil, nil, err
		case d != nil:
			if err := d(nil); err != nil {
				return nil, nil, nil, err
			}
		}
	}

	return insert, unmerge,
		func(localDone workerMergerLocalPhaseMergerDoneFunc) (reasons map[uint64]string, _ error) {
			defer func() {
				stateMergeValuesPoolClose()
				mOpsWorkers.Close()
			}()

			return reasons, dutil.ContextCauseError(wctx, wctxCancel, func(context.Context) error {
				switch r, err := wait(localDone); {
				case err != nil:
					return err
				default:
					reasons = r

					return nil
				}
			})
		},
		nil
}

func newFuncWorkerMergerInsert(
	ctx context.Context,
	isOpsWorker func(worker string) bool,
	workerInsert insertWorkerStateMergeValuesFunc,
	setStateMergeValues opindexSetStateMergeValuesFunc,
	unmerge workerUnmergedFunc,
) workerMergerInsertStateMergeValuesFunc {
	var l sync.Mutex

	return func(worker string) (
		insertStateMergeValuesFunc,
		dutil.InErrorFunc[error],
		error,
	) {
		switch {
		case worker == managerName:
		case !isOpsWorker(worker):
			return nil, nil, nil
		}

		var insert dutil.InErrorFunc[uint64]
		var done dutil.InErrorFunc[error]

		switch i, d, err := workerInsert(worker); {
		case err != nil:
			return nil, nil, err
		default:
			insert, done = i, d
		}

		f := func(opindex uint64, values []StateMergeValue) error {
			return dutil.ContextCauseError(ctx, nil, func(context.Context) error {
				l.Lock()
				defer l.Unlock()

				switch added, err := setStateMergeValues(opindex, values); {
				case err != nil:
					return err
				case added:
					return insert(opindex)
				}

				return nil
			})
		}

		return f, done, nil
	}
}

func newFuncWorkerMergerWait(
	ctx context.Context,
	local string,
	workersSize, statesShare uint64,
	workerPhaseMergerWait workerPhaseMergerWaitFunc,
	workerUnmergerLocalReady workerUnmergerLocalReadyFunc,
	workerUnmergerWait workerUnmergerWaitFunc,
) workerMergerWaitFunc {
	log := dutil.LoadContextValueLogger(ctx)

	return func(localPhaseDone workerMergerLocalPhaseMergerDoneFunc) (map[uint64]string, error) {
		var localUnmerged []uint64
		var localReasons map[uint64]string

		switch unmerged, reasons, err := workerPhaseMergerWait(); {
		case err != nil:
			return nil, err
		default:
			localUnmerged, localReasons = unmerged, reasons
		}

		var subws []string

		if localPhaseDone != nil {
			switch i, err := localPhaseDone(localUnmerged); {
			case err != nil:
				return nil, err
			default:
				subws = i
			}
		}

		log.Debug().
			Uints64("local_unmerged", localUnmerged).
			Interface("local_reasons", localReasons).
			Strs("sub_states_owners", subws).
			Msg("phase merger wait done")

		var minEnough, maxFailed uint64

		switch e, f, err := minEnoughMaxFailed(workersSize, statesShare); {
		case err != nil:
			return nil, err
		default:
			minEnough, maxFailed = e, f
		}

		if ns := uint64(len(subws)); ns != workersSize {
			minEnough = ns
			maxFailed = 1
		}

		if err := workerUnmergerLocalReady(subws, localUnmerged, minEnough, maxFailed); err != nil {
			return nil, err
		}

		switch unmerged, err := workerUnmergerWait(); {
		case err != nil:
			return nil, err
		default:
			log.Debug().
				Uints64("unmerged", unmerged).
				Uints64("local_unmerged", localUnmerged).
				Msg("unmerger wait done")

			s := slices.Concat(localUnmerged, unmerged)
			defer func() {
				dutil.ClearSlice(unmerged)
				dutil.ClearSlice(s)
			}()

			slices.Sort(s)

			return filterReasons(localReasons, slices.Compact(s)), nil
		}
	}
}

func filterReasons(reasons map[uint64]string, unmerged []uint64) map[uint64]string {
	for i := range unmerged {
		opindex := unmerged[i]

		if _, found := reasons[opindex]; !found {
			if reasons == nil {
				reasons = map[uint64]string{}
			}

			reasons[opindex] = ""
		}
	}

	return reasons
}
