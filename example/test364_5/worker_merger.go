package dop5

import (
	"context"
	"slices"

	dutil "github.com/spikeekips/mitum/example/test364_5/util"
)

type (
	insertSMVsFunc             = func(opindex uint64, _ []SMV) error
	workerMergerInsertSMVsFunc = func(worker string) (
		insertSMVsFunc,
		dutil.InErrorFunc[error],
		error,
	)
	workerMergerLocalPhaseMergerDoneFunc = func(unmerged []uint64) (subStatesOwners []string, _ error)
	workerMergerWaitFunc                 = func(workerMergerLocalPhaseMergerDoneFunc) (map[uint64]string, error)
)

func newFuncWorkerMerger(
	ctx context.Context,
	local string,
	newJobWorker dutil.NewJobWorkerFunc,
	getState GetStateFunc,
	getSVMs mergerGetSVMsFunc,
	workersSize, maxOpsSize, smvsPoolSize uint64,
	opsWorkers []string, opsShare, statesShare uint64,
) (
	workerMergerInsertSMVsFunc,
	WorkerUnmergedFunc,
	workerMergerWaitFunc,
	error,
) {
	log := dutil.NewModuleLoggerFromContext(ctx, "worker-merger", nil)

	localInOpsWorkers := true
	var mOpsWorkers dutil.LockedMap[string, struct{}]

	opsWorkersSize := uint64(len(opsWorkers))

	switch i, err := dutil.NewLockedMap[string, struct{}](nil, opsWorkersSize, opsWorkersSize); {
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
		opsWorkersSize++
	}

	log.Debug().
		Bool("local_in_ops_workers", localInOpsWorkers).
		Strs("ops_workers", opsWorkers).
		Uint64("ops_share", opsShare).
		Uint64("states_share", statesShare).
		Msg("new")

	wctx, wctxCancel := dutil.ContextWithCancelCause(ctx)

	var getSMVs opindexGetSMVsFunc
	var setSMVs opindexSetSMVsFunc
	var smvsPoolClose func()

	switch g, s, c, err := newSMVsPool(smvsPoolSize); {
	case err != nil:
		return nil, nil, nil, err
	default:
		getSMVs, setSMVs, smvsPoolClose = g, s, c
	}

	var workerPhaseMergerWorkerInsert insertWorkerSMVsFunc
	var workerPhaseMergerWait workerPhaseMergerWaitFunc

	switch i, w, err := newFuncWorkerPhaseMerger(
		wctx,
		local,
		dutil.NewIfErrErrorInFunc(wctxCancel),
		getSMVs,
		getSVMs,
		newFuncPhaseMergerFunc(newJobWorker, maxOpsSize),
		opsWorkersSize, opsShare,
	); {
	case err != nil:
		return nil, nil, nil, err
	default:
		workerPhaseMergerWorkerInsert, workerPhaseMergerWait = i, w
	}

	var workerUnmergerUnmerged WorkerUnmergedFunc
	var workerUnmergerLocalReady workerUnmergerLocalReadyFunc
	var workerUnmergerWait workerUnmergerWaitFunc

	switch u, s, w, err := newFuncWorkerUnmerger(
		wctx,
		local,
		dutil.NewIfErrErrorInFunc(wctxCancel),
		newJobWorker,
		statesShare, maxOpsSize,
		getSMVs,
		getSVMs,
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
		setSMVs,
	)

	wait := newFuncWorkerMergerWait(
		wctx,
		local,
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
				smvsPoolClose()
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
	workerInsert insertWorkerSMVsFunc,
	setSMVs opindexSetSMVsFunc,
) workerMergerInsertSMVsFunc {
	return func(worker string) (
		insertSMVsFunc,
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

		return func(opindex uint64, values []SMV) error {
				if _, err := setSMVs(opindex, values); err != nil {
					return err
				}

				return insert(opindex)
			},
			done,
			nil
	}
}

func newFuncWorkerMergerWait(
	ctx context.Context,
	local string,
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

		if err := workerUnmergerLocalReady(subws, localUnmerged); err != nil {
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
				dutil.ClearSlice(&unmerged)
				dutil.ClearSlice(&s)
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
