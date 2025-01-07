package main

import (
	"context"
	"iter"
	"slices"

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
	getStateMergers mergerGetStateMergersFunc,
	maxWorkersSize, maxOpsSize, stateMergeValuesPoolSize uint64,
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
	case maxWorkersSize < opsWorkersSize:
		maxWorkersSize = opsWorkersSize

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

			_ = dutil.TraverseSlice(opsWorkers, func(_ int, worker string) (bool, error) {
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
		getStateMergers,
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
		getStateMergers,
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
	statesShare uint64,
	workerPhaseMergerWait workerPhaseMergerWaitFunc,
	workerUnmergerLocalReady workerUnmergerLocalReadyFunc,
	workerUnmergerWait workerUnmergerWaitFunc,
) workerMergerWaitFunc {
	log := dutil.LoadContextValueLogger(ctx)

	return func(localPhaseDone workerMergerLocalPhaseMergerDoneFunc) (map[uint64]string, error) {
		var localUnmerged []uint64
		var localReasons map[uint64]string

		defer dutil.ClearMap(localReasons)

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

		if s := len(subws); s > 0 {
			switch e, f, err := minEnoughMaxFailed(uint64(s), statesShare); {
			case err != nil:
				return nil, err
			default:
				minEnough, maxFailed = e, f
			}
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

			switch m, err := mergeUnmergeds(localUnmerged, unmerged); {
			case err != nil:
				return nil, err
			default:
				return filterReasons(localReasons, m), nil
			}
		}
	}
}

func mergeUnmergeds(a, b []uint64) (iter.Seq[uint64], error) {
	switch {
	case len(a) < 1:
		return slices.Values(b), nil
	case len(b) < 1:
		return slices.Values(a), nil
	}

	as, bs := a, b

	if len(b) > len(a) {
		as = b
		bs = a
	}

	var add dutil.AppendSliceAddFunc[uint64]
	var done dutil.OutFunc[[]uint64]

	switch i, d, err := dutil.NewAppendSlice[uint64](uint64(len(as)+len(bs)), 0); {
	case err != nil:
		return nil, err
	default:
		add, done = i, d
	}

	_ = dutil.TraverseSlice(bs, func(_ int, opindex uint64) (bool, error) {
		if slices.Index(as, opindex) >= 0 {
			add(opindex)
		}

		return true, nil
	})

	s := done()
	defer dutil.ClearSlice(s)

	if len(s) < 1 {
		return slices.Values(as), nil
	}

	return func(yield func(uint64) bool) {
			for i := range as {
				if !yield(as[i]) {
					return
				}
			}

			for i := range s {
				if !yield(s[i]) {
					return
				}
			}
		},
		nil
}

func filterReasons(reasons map[uint64]string, unmerged iter.Seq[uint64]) map[uint64]string {
	next, stop := iter.Pull(unmerged)
	defer stop()

	for {
		switch opindex, ok := next(); {
		case !ok:
			return reasons
		default:
			if _, found := reasons[opindex]; !found {
				if reasons == nil {
					reasons = map[uint64]string{}
				}

				reasons[opindex] = ""
			}
		}
	}
}
