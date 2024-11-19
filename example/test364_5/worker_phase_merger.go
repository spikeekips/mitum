package main

import (
	"context"
)

type (
	newPhaseMergerFunc func(
		_ context.Context,
		getStateMergeValues opindexGetStateMergeValuesFunc,
		getStateMergers mergerGetStateMergersFunc,
	) (
		merge,
		unmerge opindexErrorFunc,
		done func(),
		_ inErrorFunc[phaseMergerWaitCallbackFunc],
		_ error,
	)
	setStateOpFunc                    func(phase int, opindex uint64, stateKeys []string, _ xMergeFlag)
	getStateOpsFunc                   func(stateKey string) (ops []uint64)
	getStatesOpsFunc                  func(inErrorFunc[getStateOpsFunc]) error
	insertStateMergeValuesFunc        func(opindex uint64, _ []StateMergeValue) error
	insertWorkerStateMergeValuesFunc  func(worker string, _ inErrorFunc[insertStateMergeValuesFunc]) error
	workerPhaseMergerWaitCallbackFunc func(_ context.Context, unmerged []uint64, reasons map[uint64]string) error
)

// newFuncWorkerPhaseMerger merges operations and collects the phase done from workers.
func newFuncWorkerPhaseMerger(
	ctx context.Context,
	getState createStateFunc,
	getStateMergeValues opindexGetStateMergeValuesFunc,
	getStateMergers mergerGetStateMergersFunc,
	newCheckWorkers newCheckEnoughFailedWorkersFunc, // NOTE newLocalCheckEnoughFailedWorkersFunc
	newPhaseMerger newPhaseMergerFunc,
	minEnough, maxFailed uint64,
) (
	insertWorkerStateMergeValuesFunc,
	inErrorFunc[workerPhaseMergerWaitCallbackFunc],
	error,
) {
	log := newModuleLoggerFromContext(ctx, "worker-phase-merger", nil)

	log.Debug().
		Uint64("min_enough", minEnough).
		Uint64("max_failed", maxFailed).
		Msg("newFuncWorkerPhaseMerger")

	wctx, wctxCancel := contextWithCancelCause(ctx)

	getStateMergeValues = mustFuncGetStateMergeValues(getStateMergeValues)

	var checkWorkerInsert checkEnoughFailedWorkerFunc
	var checkWorkerInsertClose func()

	switch w, c, err := newCheckWorkers(minEnough, maxFailed); {
	case err != nil:
		return nil, nil, err
	default:
		checkWorkerInsert = w
		checkWorkerInsertClose = c
	}

	var phaseMergerMerge opindexErrorFunc
	var phaseMergerDone func()
	var phaseMergerWait inErrorFunc[phaseMergerWaitCallbackFunc]

	switch m, _, d, w, err := newPhaseMerger(
		wctx,
		getStateMergeValues,
		getStateMergers,
	); {
	case err != nil:
		return nil, nil, err
	default:
		phaseMergerMerge = m
		phaseMergerDone = d
		phaseMergerWait = w
	}

	_, workerInsertDone, workerInsertWait := newDoneContext(wctx)

	return func(worker string, f inErrorFunc[insertStateMergeValuesFunc]) error {
			l := log.With().Str("worker", worker).Logger()

			werr := contextCauseError(wctx, dummyErrorInFunc,
				func(context.Context) error {
					return f(func(opindex uint64, values []StateMergeValue) error {
						return phaseMergerMerge(opindex)
					})
				},
			)

			switch ok, enough, failed, err := checkWorkerInsert(worker, func() error { return werr }); {
			case failed:
				l.Error().Err(err).Msg("insert; check workers: failed")

				return wctxCancel(workerInsertDone(err))
			case err != nil: // NOTE ignore
				l.Debug().Err(err).Msg("insert")

				return err
			case !ok:
				l.Debug().Msg("insert; check workers: not ok")
			case enough:
				l.Debug().Msg("insert; check workers: enough")

				_ = workerInsertDone(nil)
			}

			return werr
		},
		func(f workerPhaseMergerWaitCallbackFunc) error {
			defer func() {
				_ = phaseMergerWait(nil)
				workerInsertDone(nil)
				checkWorkerInsertClose()
			}()

			return contextCauseError(wctx, wctxCancel, func(context.Context) error {
				if err := workerInsertWait(); err != nil {
					return err
				}

				phaseMergerDone()

				return phaseMergerWait(func(_, newUnmerged []uint64, reasons map[uint64]string) error {
					if f == nil {
						return nil
					}

					return f(wctx, newUnmerged, reasons)
				})
			})
		},
		nil
}

func newFuncPhaseMergerFunc(
	newJobWorker newJobWorkerFunc,
	xMergedOpsMapSize uint64,
) newPhaseMergerFunc {
	return func(
		ctx context.Context,
		getStateMergeValues opindexGetStateMergeValuesFunc,
		getStateMergers mergerGetStateMergersFunc,
	) (
		opindexErrorFunc,
		opindexErrorFunc,
		func(),
		inErrorFunc[phaseMergerWaitCallbackFunc],
		error,
	) {
		return newFuncPhaseMerger(
			ctx,
			newJobWorker,
			xMergedOpsMapSize,
			getStateMergeValues,
			getStateMergers,
		)
	}
}
