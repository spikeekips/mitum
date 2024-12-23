package main

import (
	"context"
)

type (
	newPhaseMergerFunc = func(
		_ context.Context,
		getStateMergeValues opindexGetStateMergeValuesFunc,
		getStateMergers mergerGetStateMergersFunc,
	) (
		merge,
		unmerge inErrorFunc[uint64],
		done func(),
		_ inErrorFunc[phaseMergerWaitCallbackFunc],
		_ error,
	)
	insertWorkerStateMergeValuesFunc  = func(worker string) (inErrorFunc[uint64], inErrorFunc[error], error)
	workerPhaseMergerWaitCallbackFunc = func(_ context.Context, unmerged []uint64, reasons map[uint64]string) error
)

// newFuncWorkerPhaseMerger merges operations and collects the phase done from workers.
func newFuncWorkerPhaseMerger(
	ctx context.Context,
	ctxCancel inErrorFunc[error],
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

	getStateMergeValues = mustFuncGetStateMergeValues(getStateMergeValues)

	var checkWorker checkEnoughFailedWorkerFunc
	var checkWorkerClose func()

	switch w, c, err := newCheckWorkers(minEnough, maxFailed); {
	case err != nil:
		return nil, nil, err
	default:
		checkWorker, checkWorkerClose = w, c
	}

	var phaseMergerMerge inErrorFunc[uint64]
	var phaseMergerDone func()
	var phaseMergerWait inErrorFunc[phaseMergerWaitCallbackFunc]

	switch m, _, d, w, err := newPhaseMerger(
		ctx,
		getStateMergeValues,
		getStateMergers,
	); {
	case err != nil:
		return nil, nil, err
	default:
		phaseMergerMerge, phaseMergerDone, phaseMergerWait = m, d, w
	}

	_, workerInsertDone, workerInsertWait := newContextDone(ctx)

	return func(worker string) (inErrorFunc[uint64], inErrorFunc[error], error) {
			l := log.With().Str("worker", worker).Logger()

			return func(opindex uint64) error {
					return contextCauseError(ctx, nil, func(context.Context) error {
						return phaseMergerMerge(opindex)
					})
				},
				func(err error) error {
					return contextCauseError(ctx, nil, func(context.Context) error {
						if worker == managerName {
							l.Debug().Msg("manager done")

							return newIfErrErrorInFunc(ctxCancel)(workerInsertDone(err))
						}

						switch ok, enough, failed, err := checkWorker(worker, func() error { return err }); {
						case failed:
							l.Error().Err(err).Msg("insert; check workers: failed")

							return ctxCancel(workerInsertDone(err))
						case err != nil: // NOTE ignore
							l.Debug().Err(err).Msg("insert")

							return err
						case !ok:
							l.Debug().Msg("insert; check workers: not ok")
						case enough:
							l.Debug().Msg("insert; check workers: enough")

							_ = workerInsertDone(nil)
						}

						return nil
					})
				},
				nil
		},
		func(f workerPhaseMergerWaitCallbackFunc) error {
			defer func() {
				_ = phaseMergerWait(nil)
				workerInsertDone(nil)
				checkWorkerClose()
			}()

			return contextCauseError(ctx, ctxCancel, func(context.Context) error {
				if err := workerInsertWait(); err != nil {
					return err
				}

				phaseMergerDone()

				return phaseMergerWait(func(_, newUnmerged []uint64, reasons map[uint64]string) error {
					if f != nil {
						return f(ctx, newUnmerged, reasons)
					}

					return nil
				})
			})
		},
		nil
}

func newFuncPhaseMergerFunc(
	newJobWorker newJobWorkerFunc,
	maxOpsSize uint64,
) newPhaseMergerFunc {
	return func(
		ctx context.Context,
		getStateMergeValues opindexGetStateMergeValuesFunc,
		getStateMergers mergerGetStateMergersFunc,
	) (
		inErrorFunc[uint64],
		inErrorFunc[uint64],
		func(),
		inErrorFunc[phaseMergerWaitCallbackFunc],
		error,
	) {
		return newFuncPhaseMerger(
			ctx,
			newJobWorker,
			maxOpsSize,
			getStateMergeValues,
			getStateMergers,
		)
	}
}
