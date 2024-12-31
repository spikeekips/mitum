package main

import (
	"context"

	dutil "github.com/spikeekips/mitum/example/test364_5/util"
)

type (
	newPhaseMergerFunc = func(
		_ context.Context,
		getStateMergeValues opindexGetStateMergeValuesFunc,
		getStateMergers mergerGetStateMergersFunc,
	) (
		merge,
		unmerge dutil.InErrorFunc[uint64],
		done func(),
		_ dutil.InErrorFunc[phaseMergerWaitCallbackFunc],
		_ error,
	)
	insertWorkerStateMergeValuesFunc  = func(worker string) (dutil.InErrorFunc[uint64], dutil.InErrorFunc[error], error)
	workerPhaseMergerWaitCallbackFunc = func(_ context.Context, unmerged []uint64, reasons map[uint64]string) error
)

// newFuncWorkerPhaseMerger merges operations and collects the phase done from workers.
func newFuncWorkerPhaseMerger(
	ctx context.Context,
	ctxCancel dutil.InErrorFunc[error],
	getStateMergeValues opindexGetStateMergeValuesFunc,
	getStateMergers mergerGetStateMergersFunc,
	newCheckWorkers newCheckEnoughFailedWorkersFunc, // NOTE newLocalCheckEnoughFailedWorkersFunc
	newPhaseMerger newPhaseMergerFunc,
	minEnough, maxFailed uint64,
) (
	insertWorkerStateMergeValuesFunc,
	dutil.InErrorFunc[workerPhaseMergerWaitCallbackFunc],
	error,
) {
	log := dutil.NewModuleLoggerFromContext(ctx, "worker-phase-merger", nil)

	log.Debug().
		Uint64("min_enough", minEnough).
		Uint64("max_failed", maxFailed).
		Msg("new")

	getStateMergeValues = mustFuncGetStateMergeValues(getStateMergeValues)

	var checkWorker checkEnoughFailedWorkerFunc
	var checkWorkerClose func()

	switch w, c, err := newCheckWorkers(minEnough, maxFailed); {
	case err != nil:
		return nil, nil, err
	default:
		checkWorker, checkWorkerClose = w, c
	}

	var phaseMergerMerge dutil.InErrorFunc[uint64]
	var phaseMergerDone func()
	var phaseMergerWait dutil.InErrorFunc[phaseMergerWaitCallbackFunc]

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

	_, workerInsertDone, _, workerInsertWait := dutil.NewContextDone(ctx)

	return func(worker string) (dutil.InErrorFunc[uint64], dutil.InErrorFunc[error], error) {
			l := log.With().Str("worker", worker).Logger()

			return func(opindex uint64) error {
					return dutil.ContextCauseError(ctx, nil, func(context.Context) error {
						return phaseMergerMerge(opindex)
					})
				},
				func(err error) error {
					return dutil.ContextCauseError(ctx, nil, func(context.Context) error {
						if worker == managerName {
							l.Debug().Msg("manager done")

							return dutil.NewIfErrErrorInFunc(ctxCancel)(workerInsertDone(err))
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
				_ = workerInsertWait(nil)
				checkWorkerClose()
			}()

			return dutil.ContextCauseError(ctx, ctxCancel, func(context.Context) error {
				if err := workerInsertWait(nil); err != nil {
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
	newJobWorker dutil.NewJobWorkerFunc,
	maxOpsSize uint64,
) newPhaseMergerFunc {
	return func(
		ctx context.Context,
		getStateMergeValues opindexGetStateMergeValuesFunc,
		getStateMergers mergerGetStateMergersFunc,
	) (
		dutil.InErrorFunc[uint64],
		dutil.InErrorFunc[uint64],
		func(),
		dutil.InErrorFunc[phaseMergerWaitCallbackFunc],
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
