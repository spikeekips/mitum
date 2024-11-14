package main

import (
	"context"
)

type (
	newPhaseMergerFunc func(
		_ context.Context,
		getStateMergeValues mergerGetStateMergeValuesFunc,
		getStateMergers mergerGetStateMergersFunc,
	) (
		merge,
		unmerge phaseMergerXMergeFunc,
		done func(),
		wait phaseMergerWaitFunc,
		_ error,
	)
	setStateOpFunc                          func(phase int, opindex uint64, stateKeys []string, _ xMergeFlag)
	getStateOpsCallbackFunc                 func(stateKey string) (ops []uint64)
	getStateOpsFunc                         func(func(getStateOpsCallbackFunc) error) error
	workerPhaseMergerWorkerInsertValuesFunc func(opindex uint64, _ []StateMergeValue) error
	workerPhaseMergerWorkerInsertFunc       func(worker string, _ func(workerPhaseMergerWorkerInsertValuesFunc) error) error
	workerPhaseMergerWaitCallbackFunc       func(_ context.Context, unmerged []uint64, reasons map[uint64]string) error
	workerPhaseMergerWaitFunc               func(workerPhaseMergerWaitCallbackFunc) error
)

// newFuncWorkerPhaseMerger merges operations and collects the phase done from workers.
func newFuncWorkerPhaseMerger(
	ctx context.Context,
	getState getStateFunc,
	getStateMergeValues mergerGetStateMergeValuesFunc,
	getStateMergers mergerGetStateMergersFunc,
	newCheckWorkers checkEnoughFailedWorkersFunc, // NOTE newLocalCheckEnoughFailedWorkersFunc
	newPhaseMerger newPhaseMergerFunc,
	minEnough, maxFailed uint64,
) (
	workerPhaseMergerWorkerInsertFunc,
	workerPhaseMergerWaitFunc,
	error,
) {
	log := newModuleLoggerFromContext(ctx, "worker-phase-merger", nil)

	log.Debug().
		Uint64("min_enough", minEnough).
		Uint64("max_failed", maxFailed).
		Msg("newFuncWorkerPhaseMerger")

	wctx, wctxCancel := contextWithCancelCause(ctx)

	var checkWorkerInsert checkEnoughFailedWorkerFunc
	var checkWorkerInsertClose func()

	switch w, c, err := newCheckWorkers(minEnough, maxFailed); {
	case err != nil:
		return nil, nil, err
	default:
		checkWorkerInsert = w
		checkWorkerInsertClose = c
	}

	var phaseMergerMerge phaseMergerXMergeFunc
	var phaseMergerDone func()
	var phaseMergerWait phaseMergerWaitFunc

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

	return func(worker string, f func(workerPhaseMergerWorkerInsertValuesFunc) error) error {
			l := log.With().Str("worker", worker).Logger()

			werr := contextCauseError(wctx, dummyErrorFunc,
				func(context.Context) error {
					return f(func(opindex uint64, values []StateMergeValue) error {
						return phaseMergerMerge(opindex)
					})
				},
			)

			switch ok, enough, failed, err := checkWorkerInsert(worker, func() error { return werr }); {
			case failed:
				l.Debug().Msg("insert; check workers: failed")

				return workerInsertDone(err)
			case err != nil: // NOTE ignore
				l.Debug().Interface("err", err).Msg("insert")

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
			}()

			return contextCauseError(wctx, wctxCancel, func(context.Context) error {
				defer func() {
					workerInsertDone(nil)
					checkWorkerInsertClose()
				}()

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
		getStateMergeValues mergerGetStateMergeValuesFunc,
		getStateMergers mergerGetStateMergersFunc,
	) (
		phaseMergerXMergeFunc,
		phaseMergerXMergeFunc,
		func(),
		phaseMergerWaitFunc,
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
