package main

import (
	"context"

	"github.com/rs/zerolog"
)

func newFuncWorker(
	ctx context.Context,
	local string,
	getState createStateFunc,
	jobWorkerSemSize int64,
	xMergedOpsMapSize, stateMergeValuesPoolSize, stateMergersPoolSize uint64,
	extractStateKeyCacheSize int,
	minEnoughOps, maxFailedOps uint64,
) (
	insertWorkerStateMergeValuesFunc,
	inErrorFunc[workerPhaseMergerWaitCallbackFunc],
	error,
) {
	log := newModuleLoggerFromContext(ctx, "worker", nil)

	log.Debug().
		Dict("ops", zerolog.Dict().
			Uint64("min_enough", minEnoughOps).
			Uint64("max_failed", maxFailedOps),
		).
		Msg("newFuncWorker")

	wctx, wctxCancel := contextWithCancelCause(ctx)

	var getStateMergeValues opindexGetStateMergeValuesFunc
	var stateMergeValuesPoolClose func()

	switch g, _, c, err := newStateMergeValuesPool(stateMergeValuesPoolSize); {
	case err != nil:
		return nil, nil, err
	default:
		getStateMergeValues = g
		stateMergeValuesPoolClose = c
	}

	extractStateMergeValueKeys, extractStateKeyClose := newFuncExtractStateMergeValueKeys(extractStateKeyCacheSize)

	var getStateMergers mergerGetStateMergersFunc
	var traverseStateMergers traverseStateMergersFunc

	switch g, t, _, err := newFuncStateMergers(
		stateMergersPoolSize,
		getState,
		extractStateMergeValueKeys,
	); {
	case err != nil:
		return nil, nil, err
	default:
		getStateMergers = g
		traverseStateMergers = t
	}

	newCheckWorkers := newFuncLocalCheckEnoughFailedWorkers(local)

	// FIXME stateMergeValues extractor
	// FIXME if no stateMergeValues for local, report to manager and finish
	// phase merger

	var workerPhaseMergerWorkerInsert insertWorkerStateMergeValuesFunc
	var workerPhaseMergerWait inErrorFunc[workerPhaseMergerWaitCallbackFunc]

	switch i, w, err := newFuncWorkerPhaseMerger(
		wctx,
		getState,
		getStateMergeValues,
		getStateMergers,
		newCheckWorkers,
		newFuncPhaseMergerFunc(
			newFuncNewJobWorkerFunc(jobWorkerSemSize),
			xMergedOpsMapSize,
		),
		minEnoughOps, maxFailedOps,
	); {
	case err != nil:
		return nil, nil, err
	default:
		workerPhaseMergerWorkerInsert = i
		workerPhaseMergerWait = w
	}

	// FIXME merger
	// FIXME save

	return workerPhaseMergerWorkerInsert,
		func(f workerPhaseMergerWaitCallbackFunc) error {
			defer func() {
				_ = workerPhaseMergerWait(nil)
			}()

			return contextCauseError(wctx, wctxCancel, func(context.Context) error {
				defer func() {
					stateMergeValuesPoolClose()
					extractStateKeyClose()
				}()

				_ = traverseStateMergers // FIXME remove

				return workerPhaseMergerWait(f)
			})
		},
		nil
}
