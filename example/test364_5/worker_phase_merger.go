package main

import (
	"context"

	"github.com/pkg/errors"
	dutil "github.com/spikeekips/mitum/example/test364_5/util"
)

type (
	newPhaseMergerFunc = func(
		_ context.Context,
		getStateMergeValues opindexGetStateMergeValuesFunc,
		getStateValueMergers mergerGetStateValueMergersFunc,
	) (
		merge,
		unmerge dutil.InErrorFunc[uint64],
		done func(),
		_ phaseMergerWaitFunc,
		_ error,
	)
	insertWorkerStateMergeValuesFunc = func(worker string) (dutil.InErrorFunc[uint64], dutil.InErrorFunc[error], error)
	workerPhaseMergerWaitFunc        = func() (unmerged []uint64, reasons map[uint64]string, _ error)
)

// newFuncWorkerPhaseMerger merges operations and collects the phase done from workers.
func newFuncWorkerPhaseMerger(
	ctx context.Context,
	local string,
	ctxCancel dutil.InErrorFunc[error],
	getStateMergeValues opindexGetStateMergeValuesFunc,
	getStateValueMergers mergerGetStateValueMergersFunc,
	newPhaseMerger newPhaseMergerFunc,
	opsWorkersSize, opsShare uint64,
) (
	insertWorkerStateMergeValuesFunc,
	workerPhaseMergerWaitFunc,
	error,
) {
	log := dutil.NewModuleLoggerFromContext(ctx, "worker-phase-merger", nil)

	log.Debug().
		Uint64("workers", opsWorkersSize).
		Uint64("ops_share", opsShare).
		Msg("new")

	getStateMergeValues = mustFuncGetStateMergeValues(getStateMergeValues)

	var checkWorker checkFailedWorkersFunc

	switch w, err := newFuncCheckFailedWorkers(opsWorkersSize, opsShare, func(worker string) bool {
		return worker == local
	}); {
	case err != nil:
		return nil, nil, err
	default:
		checkWorker = w
	}

	var phaseMergerMerge dutil.InErrorFunc[uint64]
	var phaseMergerDone func()
	var phaseMergerWait phaseMergerWaitFunc

	switch m, _, d, w, err := newPhaseMerger(
		ctx,
		getStateMergeValues,
		getStateValueMergers,
	); {
	case err != nil:
		return nil, nil, err
	default:
		phaseMergerMerge, phaseMergerDone, phaseMergerWait = m, d, w
	}

	_, workerInsertDone, _, workerInsertWait := dutil.NewContextWait(ctx)

	return func(worker string) (dutil.InErrorFunc[uint64], dutil.InErrorFunc[error], error) {
			l := log.With().Str("worker", worker).Logger()

			return func(opindex uint64) error {
					return dutil.ContextCauseError(ctx, nil, func(context.Context) error {
						switch err := phaseMergerMerge(opindex); {
						case errors.Is(err, context.Canceled): // NOTE ignore error
							return nil
						default:
							return err
						}
					})
				},
				func(err error) error {
					return dutil.ContextCauseError(ctx, nil, func(context.Context) error {
						if worker == managerName {
							l.Debug().Msg("manager done")

							return dutil.NewIfErrErrorInFunc(ctxCancel)(workerInsertDone(err))
						}

						switch ok, enough, failed := checkWorker(worker, err); {
						case failed != nil:
							l.Error().Err(failed).Msg("insert; check workers: failed")

							return ctxCancel(workerInsertDone(failed))
						case !ok:
							l.Debug().Msg("insert; check workers: not ok")
						case enough:
							l.Debug().Msg("insert; check workers: enough")

							_ = workerInsertDone(nil)
						}

						return err
					})
				},
				nil
		},
		func() (unmerged []uint64, reasons map[uint64]string, _ error) {
			defer func() {
				_, _, _, _ = phaseMergerWait()
				_ = workerInsertDone(nil)
			}()

			return unmerged, reasons, dutil.ContextCauseError(ctx, ctxCancel, func(context.Context) error {
				if err := workerInsertWait(); err != nil {
					return err
				}

				phaseMergerDone()

				switch _, newUnmerged, r, err := phaseMergerWait(); {
				case err != nil:
					return err
				default:
					unmerged, reasons = newUnmerged, r

					return nil
				}
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
		getStateValueMergers mergerGetStateValueMergersFunc,
	) (
		dutil.InErrorFunc[uint64],
		dutil.InErrorFunc[uint64],
		func(),
		phaseMergerWaitFunc,
		error,
	) {
		return newFuncPhaseMerger(
			ctx,
			newJobWorker,
			maxOpsSize,
			getStateMergeValues,
			getStateValueMergers,
		)
	}
}
