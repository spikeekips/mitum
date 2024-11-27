package main

import (
	"context"
	"slices"

	"github.com/rs/zerolog"
	"github.com/spikeekips/mitum/util"
)

type (
	insertWorkerStateMergeValuesWithStateOwnersFunc func(
		worker string,
		_ func(
			insertStateMergeValuesFunc,
			inErrorFunc[ /* sub state owners */ []string],
			inErrorFunc[ /* affected state owners */ []string],
		) error,
	) error
	workerReportStateOwnersFunc func(stateOwners []string) error
	workerReportResultFunc      func(
		unmerged []uint64,
		reasons map[uint64]string,
		stateKeys []string,
	) error
	workerBroadcastUnmergedFunc func(workers []string, unmerged []uint64) error
	workerWaitFunc              func(
		workerReportStateOwnersFunc,
		workerBroadcastUnmergedFunc,
		workerReportResultFunc,
	) error
)

type workerOptions struct {
	Local                       string
	JobWorkerSemSize            int64
	MaxExtractStateKeyCacheSize int
}

func newWorkerOptions(local string) *workerOptions {
	return &workerOptions{
		Local:                       local,
		JobWorkerSemSize:            1 << 9,
		MaxExtractStateKeyCacheSize: 1 << 13,
	}
}

func newFuncWorker(
	ctx context.Context,
	options *workerOptions,
	getState createStateFunc,
	workID string,
	maxOpsSize uint64,
	workers, opsWorkers []string,
	opsShare, statesShare uint64,
) (
	insertWorkerStateMergeValuesWithStateOwnersFunc,
	workerUnmergedFunc,
	workerWaitFunc,
	error,
) {
	log := newModuleLoggerFromContext(ctx, "worker", func(zctx zerolog.Context) zerolog.Context {
		return zctx.
			Str("work_id", workID).
			Str("local", options.Local)
	})

	log.Debug().
		Interface("options", options).
		Uint64("max_ops_size", maxOpsSize).
		Dict("workers", zerolog.Dict().
			Strs("all", workers).
			Strs("ops", opsWorkers),
		).
		Dict("share", zerolog.Dict().
			Uint64("ops", opsShare).
			Uint64("states", statesShare),
		).
		Msg("newFuncWorker")

	if err := isValidWorkerArgs(
		options.Local,
		workers, opsWorkers,
		opsShare, statesShare,
	); err != nil {
		return nil, nil, nil, err
	}

	wctx, wctxCancel := contextWithCancelCause(ctx)

	approxStatesKeys := maxOpsSize * 2

	extractStateMergeValueKeys, extractStateKeyClose := newFuncExtractStateMergeValueKeys(
		options.MaxExtractStateKeyCacheSize, int(approxStatesKeys))

	var getStateMergers mergerGetStateMergersFunc
	var traverseStateMergers traverseStateMergersFunc
	var stateMergersClose func()

	switch g, t, c, err := newFuncStateMergers(
		approxStatesKeys,
		getState,
		extractStateMergeValueKeys,
	); {
	case err != nil:
		return nil, nil, nil, err
	default:
		getStateMergers, traverseStateMergers, stateMergersClose = g, t, c
	}

	var addAffectedStateOwners addOwnersFunc
	var doneAffectedStateOwners compactAppendSliceDoneFunc[string]

	switch a, d, err := newFuncCollectStateOwners(uint64(len(workers))); {
	case err != nil:
		return nil, nil, nil, err
	default:
		addAffectedStateOwners, doneAffectedStateOwners = a, d
	}

	// FIXME stateMergeValues extractor

	var insertWorkerStateMergeValues insertWorkerStateMergeValuesWithSubStateOwnersFunc
	var workerUnmerged workerUnmergedFunc
	var workerMergerWait workerMergerWaitFunc

	switch i, u, w, err := newFuncWorkerMerger(
		wctx,
		options.Local,
		newFuncNewJobWorkerFunc(options.JobWorkerSemSize),
		getState,
		getStateMergers,
		uint64(len(workers)), maxOpsSize, approxStatesKeys,
		opsWorkers, opsShare, statesShare,
	); {
	case err != nil:
		return nil, nil, nil, err
	default:
		insertWorkerStateMergeValues, workerUnmerged, workerMergerWait = i, u, w
	}

	insert := newFuncWorkerInsert(wctx, insertWorkerStateMergeValues, addAffectedStateOwners)

	return insert,
		workerUnmerged,
		func(
			reportStateOwners workerReportStateOwnersFunc,
			broadcastUnmerged workerBroadcastUnmergedFunc,
			reportMerged workerReportResultFunc,
		) error {
			defer func() {
				_ = workerMergerWait(nil, nil)
			}()

			return contextCauseError(wctx, wctxCancel, func(context.Context) error {
				defer func() {
					extractStateKeyClose()
					doneAffectedStateOwners(nil)
					stateMergersClose()
				}()

				if err := workerMergerWait(
					func(unmerged []uint64, subs []string) error {
						_ = doneAffectedStateOwners(func(affected []string) error {
							log.Debug().Strs("affected_states_owners", affected).Msg("worker merger wait done")

							if err := mergeStateOwners(subs, affected)(func(allStateOwners []string) error {
								return reportStateOwners(allStateOwners)
							}); err != nil {
								return err
							}

							return broadcastUnmerged(affected, unmerged)
						})

						return nil
					},
					func(unmerged []uint64, reasons map[uint64]string) error {
						_ = traverseStateMergers // FIXME remove

						return reportMerged(unmerged, reasons, nil) // FIXME state keys
					},
				); err != nil {
					return err
				}

				// FIXME save (operations, states) and report saved files info to manager

				return nil
			})
		},
		nil
}

func isValidWorkerArgs(
	local string,
	workers, opsWorkers []string,
	opsShare, statesShare uint64,
) error {
	switch {
	case local == "":
		return util.ErrInvalid.Errorf("empty local worker name")
	case opsShare < 1:
		return util.ErrInvalid.Errorf("zero ops share")
	case statesShare < 1:
		return util.ErrInvalid.Errorf("zero states share")
	case len(workers) < 1:
		return util.ErrInvalid.Errorf("empty workers")
	case len(opsWorkers) < 1:
		return util.ErrInvalid.Errorf("empty ops workers")
	}

	return traverseSlice(opsWorkers, func(_ int, worker string) (bool, error) {
		if slices.Index(workers, worker) < 0 {
			return false, util.ErrInvalid.Errorf("unknown ops worker, %q", worker)
		}

		return true, nil
	})
}

func newFuncWorkerInsert(
	ctx context.Context,
	insertWorker insertWorkerStateMergeValuesWithSubStateOwnersFunc,
	addAffectedStateOwners addOwnersFunc,
) insertWorkerStateMergeValuesWithStateOwnersFunc {
	return func(
		worker string,
		f func(
			insertStateMergeValuesFunc,
			inErrorFunc[[]string],
			inErrorFunc[[]string],
		) error,
	) error {
		return insertWorker(
			worker,
			func(ivf insertStateMergeValuesFunc, sub inErrorFunc[[]string]) error {
				return f(ivf, sub, func(affected []string) error {
					addAffectedStateOwners(affected)

					return nil
				})
			},
		)
	}
}

func mergeStateOwners(a, b []string) func(func([]string) error) error {
	switch {
	case len(a) < 1:
		return func(f func([]string) error) error { return f(b) }
	case len(b) < 1:
		return func(f func([]string) error) error { return f(a) }
	}

	as, bs := a, b

	if len(b) > len(a) {
		as = b
		bs = a
	}

	var add compactAppendSliceAddFunc[string]
	var done compactAppendSliceDoneFunc[string]

	if err := rCompactAppendSlice[string](uint64(len(as)+len(bs)), 0, &add, &done); err != nil {
		return func(func([]string) error) error {
			return err
		}
	}

	defer done(nil)

	_ = traverseSlice(bs, func(_ int, opindex string) (bool, error) {
		if slices.Index(as, opindex) >= 0 {
			add(opindex)
		}

		return true, nil
	})

	return func(f func([]string) error) error {
		return done(func(s []string) error {
			if len(s) < 1 {
				return f(as)
			}

			m := make([]string, len(as)+len(s))
			defer clear(m)

			copy(m, as)
			copy(m[len(as):], s)

			return f(m)
		})
	}
}
