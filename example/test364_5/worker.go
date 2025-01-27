package main

import (
	"context"
	"iter"
	"slices"
	"sync"

	"github.com/rs/zerolog"
	"github.com/spikeekips/mitum/base"
	dutil "github.com/spikeekips/mitum/example/test364_5/util"
	"github.com/spikeekips/mitum/util"
)

// FIXME use go routine for broadcast to others

type (
	newWorkerDistributeStateMergeValuesFunc func(_ context.Context, workID, worker string) (
		workerProcessInsertStateMergeValuesFunc,
		dutil.In2ErrorFunc[[]string /* subs */, []string /* affected */],
		error,
	)
	workerInitialInsertFunc = func(
		getStateOwnersFunc,
		newWorkerDistributeStateMergeValuesFunc,
		getOperationFunc,
		iter.Seq2[uint64 /* opindex */, [2]util.Hash /* {op, fact} */],
		workerReportStatesOwnersFunc,
	) error
	workerBroadcastUnmerged = func(
		_ context.Context,
		local,
		workID,
		worker string,
		unmerged []uint64,
	) error
	workerReportStatesOwnersFunc = func(
		_ context.Context,
		local,
		workID string,
		owners []string,
	) error
	workerReportResultFunc func(
		_ context.Context,
		local,
		workID string,
		reasons map[uint64]string,
		statesSize uint64,
		_ dutil.InErrorFunc[dutil.InErrorFunc[base.State]],
	) error
	workerWaitFunc = func(
		workerBroadcastUnmerged,
		workerReportResultFunc,
	) error
	workerCloseFunc = dutil.InErrorFunc[error]
)

type workerOptions struct {
	Local                       string
	JobWorkerMaxSemSize         uint64
	MaxExtractStateKeyCacheSize int
}

func newWorkerOptions(local string) *workerOptions {
	return &workerOptions{
		Local:                       local,
		JobWorkerMaxSemSize:         1 << 9,
		MaxExtractStateKeyCacheSize: 1 << 13,
	}
}

func newFuncWorker(
	ctx context.Context,
	options *workerOptions,
	getState getStateFunc,
	createState createStateFunc,
	reportCancelWork dutil.InErrorFunc[error],
	workID string,
	workers, opsWorkers []string,
	opsShare, statesShare uint64,
	maxOpsSize uint64,
) (
	workerInitialInsertFunc,
	workerProcessWorkerInsertStateMergeValuesFunc,
	workerUnmergedFunc,
	workerWaitFunc,
	workerCloseFunc,
	error,
) {
	log := dutil.NewModuleLoggerFromContext(ctx, "worker", func(zctx zerolog.Context) zerolog.Context {
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
		return nil, nil, nil, nil, nil, err
	}

	wctx, wctxCancel := dutil.ContextWithCancelCause(ctx)

	cancelWork := newFuncWorkerCancelWork(wctxCancel, reportCancelWork)

	ifErrorCancelWork := dutil.NewIfErrErrorInFunc(func(err error) error {
		return cancelWork(err, true)
	})

	_, closeWorker, _, closeWorkerWait := dutil.NewContextDone(wctx)

	var initialInsert workerProcessInitialInsertFunc
	var workerInsert workerProcessWorkerInsertStateMergeValuesFunc
	var workerUnmerge workerUnmergedFunc
	var processWait workerProcessWaitFunc

	switch i, w, u, p, err := newFuncWorkerProcess(
		wctx,
		options.Local,
		dutil.NewFuncNewJobWorkerFunc(options.JobWorkerMaxSemSize),
		getState,
		createState,
		options.MaxExtractStateKeyCacheSize,
		workers, opsWorkers,
		opsShare, statesShare,
		maxOpsSize,
	); {
	case err != nil:
		return nil, nil, nil, nil, nil, err
	default:
		initialInsert, workerInsert, workerUnmerge, processWait = i, w, u, p
	}

	return func(
			getStateOwners getStateOwnersFunc,
			distribute newWorkerDistributeStateMergeValuesFunc,
			getOperation getOperationFunc,
			ophs iter.Seq2[uint64, [2]util.Hash],
			reportStatesOwners workerReportStatesOwnersFunc,
		) error {
			return ifErrorCancelWork(initialInsert(
				getStateOwners,
				func(ctx context.Context, worker string) (
					workerProcessInsertStateMergeValuesFunc,
					dutil.In2ErrorFunc[[]string, []string],
					error,
				) {
					return distribute(ctx, workID, worker)
				},
				getOperation,
				func(f opindexOpHashesFunc) error {
					for opindex, hs := range ophs {
						if err := f(opindex, hs[0], hs[1]); err != nil {
							return err
						}
					}

					return nil
				},
				func(owners []string, _ map[uint64]string) error {
					if reportStatesOwners != nil {
						if slices.Index(opsWorkers, options.Local) >= 0 {
							return reportStatesOwners(ctx, options.Local, workID, owners)
						}
					}

					return nil
				},
			))
		},
		workerInsert,
		workerUnmerge,
		func(
			broadcastUnmerged workerBroadcastUnmerged,
			reportResult workerReportResultFunc,
		) error {
			var traverseOps iter.Seq2[uint64, operation]
			var savedStates util.LockedMap[string, base.State]

			defer func() {
				if savedStates != nil {
					savedStates.Close()
				}
			}()

			switch i, err := processWait(
				func(ctx context.Context, worker string, unmerged []uint64) error {
					return broadcastUnmerged(ctx, options.Local, workID, worker, unmerged)
				},
				func(
					ctx context.Context,
					reasons map[uint64]string,
					statesSize uint64,
					reportStates dutil.InErrorFunc[dutil.InErrorFunc[base.State]],
				) error {
					if statesSize > 0 {
						switch i, err := util.NewLockedMap[string, base.State](statesSize, nil); {
						case err != nil:
							return err
						default:
							savedStates = i
						}
					}

					return reportResult(ctx, options.Local, workID, reasons, statesSize,
						func(f dutil.InErrorFunc[base.State]) error {
							return reportStates(func(st base.State) error {
								_ = savedStates.SetValue(st.Key(), st)

								return f(st)
							})
						})
				},
			); {
			case err != nil:
				return cancelWork(err, true)
			default:
				defer i(nil)

				traverseOps = i
			}

			// FIXME save

			_ = traverseOps // FIXME save ops
			_ = savedStates // FIXME save states

			switch err := closeWorkerWait(nil); {
			case err != nil:
				log.Error().Err(err).Msg("worker canceled")

				return cancelWork(err, true)
			default:
				log.Debug().Msg("worker closed")

				return nil
			}
		},
		func(err error) error {
			if err != nil {
				return cancelWork(err, false)
			}

			return closeWorker(nil)
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
	case len(workers) < len(opsWorkers):
		return util.ErrInvalid.Errorf("invalid workers size; < ops workers")
	}

	for worker := range slices.Values(opsWorkers) {
		if slices.Index(workers, worker) < 0 {
			return util.ErrInvalid.Errorf("unknown ops worker, %q", worker)
		}
	}

	return nil
}

func newFuncWorkerCancelWork(
	cancel dutil.InErrorFunc[error],
	reportCancelWork dutil.InErrorFunc[error],
) dutil.In2ErrorFunc[error, bool] {
	var once sync.Once

	return func(err error, isInternal bool) error {
		once.Do(func() {
			_ = cancel(err)

			if isInternal && reportCancelWork != nil { // NOTE report error to manager
				_ = reportCancelWork(err)
			}
		})

		return err
	}
}
