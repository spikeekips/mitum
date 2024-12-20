package main

import (
	"context"
	"slices"

	"github.com/rs/zerolog"
	"github.com/spikeekips/mitum/base"
	"github.com/spikeekips/mitum/util"
)

type (
	workerDistributeStateMergeValuesFunc = func(_ context.Context, workID string, workersSize uint64) (
		workerDistributeWorkerStateMergeValuesFunc,
		workerStateOwnersSetFunc,
		errorFunc,
		error,
	)
	workerInitialInsertFunc = func(
		getStateOwnersFunc,
		workerDistributeStateMergeValuesFunc,
		getOperationFunc,
	) error
	workerBroacastUnmerged = func(
		_ context.Context,
		local,
		workID,
		worker string,
		unmerged []uint64,
	) error
	workerReportStateOwnersFunc = func(
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
		_ inErrorFunc[inErrorFunc[base.State]],
	) error
	workerWaitFunc = func(
		workerBroacastUnmerged,
		workerReportStateOwnersFunc,
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
	getState getStateFunc,
	newState newStateFunc,
	workID string,
	workers, opsWorkers []string,
	opsShare, statesShare uint64,
	ophs [][2]util.Hash,
) (
	workerInitialInsertFunc,
	workerProcessWorkerInsertStateMergeValuesFunc,
	workerUnmergedFunc,
	workerWaitFunc,
	error,
) {
	log := newModuleLoggerFromContext(ctx, "worker", func(zctx zerolog.Context) zerolog.Context {
		return zctx.
			Str("work_id", workID).
			Str("local", options.Local)
	})

	opsSize := uint64(len(ophs))

	log.Debug().
		Interface("options", options).
		Uint64("ops_size", opsSize).
		Dict("workers", zerolog.Dict().
			Strs("all", workers).
			Strs("ops", opsWorkers),
		).
		Dict("share", zerolog.Dict().
			Uint64("ops", opsShare).
			Uint64("states", statesShare),
		).
		Func(func(e *zerolog.Event) {
			if log.GetLevel() == zerolog.TraceLevel {
				e.Interface("ops", ophs)
			}
		}).
		Msg("newFuncWorker")

	if err := isValidWorkerArgs(
		options.Local,
		workers, opsWorkers,
		opsShare, statesShare,
	); err != nil {
		return nil, nil, nil, nil, err
	}

	var initialInsert workerProcessInitialInsertFunc
	var workerInsert workerProcessWorkerInsertStateMergeValuesFunc
	var workerUnmerge workerUnmergedFunc
	var processWait workerProcessWaitFunc

	switch i, w, u, p, err := newFuncWorkerProcess(
		ctx,
		options.Local,
		newFuncNewJobWorkerFunc(options.JobWorkerSemSize),
		getState,
		newState,
		options.MaxExtractStateKeyCacheSize,
		workers, opsWorkers,
		opsShare, statesShare,
		opsSize,
	); {
	case err != nil:
		return nil, nil, nil, nil, err
	default:
		initialInsert, workerInsert, workerUnmerge, processWait = i, w, u, p
	}

	return func(
			getStateOwners getStateOwnersFunc,
			distribute workerDistributeStateMergeValuesFunc,
			getOperation getOperationFunc,
		) error {
			return initialInsert(
				getStateOwners,
				func(ctx context.Context, workersSize uint64) (
					workerDistributeWorkerStateMergeValuesFunc,
					workerStateOwnersSetFunc,
					errorFunc,
					error,
				) {
					return distribute(ctx, workID, workersSize)
				},
				getOperation,
				func(f opindexOpHashesFunc) error {
					return traverseSlice(ophs, func(i int, oph [2]util.Hash) (bool, error) {
						return true, f(uint64(i), oph[0], oph[1])
					})
				},
			)
		},
		workerInsert,
		workerUnmerge,
		func(
			broacastUnmerged workerBroacastUnmerged,
			reportStateOwners workerReportStateOwnersFunc,
			reportResult workerReportResultFunc,
		) error {
			var traverseOps workerProcessTraverseOperationsFunc
			var savedStates util.LockedMap[string, base.State]

			defer func() {
				if savedStates != nil {
					savedStates.Close()
				}
			}()

			switch i, err := processWait(
				func(ctx context.Context, worker string, unmerged []uint64) error {
					return broacastUnmerged(ctx, options.Local, workID, worker, unmerged)
				},
				func(ctx context.Context, owners []string) error {
					return reportStateOwners(ctx, options.Local, workID, owners)
				},
				func(
					ctx context.Context,
					reasons map[uint64]string,
					statesSize uint64,
					reportStates inErrorFunc[inErrorFunc[base.State]],
				) error {
					switch i, err := util.NewLockedMap[string, base.State](statesSize, nil); {
					case err != nil:
						return err
					default:
						savedStates = i
					}

					return reportResult(ctx, options.Local, workID, reasons, statesSize,
						func(f inErrorFunc[base.State]) error {
							return reportStates(func(st base.State) error {
								_ = savedStates.SetValue(st.Key(), st)

								return f(st)
							})
						})
				},
			); {
			case err != nil:
				return err
			default:
				defer i(nil)

				traverseOps = i
			}

			// FIXME save

			_ = traverseOps // FIXME save ops
			_ = savedStates // FIXME save states

			return nil
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
