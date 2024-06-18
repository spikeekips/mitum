package main

import (
	"context"

	"github.com/rs/zerolog"
	"github.com/spikeekips/mitum/util"
	"github.com/spikeekips/mitum/util/logging"
)

type (
	stateValuesProcessorInsertFunc func(opindex uint64, oph util.Hash) error
	reasonsFunc                    func(reasons map[uint64]string) error
	waitWithReasonsFunc            func(reasonsFunc) error
)

type stateValuesProcessorArgs struct {
	*logging.Logging
	NewJobWorker func(context.Context) (JobWorker, error)
	GetOperation func(_ context.Context, opindex uint64, oph util.Hash) (Operation, bool, error)
	ProcessedOp  func(_ context.Context, opindex uint64, _ Operation, _ []StateMergeValue) error
}

func newStateValuesProcessorArgs() *stateValuesProcessorArgs {
	return &stateValuesProcessorArgs{
		Logging: logging.NewLogging(func(zctx zerolog.Context) zerolog.Context {
			return zctx.Str("module", "state-values-processor")
		}),
		NewJobWorker: func(ctx context.Context) (JobWorker, error) {
			return NewBaseJobWorker(ctx, 1<<6)
		},
		GetOperation: func(context.Context, uint64, util.Hash) (Operation, bool, error) {
			return nil, false, util.ErrNotImplemented.Errorf("GetOperation")
		},
		ProcessedOp: func(context.Context, uint64, Operation, []StateMergeValue) error {
			return util.ErrNotImplemented.Errorf("NewProcessed")
		},
	}
}

func newStateValuesProcessor(ctx context.Context, args *stateValuesProcessorArgs) (
	insert stateValuesProcessorInsertFunc,
	insertDone func(),
	wait waitWithReasonsFunc,
	_ error,
) {
	pctx, pctxCancel := context.WithCancelCause(ctx)

	var jobWorker JobWorker

	switch i, err := args.NewJobWorker(pctx); {
	case err != nil:
		return nil, nil, nil, err
	default:
		jobWorker = i
	}

	reasons := util.NewSingleLockedMap[uint64, string]()

	process := processOperationFunc(args.GetOperation)

	return func(opindex uint64, oph util.Hash) error {
			l := args.Log().With().Uint64("opindex", opindex).Stringer("op", oph).Logger()

			l.Debug().Msg("> insert")

			return jobWorker.NewJob(func(ctx context.Context) error {
				l.Debug().Msg("> process")
				defer l.Debug().Msg("< processed")

				switch op, values, reason, err := process(ctx, opindex, oph); {
				case err != nil:
					l.Error().Err(err).Msg("failed to process")

					return err
				case len(reason) > 0:
					l.Debug().Str("reason", reason).Msg("processed, but reason")

					_ = reasons.SetValue(opindex, reason)

					return nil
				default:
					return args.ProcessedOp(ctx, opindex, op, values)
				}

				return nil
			})
		},
		jobWorker.JobDone,
		func(f reasonsFunc) error {
			defer pctxCancel(nil)
			defer reasons.Close()

			if err := jobWorker.Wait(); err != nil {
				return err
			}

			if f != nil && reasons.Len() > 0 {
				return f(reasons.Map())
			}

			return nil
		},
		nil
}

func processOperationFunc(
	getOperation func(_ context.Context, opindex uint64, oph util.Hash) (Operation, bool, error),
) func(context.Context, uint64, util.Hash) (
	op Operation,
	_ []StateMergeValue,
	reason string,
	_ error,
) {
	return func(
		ctx context.Context,
		opindex uint64,
		oph util.Hash,
	) (
		Operation,
		[]StateMergeValue,
		string,
		error,
	) {
		var op Operation

		switch i, found, err := getOperation(ctx, opindex, oph); {
		case err != nil:
			return nil, nil, "", err
		case !found:
			return nil, nil, "operation not found", nil
		default:
			op = i
		}

		switch values, reason, err := op.ProcessNew(ctx); {
		case err != nil:
			return nil, nil, "", err
		case reason != nil:
			return nil, nil, reason.OperationProcessFailedReason(), nil
		case len(values) < 1:
			return nil, nil, "empty state merge values", nil
		default:
			return op, values, "", nil
		}
	}
}

func newStateValuesProcessWorker(
	ctx context.Context,
	newfs newStateValuesProcessorFuncs,
	stateOwners stateOwnersFunc,
	insertLocalFuncs insertWorkerOpFuncs,
	broadcast distributeStateMergeValuesFunc,
	saveOperation saveOperationFunc,
) (
	initials processWorkerInitialOpFuncs,
	_ waitWithReasonsFunc,
	_ error,
) {
	var args *stateValuesProcessorArgs
	var newf newStateValuesProcessorFunc

	switch a, n, err := newfs(); {
	case err != nil:
		return nil, nil, err
	default:
		args = a
		newf = n
	}

	insertLocal, insertLocalDone := insertLocalFuncs(localWorkerName)

	var distributeInsert insertOpFunc
	var distributeDone func()
	var distributeWait func() error

	switch i, d, w, err := distributeStateValuesFuncs(
		ctx,
		stateOwners,
		args.NewJobWorker,
		func(ctx context.Context, worker string, opindex uint64, oph util.Hash, values []StateMergeValue) error {
			if err := insertLocal(opindex, oph, values); err != nil {
				return err
			}

			return broadcast(ctx, worker, opindex, oph, values)
		},
	); {
	case err != nil:
		return nil, nil, err
	default:
		distributeInsert = i
		distributeDone = d
		distributeWait = w
	}

	var wait waitWithReasonsFunc

	switch i, d, w, err := newf(ctx, args); {
	case err != nil:
		return nil, nil, err
	default:
		initials = func() (stateValuesProcessorInsertFunc, func()) {
			return i, d
		}
		wait = w
	}

	args.ProcessedOp = func(ctx context.Context, opindex uint64, op Operation, values []StateMergeValue) error {
		if err := saveOperation(opindex, op); err != nil {
			return err
		}

		return distributeInsert(opindex, op.Hash(), values)
	}

	return initials,
		func(f reasonsFunc) error {
			if err := FuncErrorLog(args.Log(), "process state values")(wait(f)); err != nil {
				return err
			}

			distributeDone()

			if err := FuncErrorLog(args.Log(), "distribute values wait")(distributeWait()); err != nil {
				return err
			}

			insertLocalDone()

			return nil
		},
		nil
}

func distributeStateValuesFuncs(
	ctx context.Context,
	stateOwners stateOwnersFunc,
	newJobWorker func(context.Context) (JobWorker, error),
	broadcast func(_ context.Context, worker string, opindex uint64, oph util.Hash, _ []StateMergeValue) error,
) (
	insert insertOpFunc,
	done func(),
	wait func() error,
	_ error,
) {
	var jobWorker JobWorker

	switch i, err := newJobWorker(ctx); {
	case err != nil:
		return nil, nil, nil, err
	default:
		jobWorker = i
	}

	distribute := stateValuesByOwnerFunc(stateOwners)

	return func(opindex uint64, oph util.Hash, values []StateMergeValue) error {
			return jobWorker.NewJob(func(ctx context.Context) error {
				return distribute(ctx, values, func(worker string, filtered []StateMergeValue) error {
					return broadcast(ctx, worker, opindex, oph, filtered)
				})
			})
		},
		jobWorker.JobDone,
		jobWorker.Wait,
		nil
}

func stateValuesByOwnerFunc(
	stateOwners stateOwnersFunc,
) func(context.Context, []StateMergeValue, func(worker string, _ []StateMergeValue) error) error {
	return func(ctx context.Context, values []StateMergeValue, f func(worker string, _ []StateMergeValue) error) error {
		m := map[string][]StateMergeValue{}
		defer clear(m)

		_ = util.TraverseSliceV1(values, func(_ int, value StateMergeValue) (bool, error) {
			switch owners, err := stateOwners(ctx, value.Key()); {
			case err != nil:
				return false, err
			default:
				_ = util.TraverseSliceV1(owners, func(_ int, owner string) (bool, error) {
					m[owner] = append(m[owner], value)

					return true, nil
				})

				return true, nil
			}
		})

		return TraverseMap(m, func(owner string, values []StateMergeValue) (bool, error) {
			return true, f(owner, values)
		})
	}
}
