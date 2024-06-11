package main

import (
	"context"

	"github.com/rs/zerolog"
	"github.com/spikeekips/mitum/util"
	"github.com/spikeekips/mitum/util/logging"
)

type stateValuesProcessorArgs struct {
	*logging.Logging
	NewJobWorker func(context.Context) (JobWorker, error)
	GetOperation func(_ context.Context, opindex uint64) (Operation, bool, error)
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
		GetOperation: func(context.Context, uint64) (Operation, bool, error) {
			return nil, false, util.ErrNotImplemented.Errorf("GetOperation")
		},
		ProcessedOp: func(context.Context, uint64, Operation, []StateMergeValue) error {
			return util.ErrNotImplemented.Errorf("NewProcessed")
		},
	}
}

func newStateValuesProcessor(ctx context.Context, args *stateValuesProcessorArgs) (
	insert func(opindex uint64) error,
	insertDone func(),
	wait func(func(reasons map[uint64]string) error) error,
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

	return func(opindex uint64) error {
			l := args.Log().With().Uint64("opindex", opindex).Logger()

			l.Debug().Msg("> insert")

			return jobWorker.NewJob(func(ctx context.Context) error {
				l.Debug().Msg("> process")
				defer l.Debug().Msg("< processed")

				switch op, values, reason, err := process(ctx, opindex); {
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
		func(f func(map[uint64]string) error) error {
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
	getOperation func(_ context.Context, opindex uint64) (Operation, bool, error),
) func(context.Context, uint64) (
	op Operation,
	_ []StateMergeValue,
	reason string,
	_ error,
) {
	return func(
		ctx context.Context,
		opindex uint64,
	) (
		Operation,
		[]StateMergeValue,
		string,
		error,
	) {
		var op Operation

		switch i, found, err := getOperation(ctx, opindex); {
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
