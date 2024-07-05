package main

import (
	"context"

	"github.com/rs/zerolog"
	"github.com/spikeekips/mitum/util"
	"github.com/spikeekips/mitum/util/logging"
)

type (
	stateMergeValueExtractorInsertFunc  func(opindex uint64, oph util.Hash) error
	stateMergeValueExtractorReasonsFunc func(reasons map[uint64]string) error
	stateMergeValueExtractorWaitFunc    func(stateMergeValueExtractorReasonsFunc) error
)

type stateMergeValueExtractorArgs struct {
	*logging.Logging
	NewJobWorker func(context.Context) (JobWorker, error)
	GetOperation func(_ context.Context, opindex uint64, oph util.Hash) (Operation, bool, error)
	Done         func(_ context.Context, opindex uint64, _ Operation, _ []StateMergeValue) error
}

func newStateMergeValueExtractorArgs() *stateMergeValueExtractorArgs {
	return &stateMergeValueExtractorArgs{
		Logging: logging.NewLogging(func(zctx zerolog.Context) zerolog.Context {
			return zctx.Str("module", "state-merge-value-extractor")
		}),
		NewJobWorker: func(ctx context.Context) (JobWorker, error) {
			return NewBaseJobWorker(ctx, 1<<6)
		},
		GetOperation: func(context.Context, uint64, util.Hash) (Operation, bool, error) {
			// FIXME check fact hash is correct
			return nil, false, util.ErrNotImplemented.Errorf("GetOperation")
		},
		Done: func(context.Context, uint64, Operation, []StateMergeValue) error {
			return util.ErrNotImplemented.Errorf("Done")
		},
	}
}

func newStateMergeValueExtractor(ctx context.Context, args *stateMergeValueExtractorArgs) (
	insert stateMergeValueExtractorInsertFunc,
	insertDone func(),
	wait stateMergeValueExtractorWaitFunc,
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

	extract := newExtractStateMergeValueFunc(args.GetOperation)

	return func(opindex uint64, oph util.Hash) error {
			l := args.Log().With().Uint64("opindex", opindex).Stringer("op", oph).Logger()

			l.Debug().Msg("> insert")

			return jobWorker.NewJob(func(ctx context.Context) error {
				l.Debug().Msg("> extract")
				defer l.Debug().Msg("< extracted")

				switch op, values, reason, err := extract(ctx, opindex, oph); {
				case err != nil:
					l.Error().Err(err).Msg("failed to extract")

					return err
				case len(reason) > 0:
					l.Debug().Str("reason", reason).Msg("extracted, but reason")

					_ = reasons.SetValue(opindex, reason)

					return nil
				default:
					return args.Done(ctx, opindex, op, values)
				}

				return nil
			})
		},
		jobWorker.JobDone,
		func(f stateMergeValueExtractorReasonsFunc) error {
			defer func() {
				pctxCancel(nil)
				reasons.Close()
			}()

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

func newExtractStateMergeValueFunc(
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

		switch values, reason, err := op.StateMergeValues(ctx); {
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
