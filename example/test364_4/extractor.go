package main

import (
	"context"

	"github.com/rs/zerolog"
	"github.com/spikeekips/mitum/util"
	"github.com/spikeekips/mitum/util/logging"
)

type (
	stateMergeValueExtractorGetOperationFunc func(_ context.Context, opindex uint64, oph, fact util.Hash) (Operation, bool, error)
	stateMergeValueExtractorInsertFunc       func(opindex uint64, oph util.Hash) error
	stateMergeValueExtractorReasonsFunc      func(reasons map[uint64]string) error
	stateMergeValueExtractorExtractDoneFunc  func(_ context.Context, opindex uint64, _ Operation, _ []StateMergeValue) error
	stateMergeValueExtractorWaitFunc         func(stateMergeValueExtractorReasonsFunc) error
	newStateMergeValueExtractorFunc          func(
		context.Context,
		stateMergeValueExtractorGetOperationFunc,
		stateMergeValueExtractorExtractDoneFunc,
	) (
		_ stateMergeValueExtractorInsertFunc,
		extractDone func(),
		wait stateMergeValueExtractorWaitFunc,
		_ error,
	)
)

type stateMergeValueExtractorArgs struct {
	*logging.Logging
	NewJobWorker newJobWorkerFunc
	local        string
}

func newStateMergeValueExtractorArgs(local string) *stateMergeValueExtractorArgs {
	return &stateMergeValueExtractorArgs{
		Logging: logging.NewLogging(func(zctx zerolog.Context) zerolog.Context {
			return zctx.Str("module", "state-merge-value-extractor").Str("local", local)
		}),
		NewJobWorker: func(ctx context.Context) (JobWorker, error) {
			return NewBaseJobWorker(ctx, 1<<6)
		},
		local: local,
	}
}

func newFuncStateMergeValueExtractor(args *stateMergeValueExtractorArgs) newStateMergeValueExtractorFunc {
	return func(
		ctx context.Context,
		getOperation stateMergeValueExtractorGetOperationFunc,
		done stateMergeValueExtractorExtractDoneFunc,
	) (
		_ stateMergeValueExtractorInsertFunc,
		extractDone func(),
		wait stateMergeValueExtractorWaitFunc,
		_ error,
	) {
		pctx, pctxCancel := ContextWithCancelCause(ctx)

		var jobWorker JobWorker

		switch i, err := args.NewJobWorker(pctx); {
		case err != nil:
			return nil, nil, nil, err
		default:
			jobWorker = i
		}

		reasons := util.NewSingleLockedMap[uint64, string]()

		extractf := newExtractStateMergeValueFunc(getOperation)
		extract := func(ctx context.Context, opindex uint64, oph util.Hash) error {
			l := args.Log().With().Uint64("opindex", opindex).Stringer("op", oph).Logger()

			l.Debug().Msg("> extract")
			defer l.Debug().Msg("< extracted")

			switch op, values, reason, err := extractf(ctx, opindex, oph); {
			case err != nil:
				l.Error().Err(err).Msg("failed to extract")

				return err
			case len(reason) > 0:
				l.Debug().Str("reason", reason).Msg("extracted, but reason")

				_ = reasons.SetValue(opindex, reason)

				return nil
			default:
				return done(ctx, opindex, op, values)
			}

			return nil
		}

		return func(opindex uint64, oph util.Hash) error {
				return jobWorker.NewJob(func(ctx context.Context) error {
					if err := extract(ctx, opindex, oph); err != nil {
						return pctxCancel(err)
					}

					return nil
				})
			},
			jobWorker.JobDone,
			func(f stateMergeValueExtractorReasonsFunc) error {
				return pctxCancel(func() error {
					defer reasons.Close()

					if err := jobWorker.Wait(); err != nil {
						return err
					}

					if f != nil && reasons.Len() > 0 {
						return f(reasons.Map())
					}

					return nil
				}())
			},
			nil
	}
}

func newExtractStateMergeValueFunc(
	getOperation stateMergeValueExtractorGetOperationFunc,
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

		switch i, found, err := getOperation(ctx, opindex, oph, nil); {
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
