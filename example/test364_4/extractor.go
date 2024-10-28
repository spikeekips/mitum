package main

import (
	"context"

	"github.com/spikeekips/mitum/util"
)

type (
	stateMergeValueExtractorGetOperationFunc func(
		_ context.Context,
		opindex uint64,
		oph, fact util.Hash,
	) (Operation, bool, error)
	stateMergeValueExtractorInsertFunc      func(opindex uint64, oph util.Hash) error
	stateMergeValueExtractorReasonsFunc     func(reasons map[uint64]string) error
	stateMergeValueExtractorExtractDoneFunc func(
		_ context.Context,
		opindex uint64,
		_ Operation,
		_ []StateMergeValue,
	) error
	stateMergeValueExtractorWaitFunc func(stateMergeValueExtractorReasonsFunc) error
	newStateMergeValueExtractorFunc  func(
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

func newFuncStateMergeValueExtractor(
	newJobWorker newJobWorkerFunc,
) newStateMergeValueExtractorFunc {
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
		pctx, pctxCancel := contextWithCancelCause(ctx)

		var jobWorker JobWorker

		switch i, err := newJobWorker(pctx); {
		case err != nil:
			return nil, nil, nil, err
		default:
			jobWorker = i
		}

		reasons := util.NewSingleLockedMap[uint64, string]()

		extractf := newFuncExtractStateMergeValue(getOperation)
		extract := func(ctx context.Context, opindex uint64, oph util.Hash) error {
			switch op, values, reason, err := extractf(ctx, opindex, oph); {
			case err != nil:
				return err
			case reason != "":
				_ = reasons.SetValue(opindex, reason)

				return nil
			default:
				return done(ctx, opindex, op, values)
			}
		}

		closef := func(err error) error {
			pctxCancel(err)
			reasons.Close()

			return err
		}

		return func(opindex uint64, oph util.Hash) error {
				return jobWorker.NewJob(func(ctx context.Context) error {
					return ifErrErrorFunc(closef)(extract(ctx, opindex, oph))
				})
			},
			jobWorker.JobDone,
			func(f stateMergeValueExtractorReasonsFunc) error {
				return contextCauseError(pctx, closef, func(context.Context) error {
					if err := jobWorker.Wait(); err != nil {
						return err
					}

					if f != nil && reasons.Len() > 0 {
						return f(reasons.Map())
					}

					return nil
				})
			},
			nil
	}
}

func newFuncExtractStateMergeValue(
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
