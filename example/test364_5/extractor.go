package main

import (
	"context"

	"github.com/spikeekips/mitum/util"
)

type (
	opindexHashFunc    = func(opindex uint64, oph, fact util.Hash) error
	ctxOpindexHashFunc = func(_ context.Context, opindex uint64, oph, fact util.Hash) error
	getOperationFunc   = func(
		_ context.Context,
		opindex uint64,
		oph, fact util.Hash,
	) (Operation, bool, error)
	extracttateMergeValuesFunc = func(context.Context, uint64, util.Hash, util.Hash) (
		_ Operation,
		_ StateMergeValue,
		_ []StateMergeValue,
		reason string,
		_ error,
	)
	stateMergeValueExtractorInsertFunc    = in3ErrorFunc[getOperationFunc, inErrorFunc[opindexHashFunc], stateMergeValueExtractorExtractedFunc]
	stateMergeValueExtractorExtractedFunc = func(
		opindex uint64,
		_ Operation,
		_ StateMergeValue,
		_ []StateMergeValue,
	) error
	stateMergeValueExtractorWaitCallback = func(reasons map[uint64]string) error
)

var (
	reasonOperationNotFound     = "operation not found"
	reasonEmptyStateMergeValues = "empty state merge values"
)

func newFuncStateMergeValueExtractor(
	ctx context.Context,
	newJobWorker newJobWorkerFunc,
	maxOpsSize uint64,
) (
	stateMergeValueExtractorInsertFunc,
	inErrorFunc[stateMergeValueExtractorWaitCallback],
	error,
) {
	var newJob jobWorkerNewJobFunc
	var jobDone func()
	var jobWait errorFunc

	switch n, d, w, err := newJobWorker(ctx); {
	case err != nil:
		return nil, nil, err
	default:
		newJob, jobDone, jobWait = n, d, w
	}

	var reasons util.LockedMap[uint64, string]

	switch i, err := util.NewLockedMap[uint64, string](maxOpsSize, nil); {
	case err != nil:
		return nil, nil, err
	default:
		reasons = i
	}

	return func(
			getOperation getOperationFunc,
			f inErrorFunc[opindexHashFunc],
			extracted stateMergeValueExtractorExtractedFunc,
		) error {
			defer jobDone()

			extractf := newFuncExtractorStateMergeValues(getOperation, extracted, func(opindex uint64, reason string) {
				_ = reasons.SetValue(opindex, reason)
			})

			return f(func(opindex uint64, oph, fact util.Hash) error {
				return newJob(func(ctx context.Context) error {
					return extractf(ctx, opindex, oph, fact)
				})
			})
		},
		func(f stateMergeValueExtractorWaitCallback) error {
			defer func() {
				_ = jobWait()
				reasons.Close()
			}()

			return contextCauseError(ctx, dummyErrorInFunc, func(context.Context) error {
				if err := jobWait(); err != nil {
					return err
				}

				if f != nil {
					return f(reasons.Map())
				}

				return nil
			})
		},
		nil
}

func newFuncExtractStateMergeValue(
	getOperation getOperationFunc,
) extracttateMergeValuesFunc {
	return func(ctx context.Context, opindex uint64, oph, fact util.Hash) (
		Operation,
		StateMergeValue,
		[]StateMergeValue,
		string,
		error,
	) {
		var op Operation

		switch i, found, err := getOperation(ctx, opindex, oph, fact); {
		case err != nil:
			return nil, nil, nil, "", err
		case !found:
			return nil, nil, nil, reasonOperationNotFound, nil
		default:
			op = i
		}

		switch subv, addvs, reason, err := op.StateMergeValues(ctx); {
		case err != nil:
			return nil, nil, nil, "", err
		case reason != "":
			return nil, nil, nil, reason, nil
		case subv == nil && len(addvs) < 1:
			return nil, nil, nil, reasonEmptyStateMergeValues, nil
		default:
			return op, subv, addvs, "", nil
		}
	}
}

func newFuncExtractorStateMergeValues(
	getOperation getOperationFunc,
	extracted stateMergeValueExtractorExtractedFunc,
	setReason func(opindex uint64, reason string),
) ctxOpindexHashFunc {
	extractf := newFuncExtractStateMergeValue(getOperation)

	return func(ctx context.Context, opindex uint64, oph, fact util.Hash) error {
		switch op, subv, addvs, reason, err := extractf(ctx, opindex, oph, fact); {
		case err != nil:
			return err
		case reason != "":
			setReason(opindex, reason)

			return nil
		default:
			return extracted(opindex, op, subv, addvs)
		}
	}
}
