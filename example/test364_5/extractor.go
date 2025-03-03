package dop5

import (
	"context"

	dutil "github.com/spikeekips/mitum/example/test364_5/util"
	"github.com/spikeekips/mitum/util"
)

type (
	OpindexOpHashesFunc    = func(opindex uint64, oph, facth util.Hash) error
	ctxOpindexOpHashesFunc = func(_ context.Context, opindex uint64, oph, facth util.Hash) error
	getOperationFactFunc   = func(_ context.Context, opindex uint64, oph, facth util.Hash) (_ OperationFact, found bool, reason string, _ error)
	extractSMVsFunc        = func(context.Context, uint64, util.Hash, util.Hash) (
		_ OperationFact,
		subv SMV,
		addvs []SMV,
		reason string,
		_ error,
	)
	smvExtractorInsertFunc = dutil.In3ErrorFunc[
		getOperationFactFunc, dutil.InErrorFunc[OpindexOpHashesFunc], smvExtractorExtractedFunc]
	smvExtractorExtractedFunc = func(
		opindex uint64,
		_ OperationFact,
		subv SMV,
		addvs []SMV,
	) error
)

var (
	reasonOperationNotFound = "operation not found"
	reasonEmptySMVs         = "empty state merge values"
)

func newFuncSMVExtractor(
	ctx context.Context,
	newJobWorker dutil.NewJobWorkerFunc,
	maxOpsSize uint64,
) (
	smvExtractorInsertFunc,
	dutil.InErrorFunc[dutil.InErrorFunc[map[uint64]string]],
	error,
) {
	var newJob dutil.JobWorkerNewJobFunc
	var jobDone func()
	var jobWait dutil.InErrorFunc[error]

	switch n, d, w, err := newJobWorker(ctx, maxOpsSize); {
	case err != nil:
		return nil, nil, err
	default:
		newJob, jobDone, jobWait = n, d, w
	}

	var reasons dutil.LockedMap[uint64, string]

	switch i, err := dutil.NewLockedMap[uint64, string](nil, maxOpsSize, maxOpsSize); {
	case err != nil:
		return nil, nil, err
	default:
		reasons = i
	}

	return func(
			getOperationFact getOperationFactFunc,
			f dutil.InErrorFunc[OpindexOpHashesFunc],
			extracted smvExtractorExtractedFunc,
		) error {
			defer jobDone()

			extractf := newFuncExtractorSMVs(getOperationFact, extracted, func(opindex uint64, reason string) {
				_ = reasons.SetValue(opindex, reason)
			})

			return dutil.NewIfErrErrorInFunc(jobWait)(f(func(opindex uint64, oph, facth util.Hash) error {
				return newJob(func(ctx context.Context) error {
					return extractf(ctx, opindex, oph, facth)
				})
			}))
		},
		func(f dutil.InErrorFunc[map[uint64]string]) error {
			defer reasons.Close()

			if err := jobWait(nil); err != nil {
				return err
			}

			if f != nil {
				return f(reasons.Map())
			}

			return nil
		},
		nil
}

func newFuncExtractSMV(
	getOperationFact getOperationFactFunc,
) extractSMVsFunc {
	return func(ctx context.Context, opindex uint64, oph, facth util.Hash) (
		OperationFact,
		SMV,
		[]SMV,
		string,
		error,
	) {
		var fact OperationFact

		switch i, found, reason, err := getOperationFact(ctx, opindex, oph, facth); {
		case err != nil:
			return nil, nil, nil, "", err
		case !found:
			return nil, nil, nil, reasonOperationNotFound, nil
		case reason != "":
			return nil, nil, nil, reason, nil
		default:
			fact = i
		}

		switch subv, addvs, reason, err := fact.SMVs(ctx); {
		case err != nil:
			return nil, nil, nil, "", err
		case reason != "":
			return nil, nil, nil, reason, nil
		case subv == nil && len(addvs) < 1:
			return nil, nil, nil, reasonEmptySMVs, nil
		default:
			return fact, subv, addvs, "", nil
		}
	}
}

func newFuncExtractorSMVs(
	getOperationFact getOperationFactFunc,
	extracted smvExtractorExtractedFunc,
	setReason func(opindex uint64, reason string),
) ctxOpindexOpHashesFunc {
	extractf := newFuncExtractSMV(getOperationFact)

	return func(ctx context.Context, opindex uint64, oph, facth util.Hash) error {
		switch fact, subv, addvs, reason, err := extractf(ctx, opindex, oph, facth); {
		case err != nil:
			return err
		case reason != "":
			setReason(opindex, reason)

			return nil
		default:
			return extracted(opindex, fact, subv, addvs)
		}
	}
}
