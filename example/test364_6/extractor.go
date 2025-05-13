package dop6

import (
	"context"

	dutil "github.com/spikeekips/mitum/example/test364_5/util"
	"github.com/spikeekips/mitum/util"
)

type (
	getOperationFactFunc = func(_ context.Context, opindex uint64, oph, facth util.Hash) (
		_ OperationFact, found bool, reason string, _ error)
	opindexOpHashesFunc         = func(opindex uint64, oph, facth util.Hash) error
	smvExtractorExtractFunc     = func(_ context.Context, opindex uint64, oph, facth util.Hash) error
	smvExtractorExtractSMVsFunc = func(_ context.Context, opindex uint64, oph, facth util.Hash) (SMV, []SMV, string, error)
	smvExtractorExtractedFunc   = func(opindex uint64, subv SMV, addvs []SMV) error
	smvExtractorInsertFunc      = func(
		dutil.InErrorFunc[opindexOpHashesFunc],
		smvExtractorExtractedFunc,
	) error
)

var (
	reasonOperationNotFound = "operation not found"
	reasonEmptySMVs         = "empty state merge values"
)

func newSMVExtractor(
	ctx context.Context,
	maxOpsSize uint64,
	getOperationFact getOperationFactFunc,
	reportUnmerged mergerUnmergedFunc,
) (
	smvExtractorInsertFunc,
	dutil.ErrorFunc,
	error,
) {
	var newJob dutil.JobWorkerNewJobFunc
	var jobDone func()
	var jobWait dutil.InErrorFunc[error]

	switch n, d, w, err := dutil.NewFuncJobWorker(ctx, dutil.SafeUint64ToInt64(maxOpsSize)); {
	case err != nil:
		return nil, nil, err
	default:
		newJob, jobDone, jobWait = n, d, w
	}

	return func(
			insert dutil.InErrorFunc[opindexOpHashesFunc],
			extracted smvExtractorExtractedFunc,
		) error {
			defer jobDone()

			extract := newFuncSMVExtractorExtract(getOperationFact, extracted, reportUnmerged)

			if err := insert(func(opindex uint64, oph, facth util.Hash) error {
				return newJob(func(ctx context.Context) error {
					return extract(ctx, opindex, oph, facth)
				})
			}); err != nil {
				return jobWait(err)
			}

			return nil
		},
		func() error {
			return jobWait(nil)
		},
		nil
}

func newFuncSMVExtractorExtractSMVs(getOperationFact getOperationFactFunc) smvExtractorExtractSMVsFunc {
	return func(ctx context.Context, opindex uint64, oph, facth util.Hash) (
		SMV,
		[]SMV,
		string,
		error,
	) {
		var fact OperationFact

		switch i, found, reason, err := getOperationFact(ctx, opindex, oph, facth); {
		case err != nil:
			return nil, nil, "", err
		case !found:
			return nil, nil, reasonOperationNotFound, nil
		case reason != "":
			return nil, nil, reason, nil
		default:
			fact = i
		}

		switch subv, addvs, reason, err := fact.SMVs(ctx); {
		case err != nil:
			return nil, nil, "", err
		case reason != "":
			return nil, nil, reason, nil
		case subv == nil && len(addvs) < 1:
			return nil, nil, reasonEmptySMVs, nil
		default:
			return subv, addvs, "", nil
		}
	}
}

func newFuncSMVExtractorExtract(
	getOperationFact getOperationFactFunc,
	extracted smvExtractorExtractedFunc,
	reportUnmerged mergerUnmergedFunc,
) smvExtractorExtractFunc {
	extractf := newFuncSMVExtractorExtractSMVs(getOperationFact)

	return func(ctx context.Context, opindex uint64, oph, facth util.Hash) error {
		switch subv, addvs, reason, err := extractf(ctx, opindex, oph, facth); {
		case err != nil:
			return err
		case reason != "":
			return reportUnmerged(opindex, reason)
		default:
			return extracted(opindex, subv, addvs)
		}
	}
}
