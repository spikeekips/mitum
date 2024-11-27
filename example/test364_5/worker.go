package main

import (
	"context"
	"slices"

	"github.com/rs/zerolog"
	"github.com/spikeekips/mitum/util"
)

type workerOptions struct {
	Local                    string
	JobWorkerSemSize         int64
	ExtractStateKeyCacheSize int
}

func newWorkerOptions(local string) *workerOptions {
	return &workerOptions{
		Local:                    local,
		JobWorkerSemSize:         1 << 9,
		ExtractStateKeyCacheSize: 1 << 13,
	}
}

func newFuncWorker(
	ctx context.Context,
	options *workerOptions,
	workID string,
	getState createStateFunc,
	maxOpsSize, stateMergeValuesPoolSize, stateMergersPoolSize uint64,
	extractStateKeyCacheSize int,
	workers, opsWorkers []string,
	opsShare, statesShare uint64,
) (
	insertWorkerStateMergeValuesWithStateOwnersFunc,
	workerUnmergedFunc,
	func() error,
	error,
) {
	log := newModuleLoggerFromContext(ctx, "worker", func(zctx zerolog.Context) zerolog.Context {
		return zctx.
			Str("work_id", workID).
			Str("local", options.Local)
	})

	log.Debug().
		Interface("options", options).
		Dict("workers", zerolog.Dict().
			Strs("workers", workers).
			Strs("ops_workers", opsWorkers),
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
		return nil, nil, nil, err
	}

	wctx, wctxCancel := contextWithCancelCause(ctx)

	extractStateMergeValueKeys, extractStateKeyClose := newFuncExtractStateMergeValueKeys(options.ExtractStateKeyCacheSize)

	var getStateMergers mergerGetStateMergersFunc
	var traverseStateMergers traverseStateMergersFunc

	switch g, t, _, err := newFuncStateMergers(
		stateMergersPoolSize,
		getState,
		extractStateMergeValueKeys,
	); {
	case err != nil:
		return nil, nil, nil, err
	default:
		getStateMergers = g
		traverseStateMergers = t
	}

	// FIXME stateMergeValues extractor

	var insertWorkerStateMergeValues insertWorkerStateMergeValuesWithStateOwnersFunc
	var workerUnmerged workerUnmergedFunc
	var workerMergerWait workerMergerWaitFunc

	switch i, u, w, err := newFuncWorkerMerger(
		wctx,
		options.Local,
		newFuncNewJobWorkerFunc(options.JobWorkerSemSize),
		getState,
		getStateMergers,
		uint64(len(workers)), maxOpsSize, maxOpsSize*2,
		opsWorkers, opsShare, statesShare,
	); {
	case err != nil:
		return nil, nil, nil, err
	default:
		insertWorkerStateMergeValues = i
		workerUnmerged = u
		workerMergerWait = w
	}

	// FIXME save (operations, states) and report saved files info to manager

	return insertWorkerStateMergeValues,
		workerUnmerged,
		func() error {
			defer func() {
				_ = workerMergerWait(nil, nil)
			}()

			return contextCauseError(wctx, wctxCancel, func(context.Context) error {
				defer func() {
					extractStateKeyClose()
				}()

				return workerMergerWait(
					func(unmerged []uint64, subStatesOwners []string) error {
						// FIXME report subStatesOwners to manager
						// FIXME broadcast unmerged to other subStatesOwners

						return nil
					},
					func(unmerged []uint64, reasons map[uint64]string) error {
						// FIXME report unmerged and reasons to manager

						_ = traverseStateMergers // FIXME remove

						return nil
					},
				)
			})
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
