package main

import (
	"context"
	"slices"

	"github.com/rs/zerolog"
	"github.com/spikeekips/mitum/util"
)

type (
	insertStateMergeValuesFunc           = func(opindex uint64, _ []StateMergeValue) error
	workerInsertStateMergeValuesCallback = func(
		insertStateMergeValuesFunc,
		in2ErrorFunc[[]string /* sub */, []string /* affected */],
	) error
	workerInsertStateMergeValuesFunc = func(
		worker string,
		_ workerInsertStateMergeValuesCallback,
	) error
	workerWaitFunc = func(
		inErrorFunc[[]string /* state owners */],
		in2ErrorFunc[[]string /* workers */, []uint64 /* unmerged */],
		in3ErrorFunc[[]uint64 /* unmerged */, map[uint64]string /* reasons */, []string /* state hashes */],
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
	workID string,
	workers, opsWorkers []string,
	opsShare, statesShare uint64,
	ops []uint64,
) (
	workerInsertStateMergeValuesFunc,
	workerUnmergedFunc,
	workerWaitFunc,
	error,
) {
	log := newModuleLoggerFromContext(ctx, "worker", func(zctx zerolog.Context) zerolog.Context {
		return zctx.
			Str("work_id", workID).
			Str("local", options.Local)
	})

	opsSize := uint64(len(ops))

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
		Msg("newFuncWorker")

	if err := isValidWorkerArgs(
		options.Local,
		workers, opsWorkers,
		opsShare, statesShare,
	); err != nil {
		return nil, nil, nil, err
	}

	wctx, wctxCancel := contextWithCancelCause(ctx)

	approxStatesKeys := opsSize * 2

	extractStateMergeValueKeys, extractStateKeyClose := newFuncExtractStateMergeValueKeys(
		options.MaxExtractStateKeyCacheSize, int(approxStatesKeys))

	var getStateMergers mergerGetStateMergersFunc
	var traverseStateMergers traverseStateMergersFunc
	var stateMergersClose func()

	switch g, t, c, err := newFuncStateMergers(
		approxStatesKeys,
		getState,
		extractStateMergeValueKeys,
	); {
	case err != nil:
		return nil, nil, nil, err
	default:
		getStateMergers, traverseStateMergers, stateMergersClose = g, t, c
	}

	var insertWorkerStateMergeValues workerMergerInsertStateMergeValuesFunc
	var workerUnmerged workerUnmergedFunc
	var workerMergerWait workerMergerWaitFunc

	switch i, u, w, err := newFuncWorkerMerger(
		wctx,
		options.Local,
		newFuncNewJobWorkerFunc(options.JobWorkerSemSize),
		getState,
		getStateMergers,
		uint64(len(workers)), opsSize, approxStatesKeys,
		opsWorkers, opsShare, statesShare,
	); {
	case err != nil:
		return nil, nil, nil, err
	default:
		insertWorkerStateMergeValues, workerUnmerged, workerMergerWait = i, u, w
	}

	var stateOwnersLocal localExtractedStateOwnersFunc
	var stateOwnersWorker workerStateOwnersSetFunc
	var stateOwnersDone func(insertStateOwnersSetFunc) error

	switch l, o, d, err := newFuncWorkerStateOwners(wctx, options.Local, uint64(len(workers))); {
	case err != nil:
		return nil, nil, nil, err
	default:
		stateOwnersLocal = l
		stateOwnersWorker = o
		stateOwnersDone = d

		_ = stateOwnersLocal // FIXME remove
	}

	// FIXME insert ops
	// FIXME stateMergeValues extractor if len(ops) > 0

	insert := newFuncWorkerInsert(wctx, insertWorkerStateMergeValues, stateOwnersWorker) // FIXME remove

	return insert,
		workerUnmerged,
		func(
			reportStateOwners inErrorFunc[[]string],
			broadcastUnmerged in2ErrorFunc[[]string, []uint64],
			reportMerged in3ErrorFunc[[]uint64, map[uint64]string, []string],
		) error {
			defer func() {
				_ = workerMergerWait(nil, nil)
			}()

			return contextCauseError(wctx, wctxCancel, func(context.Context) error {
				defer func() {
					extractStateKeyClose()
					stateMergersClose()
				}()

				if err := workerMergerWait(
					func(unmerged []uint64) (localSubws []string, _ error) {
						return localSubws, stateOwnersDone(func(subws, affected []string) error {
							if err := reportStateOwners(slices.Concat(subws, affected)); err != nil {
								return err
							}

							if err := broadcastUnmerged(affected, unmerged); err != nil {
								return err
							}

							localSubws = slices.Clone(subws)

							return nil
						})
					},
					func(unmerged []uint64, reasons map[uint64]string) error {
						var addStateKey compactAppendSliceAddFunc[string]
						var doneStateKey compactAppendSliceDoneFunc[string]

						if err := rCompactAppendSlice[string](approxStatesKeys, 1<<9, &addStateKey, &doneStateKey); err != nil {
							return err
						}

						if err := traverseStateMergers(func(stateKey string, merger StateMerger) (bool, error) {
							addStateKey(stateKey) // FIXME state key and hash

							// FIXME merger.Close()

							return true, nil
						}); err != nil {
							return err
						}

						return doneStateKey(func(s []string) error {
							return reportMerged(unmerged, reasons, s)
						})
					},
				); err != nil {
					return err
				}

				// FIXME save (operations, states) and report saved files info to manager

				return nil
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

func newFuncWorkerInsert(
	ctx context.Context,
	insertWorker workerMergerInsertStateMergeValuesFunc,
	addStatesOwners workerStateOwnersSetFunc,
) workerInsertStateMergeValuesFunc {
	return func(
		worker string,
		f func(
			insertStateMergeValuesFunc,
			in2ErrorFunc[[]string, []string],
		) error,
	) error {
		insert, insertDone, err := insertWorker(worker)
		if err != nil {
			return err
		}

		return insertDone(f(insert, func(subws, affected []string) error { // FIXME sub owners
			return addStatesOwners(worker, subws, affected)
		}))
	}
}

func mergeStateOwners(a, b []string) func(func([]string) error) error {
	switch {
	case len(a) < 1:
		return func(f func([]string) error) error { return f(b) }
	case len(b) < 1:
		return func(f func([]string) error) error { return f(a) }
	}

	as, bs := a, b

	if len(b) > len(a) {
		as = b
		bs = a
	}

	var add compactAppendSliceAddFunc[string]
	var done compactAppendSliceDoneFunc[string]

	if err := rCompactAppendSlice[string](uint64(len(as)+len(bs)), 0, &add, &done); err != nil {
		return func(func([]string) error) error {
			return err
		}
	}

	defer done(nil)

	_ = traverseSlice(bs, func(_ int, opindex string) (bool, error) {
		if slices.Index(as, opindex) >= 0 {
			add(opindex)
		}

		return true, nil
	})

	return func(f func([]string) error) error {
		return done(func(s []string) error {
			if len(s) < 1 {
				return f(as)
			}

			m := make([]string, len(as)+len(s))
			defer clear(m)

			copy(m, as)
			copy(m[len(as):], s)

			return f(m)
		})
	}
}
