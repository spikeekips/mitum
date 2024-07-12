package main

import (
	"context"
	"slices"

	"github.com/pkg/errors"
	"github.com/rs/zerolog"
	"github.com/spikeekips/mitum/base"
	"github.com/spikeekips/mitum/util"
	"github.com/spikeekips/mitum/util/logging"
)

type (
	operationsWorkerLocalInsertOpFunc   func(opindex uint64, oph, fact util.Hash) error
	operationsWorkerInsertOpsFunc       func(operationsWorkerLocalInsertOpFunc) error
	operationsWorkerLocalInsertOpsFuncs func(operationsWorkerInsertOpsFunc) error
	operationsWorkerSaveOperationFunc   func(opindex uint64, _ Operation) error
	operationsWorkerMergerResultFunc    func(
		unmerged []uint64,
		reasons map[uint64]string,
		traverseStates func(func(base.State) error) error,
		traverseOperations func(operationsWorkerSaveOperationFunc) error,
	) error
	operationsWorkerMergerBroadcastStateMergeValueFuncs func(context.Context) (
		workerInitialInsertOpFunc,
		func() error,
		error,
	)
	operationsWorkerMergerWaitFunc func(operationsWorkerMergerResultFunc) error
)

type operationsWorkerMergerArgs struct {
	*logging.Logging
	*statesWorkerMergerArgs
	e                              *stateMergeValueExtractorArgs
	IsLocalStateValue              func(stateKey string) bool
	BroadcastStateMergeValuesFuncs operationsWorkerMergerBroadcastStateMergeValueFuncs
}

func (args *operationsWorkerMergerArgs) SetLogging(l *logging.Logging) *logging.Logging {
	args.statesWorkerMergerArgs.SetLogging(l)
	args.e.SetLogging(l)

	return args.Logging.SetLogging(l)
}

func newOperationsWorkerMergerArgs(
	args *statesWorkerMergerArgs,
	eArgs *stateMergeValueExtractorArgs,
) *operationsWorkerMergerArgs {
	return &operationsWorkerMergerArgs{
		Logging: logging.NewLogging(func(zctx zerolog.Context) zerolog.Context {
			return zctx.Str("module", "operations-worker-merger").Str("local", args.local)
		}),
		statesWorkerMergerArgs: args,
		e:                      eArgs,
		IsLocalStateValue:      func(string) bool { return false },
		BroadcastStateMergeValuesFuncs: func(context.Context) (
			workerInitialInsertOpFunc,
			func() error,
			error,
		) {
			return nil, nil, util.ErrNotImplemented.Errorf("BroadcastStateMergeValuesFuncs")
		},
	}
}

func newOperationsWorkerMerger(ctx context.Context, args *operationsWorkerMergerArgs) (
	operationsWorkerLocalInsertOpsFuncs,
	workerInitialInsertOpsFuncs,
	workerMergerPhaseDoneFunc,
	operationsWorkerMergerWaitFunc,
	error,
) {
	wctx, wctxCancel := context.WithCancelCause(ctx)

	var initialInsert workerInitialInsertOpsFuncs
	var workerPhaseDone workerMergerPhaseDoneFunc
	var statesWait statesWorkerMergerWaitFunc

	switch i, p, w, err := newStatesWorkerMerger(wctx, args.statesWorkerMergerArgs); {
	case err != nil:
		return nil, nil, nil, nil, err
	default:
		initialInsert = i
		workerPhaseDone = p
		statesWait = w
	}

	var initialInsertLocal workerInitialInsertOpFunc
	var initialInsertLocalDone func() error

	switch i, d, err := initialInsert(localWorkerName); {
	case err != nil:
		return nil, nil, nil, nil, err
	default:
		initialInsertLocal = i
		initialInsertLocalDone = d
	}

	var traverseOpsPool func(func(opindex uint64, _ Operation) bool) bool
	var broadcastStateValuesDone func() error
	var closeOpsPool func()

	switch t, d, c, err := patchOperationsWorkerMergerArgs(wctx, args, initialInsertLocal); {
	case err != nil:
		return nil, nil, nil, nil, err
	default:
		traverseOpsPool, broadcastStateValuesDone, closeOpsPool = t, d, c
	}

	var extractInsert stateMergeValueExtractorInsertFunc
	var extractInsertDone func()
	var extractWait stateMergeValueExtractorWaitFunc

	switch i, d, w, err := newStateMergeValueExtractor(wctx, args.e); {
	case err != nil:
		return nil, nil, nil, nil, err
	default:
		extractInsert = i
		extractInsertDone = d
		extractWait = func(f stateMergeValueExtractorReasonsFunc) error {
			if err := w(f); err != nil {
				return err
			}

			return initialInsertLocalDone()
		}
	}

	waitf := func(f operationsWorkerMergerResultFunc) error {
		defer closeOpsPool()

		mergeReasons, mergeReasonsToUnmerged := newUpdateReasonsFuncs()

		if err := extractWait(func(reasons map[uint64]string) error {
			_ = mergeReasons(reasons)

			return nil
		}); err != nil {
			return err
		}

		if err := broadcastStateValuesDone(); err != nil {
			return err
		}

		return statesWait(func(
			unmerged []uint64,
			reasons map[uint64]string,
			traverseStates func(func(base.State) error) error,
		) error {
			if f == nil {
				return nil
			}

			return f(
				mergeReasonsToUnmerged(unmerged),
				mergeReasons(reasons),
				traverseStates,
				func(f operationsWorkerSaveOperationFunc) (gerr error) {
					_ = traverseOpsPool(func(opindex uint64, op Operation) bool {
						gerr = f(opindex, op)

						return gerr == nil
					})

					return gerr
				},
			)
		})
	}

	return func(fs operationsWorkerInsertOpsFunc) error {
			defer extractInsertDone()

			err := fs(func(opindex uint64, oph, _ util.Hash) error {
				return extractInsert(opindex, oph)
			})
			if err != nil {
				args.Log().Error().Err(err).Msg("extract insert error")

				wctxCancel(err)
			}

			return err
		},
		func(worker string) (workerInitialInsertOpFunc, func() error, error) {
			if worker == localWorkerName {
				return nil, nil, util.ErrFound.Errorf("%q", localWorkerName)
			}

			i, d, err := initialInsert(worker)
			if err != nil {
				args.Log().Error().Err(err).Msg("initial insert error")

				wctxCancel(err)
			}

			return i, d, err
		},
		workerPhaseDone,
		OnceFuncCallback(func(f operationsWorkerMergerResultFunc) error {
			err := waitf(f)
			wctxCancel(err)

			return err
		}),
		nil
}

func patchOperationsWorkerMergerArgs(
	ctx context.Context,
	args *operationsWorkerMergerArgs,
	initialInsertLocal workerInitialInsertOpFunc,
) (
	traverseOpsPool func(func(opindex uint64, _ Operation) bool) bool,
	broadcastStateValuesDone func() error,
	closef func(),
	_ error,
) {
	var broadcastStateValues workerInitialInsertOpFunc

	switch d, o, err := args.BroadcastStateMergeValuesFuncs(ctx); {
	case err != nil:
		return nil, nil, nil, err
	default:
		broadcastStateValues = d
		broadcastStateValuesDone = o
	}

	opsPool := util.NewSingleLockedMap[uint64, Operation]()

	origDone := args.e.Done
	if origDone == nil {
		origDone = func(context.Context, uint64, Operation, []StateMergeValue) error {
			return nil
		}
	}

	filterLocalStateValues := newFilterLocalStateValuesFunc(args.IsLocalStateValue)

	args.e.Done = func(ctx context.Context, opindex uint64, op Operation, values []StateMergeValue) error {
		_ = opsPool.SetValue(opindex, op)

		if filtered := filterLocalStateValues(values); len(filtered) > 0 {
			if err := initialInsertLocal(opindex, filtered); err != nil {
				return err
			}
		}

		if err := origDone(ctx, opindex, op, values); !errors.Is(err, util.ErrNotImplemented) {
			return err
		}

		return broadcastStateValues(opindex, values)
	}

	args.e.SetLogging(args.Logging)

	return opsPool.Traverse, broadcastStateValuesDone, opsPool.Close, nil
}

func newUpdateReasonsFuncs() (
	func(reasons map[uint64]string) map[uint64]string,
	func(unmerged []uint64) []uint64,
) {
	var m map[uint64]string

	return func(reasons map[uint64]string) map[uint64]string {
			if m == nil {
				m = reasons

				return m
			}

			TraverseMap(reasons, func(opindex uint64, reason string) (bool, error) {
				m[opindex] = reason

				return true, nil
			})

			return m
		},
		func(unmerged []uint64) []uint64 {
			if len(m) < 1 {
				return unmerged
			}

			n := make([]uint64, len(unmerged)+len(m))
			copy(n, unmerged)

			i := len(unmerged)

			TraverseMap(m, func(opindex uint64, _ string) (bool, error) {
				if slices.Index(unmerged, opindex) < 0 {
					n[i] = opindex

					i++
				}

				return true, nil
			})

			return n[:i]
		}
}

func newFilterLocalStateValuesFunc(
	isLocal func(stateKey string) bool,
) func([]StateMergeValue) []StateMergeValue {
	return func(values []StateMergeValue) []StateMergeValue {
		return util.FilterSlice(values, func(value StateMergeValue) bool {
			return isLocal(value.Key())
		})
	}
}
