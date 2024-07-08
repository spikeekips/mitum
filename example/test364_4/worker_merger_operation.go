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
	operationsWorkerInsertOpFunc     func(opindex uint64, oph, fact util.Hash) error
	operationsWorkerInsertOpsFunc    func(operationsWorkerInsertOpFunc) error
	operationsWorkerInsertOpsFuncs   func(operationsWorkerInsertOpsFunc) error
	operationsWorkerMergerResultFunc func(
		unmerged []uint64,
		reasons map[uint64]string,
		traverseStates func(func(base.State) error) error,
		traverseOperations func(func(opindex uint64, _ Operation) error) error,
	) error
	operationsWorkerMergerWaitFunc func(operationsWorkerMergerResultFunc) error
)

type operationsWorkerMergerArgs struct {
	*logging.Logging
	*statesWorkerMergerArgs
	e                               *stateMergeValueExtractorArgs
	DistributeStateMergeValuesFuncs workerInitialInsertOpsFuncs
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
			return zctx.Str("module", "operations-worker-merger")
		}),
		statesWorkerMergerArgs: args,
		e:                      eArgs,
		DistributeStateMergeValuesFuncs: func(string) (
			workerInitialInsertOpFunc,
			func() error,
			error,
		) {
			return nil, nil, util.ErrNotImplemented.Errorf("DistributeStateMergeValuesFuncs")
		},
	}
}

func newOperationsWorkerMerger(
	ctx context.Context,
	args *operationsWorkerMergerArgs,
) (
	_ operationsWorkerInsertOpsFuncs,
	_ workerInitialInsertOpsFuncs,
	workerPhaseDone workerMergerPhaseDoneFunc,
	wait operationsWorkerMergerWaitFunc,
	_ error,
) {
	// FIXME use own context.Context
	var initialInsert workerInitialInsertOpsFuncs
	var statesWait statesWorkerMergerWaitFunc

	switch i, p, w, err := newStatesWorkerMerger(ctx, args.statesWorkerMergerArgs); {
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

	traverseOpsPool, closeOpsPool := patchOperationsWorkerMergerArgs(args, initialInsertLocal)

	var extractInsert stateMergeValueExtractorInsertFunc
	var extractInsertDone func()
	var extractWait stateMergeValueExtractorWaitFunc

	switch i, d, w, err := newStateMergeValueExtractor(ctx, args.e); {
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

	return func(fs operationsWorkerInsertOpsFunc) error {
			defer extractInsertDone()

			return fs(func(opindex uint64, oph, _ util.Hash) error {
				return extractInsert(opindex, oph)
			})
		},
		func(worker string) (workerInitialInsertOpFunc, func() error, error) {
			if worker == localWorkerName {
				return nil, nil, util.ErrFound.Errorf("%q", localWorkerName)
			}

			return initialInsert(worker)
		},
		workerPhaseDone,
		func(f operationsWorkerMergerResultFunc) error {
			defer closeOpsPool()

			mergeReasons, mergeReasonsToUnmerged := newUpdateReasonsFuncs()

			if err := extractWait(func(reasons map[uint64]string) error {
				_ = mergeReasons(reasons)

				return nil
			}); err != nil {
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
					func(f func(uint64, Operation) error) (gerr error) {
						_ = traverseOpsPool(func(opindex uint64, op Operation) bool {
							gerr = f(opindex, op)

							return gerr == nil
						})

						return gerr
					},
				)
			})
		},
		nil
}

func patchOperationsWorkerMergerArgs(
	args *operationsWorkerMergerArgs,
	initialInsertLocal workerInitialInsertOpFunc,
) (
	traverseOpsPool func(func(opindex uint64, _ Operation) bool) bool,
	closeOpsPool func(),
) {
	opsPool := util.NewSingleLockedMap[uint64, Operation]()

	origDone := args.e.Done
	if origDone == nil {
		origDone = func(context.Context, uint64, Operation, []StateMergeValue) error {
			return nil
		}
	}

	args.e.Done = func(ctx context.Context, opindex uint64, op Operation, values []StateMergeValue) error {
		_ = opsPool.SetValue(opindex, op)

		if err := initialInsertLocal(opindex, values); err != nil {
			return err
		}

		if err := origDone(ctx, opindex, op, values); !errors.Is(err, util.ErrNotImplemented) {
			return err
		}

		// FIXME distribute values to other workers

		return nil
	}

	return opsPool.Traverse, opsPool.Close
}

func newDistributeStateMergeValues(
	workers []string,
	distributeStateMergeValuesFuncs workerInitialInsertOpsFuncs,
) (
	func(worker string, opindex uint64, _ []StateMergeValue) error,
	func() error,
) {
	funcs := util.NewSingleLockedMap[string, [2]interface{}]()

	// FIXME use JobWorker

	return func(worker string, opindex uint64, values []StateMergeValue) error {
			return funcs.GetOrCreate(
				worker,
				func(f [2]interface{}, _ bool) error {
					return f[0].(workerInitialInsertOpFunc)(opindex, values)
				},
				func() ([2]interface{}, error) {
					switch i, d, err := distributeStateMergeValuesFuncs(worker); {
					case err != nil:
						return [2]interface{}{}, err
					default:
						return [2]interface{}{i, d}, nil
					}
				},
			)
		},
		func() error {
			defer funcs.Close()

			return util.TraverseSliceV1(workers, func(_ int, worker string) (bool, error) {
				if f, found := funcs.Value(worker); found {
					return true, f[1].(func() error)()
				}

				switch _, d, err := distributeStateMergeValuesFuncs(worker); {
				case err != nil:
					return false, err
				default:
					return true, d()
				}
			})
		}
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
