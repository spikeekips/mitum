package main

import (
	"context"
	"slices"

	"github.com/rs/zerolog"
	"github.com/spikeekips/mitum/base"
	"github.com/spikeekips/mitum/util"
	"github.com/spikeekips/mitum/util/logging"
)

type (
	operationsWorkerLocalInsertOpFunc          func(opindex uint64, oph, fact util.Hash) error
	operationsWorkerInsertOpsFunc              func(operationsWorkerLocalInsertOpFunc) error
	operationsWorkerLocalInsertOpsFuncs        func(operationsWorkerInsertOpsFunc) error
	operationsWorkerSaveOperationFunc          func(opindex uint64, _ Operation) error
	operationsWorkerSaveOperationsCallbackFunc func(func(operationsWorkerSaveOperationFunc) error) error
	operationsWorkerSaveOperationsFunc         func(total uint64) operationsWorkerSaveOperationsCallbackFunc
	operationsworkerInitialInsertOpFunc        func(opindex uint64, oph util.Hash, _ []StateMergeValue) error
	operationsworkerInitialInsertOpsFuncs      func(worker string) (
		operationsworkerInitialInsertOpFunc,
		func() error,
		error,
	)

	operationsWorkerMergerNewStateFunc        func(stateKey string, _ base.StateValue, ophs []util.Hash) error
	operationsWorkerMergerTraverseMergersFunc func(operationsWorkerMergerNewStateFunc) error
	operationsWorkerMergerResultFunc          func(
		unmerged []uint64,
		reasons map[uint64]string,
		traverseMergers operationsWorkerMergerTraverseMergersFunc,
		traverseOperations func(operationsWorkerSaveOperationsFunc) error,
	) error
	operationsWorkerMergerBroadcastStateMergeValueFuncs func(context.Context) (
		operationsworkerInitialInsertOpFunc,
		func() error,
		error,
	)
	operationsWorkerMergerWaitFunc func(operationsWorkerMergerResultFunc) error
	newOperationsWorkerMergerFunc  func( // FIXME rename to operationsWorkerMergerFunc
		_ context.Context,
		minEnough,
		maxOps uint64,
		_ workerBroadcastPhaseDoneFunc,
		_ getStateFunc,
		_ isLocalStateValueFunc,
		_ operationsWorkerMergerBroadcastStateMergeValueFuncs,
		_ stateMergeValueExtractorGetOperationFunc,
	) (
		operationsWorkerLocalInsertOpsFuncs,
		operationsworkerInitialInsertOpsFuncs,
		workerMergerPhaseDoneFunc,
		operationsWorkerMergerWaitFunc,
		error,
	)
	isLocalStateValueFunc func(stateKey string) bool
)

type operationsWorkerMergerArgs struct {
	*logging.Logging
	*statesWorkerMergerArgs
}

func (args *operationsWorkerMergerArgs) SetLogging(l *logging.Logging) *logging.Logging {
	args.statesWorkerMergerArgs.SetLogging(l)

	return args.Logging.SetLogging(l)
}

func newOperationsWorkerMergerArgs(args *statesWorkerMergerArgs) *operationsWorkerMergerArgs {
	return &operationsWorkerMergerArgs{
		Logging: logging.NewLogging(func(zctx zerolog.Context) zerolog.Context {
			return zctx.Str("module", "operations-worker-merger").Str("local", args.local)
		}),
		statesWorkerMergerArgs: args,
	}
}

func newFuncOperationsWorkerMerger(
	args *operationsWorkerMergerArgs,
	eargs *stateMergeValueExtractorArgs,
	wpargs *workerPhaseMergerArgs,
	pargs *phaseMergerArgs,
) newOperationsWorkerMergerFunc {
	newStatesWorkerMergerf := newFuncStatesWorkerMerger(args.statesWorkerMergerArgs, wpargs, pargs)
	newStateMergeValueExtractorf := newFuncStateMergeValueExtractor(eargs)

	return func(
		ctx context.Context,
		minEnough, maxOps uint64,
		broadcastPhaseDone workerBroadcastPhaseDoneFunc,
		getState getStateFunc,
		isLocalStateValue isLocalStateValueFunc,
		broadcastStateMergeValues operationsWorkerMergerBroadcastStateMergeValueFuncs,
		getOperation stateMergeValueExtractorGetOperationFunc,
	) (
		operationsWorkerLocalInsertOpsFuncs,
		operationsworkerInitialInsertOpsFuncs,
		workerMergerPhaseDoneFunc,
		operationsWorkerMergerWaitFunc,
		error,
	) {
		wctx, wctxCancel := ContextWithCancelCause(ctx)

		var initialInsert workerInitialInsertOpsFuncs
		var workerPhaseDone workerMergerPhaseDoneFunc
		var statesWait statesWorkerMergerWaitFunc

		switch i, p, w, err := newStatesWorkerMergerf(wctx, minEnough, maxOps, broadcastPhaseDone, getState); {
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

		opsPool := util.NewSingleLockedMap[uint64, Operation]()
		ophsPool := util.NewSingleLockedMap[uint64, util.Hash]()

		var broadcastStateValuesDone func() error
		var extractorDone stateMergeValueExtractorExtractDoneFunc

		switch d, e, err := patchOperationsWorkerMerger(
			wctx,
			args,
			initialInsertLocal,
			opsPool,
			isLocalStateValue,
			broadcastStateMergeValues,
		); {
		case err != nil:
			return nil, nil, nil, nil, err
		default:
			broadcastStateValuesDone = d
			extractorDone = e
		}

		var extractInsert stateMergeValueExtractorInsertFunc
		var extractInsertDone func()
		var extractWait stateMergeValueExtractorWaitFunc

		switch i, d, w, err := newStateMergeValueExtractorf(wctx, getOperation, extractorDone); {
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
			defer func() {
				opsPool.Close()
				ophsPool.Close()
			}()

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
				traverseMergers statesWorkerMergerTraverseMergers,
			) error {
				if f == nil {
					return nil
				}

				return f(
					mergeReasonsToUnmerged(unmerged),
					mergeReasons(reasons),
					func(f operationsWorkerMergerNewStateFunc) error {
						return traverseMergers(func(stateKey string, value base.StateValue, ops []uint64) error {
							ophs := make([]util.Hash, len(ops))

							if err := util.TraverseSliceV1(ops, func(i int, opindex uint64) (bool, error) {
								switch op, found := opsPool.Value(opindex); {
								case !found:
									switch h, found := ophsPool.Value(opindex); {
									case !found:
										return false, util.ErrNotFound.Errorf("op not found in ops pool, %d", opindex)
									default:
										ophs[i] = h
									}
								default:
									ophs[i] = op.Hash()
								}

								return true, nil
							}); err != nil {
								return err
							}

							return f(stateKey, value, ophs)
						})
					},
					newTraverseOperationsFunc(wctx, uint64(opsPool.Len()), opsPool.Traverse, args.NewJobWorker),
				)
			})
		}

		return func(fs operationsWorkerInsertOpsFunc) error {
				defer extractInsertDone()

				if err := fs(func(opindex uint64, oph, _ util.Hash) error {
					return extractInsert(opindex, oph)
				}); err != nil {
					args.Log().Error().Err(err).Msg("extract insert error")

					return wctxCancel(err)
				}

				return nil
			},
			func(worker string) (operationsworkerInitialInsertOpFunc, func() error, error) {
				if worker == localWorkerName {
					return nil, nil, wctxCancel(util.ErrFound.Errorf("%q", localWorkerName))
				}

				var insert workerInitialInsertOpFunc
				var done func() error

				switch i, d, err := initialInsert(worker); {
				case err != nil:
					return nil, nil, err
				default:
					insert = i
					done = d
				}

				return func(opindex uint64, oph util.Hash, values []StateMergeValue) error {
						_ = ophsPool.SetValue(opindex, oph)

						return insert(opindex, values)
					},
					done,
					nil
			},
			workerPhaseDone,
			func(f operationsWorkerMergerResultFunc) error {
				return wctxCancel(waitf(f))
			},
			nil
	}
}

func patchOperationsWorkerMerger(
	ctx context.Context,
	args *operationsWorkerMergerArgs,
	initialInsertLocal workerInitialInsertOpFunc,
	opsPool util.LockedMap[uint64, Operation],
	isLocalStateValue isLocalStateValueFunc,
	broadcastStateMergeValues operationsWorkerMergerBroadcastStateMergeValueFuncs,
) (
	broadcastStateValuesDone func() error,
	extractorDone stateMergeValueExtractorExtractDoneFunc,
	_ error,
) {
	var broadcastStateValues operationsworkerInitialInsertOpFunc

	switch d, o, err := broadcastStateMergeValues(ctx); {
	case err != nil:
		return nil, nil, err
	default:
		broadcastStateValues = d
		broadcastStateValuesDone = o
	}

	filterLocalStateValues := newFilterLocalStateValuesFunc(isLocalStateValue)

	extractorDone = func(ctx context.Context, opindex uint64, op Operation, values []StateMergeValue) error {
		_ = opsPool.SetValue(opindex, op)

		if filtered := filterLocalStateValues(values); len(filtered) > 0 {
			if err := initialInsertLocal(opindex, filtered); err != nil {
				return err
			}
		}

		return broadcastStateValues(opindex, op.Hash(), values)
	}

	return broadcastStateValuesDone, extractorDone, nil
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

func newTraverseOperationsFunc(
	ctx context.Context,
	total uint64,
	traverse func(func(uint64, Operation) bool) bool,
	newJobWorker newJobWorkerFunc,
) func(operationsWorkerSaveOperationsFunc) error {
	return func(f operationsWorkerSaveOperationsFunc) error {
		return f(total)(func(sf operationsWorkerSaveOperationFunc) error {
			var jw JobWorker

			switch i, err := newJobWorker(ctx); {
			case err != nil:
				return err
			default:
				defer i.Close()

				jw = i
			}

			var gerr error

			_ = traverse(func(opindex uint64, op Operation) bool {
				gerr = jw.NewJob(func(context.Context) error {
					return sf(opindex, op)
				})

				return gerr == nil
			})
			if gerr != nil {
				return gerr
			}

			jw.JobDone()

			return jw.Wait()
		})
	}
}
