package main

import (
	"context"
	"sync"

	"github.com/spikeekips/mitum/base"
	"github.com/spikeekips/mitum/util"
	"github.com/spikeekips/mitum/util/logging"
)

type (
	newStateFunc                 func(stateKey string, _ base.StateValue, ophs []util.Hash) (base.State, error)
	workerSaveMergeStatesFunc    func(stateKey string, _ util.Hash) error
	workerSaveMergeStatesFuncs   func(workerSaveMergeStatesFunc) error
	workerSaveSendStatesFuncs    func(workerSaveMergeStatesFuncs) error
	workerSaveUnmergedFunc       func(unmerged []uint64) error
	workerSaveMergeDoneFunc      func(workerSaveSendStatesFuncs, workerSaveUnmergedFunc) error
	workerSaveSendMergeDoneFuncs func(context.Context, workerSaveMergeDoneFunc) error
	workerSaveSaveOperationsFunc func(context.Context, func(operationsWorkerSaveOperationsFunc) error) (
		[]blockFileChunk, error)
	workerSaveStateFunc           func(base.State) error
	workerSaveStatesCallbackFunc  func(func(workerSaveStateFunc) error) error
	workerSaveStatesFunc          func(total uint64) workerSaveStatesCallbackFunc
	workerSaveStatesFuncs         func(context.Context, func(workerSaveStatesFunc) error) ([]blockFileChunk, error)
	workerSaveReasonsFunc         func(reasons map[uint64]string) error
	workerSaveTraverseStatesFunc  func(func(base.State) error) error
	workerSaveBlockFileChunksFunc func(base.BlockItemType, []blockFileChunk) error
	workerSaveSendSaveDoneFunc    func(context.Context, func(workerSaveBlockFileChunksFunc) error) error
	workerSaveFunc                func(
		_ context.Context,
		minEnough,
		maxOps uint64,
		_ workerBroadcastPhaseDoneFunc,
		_ getStateFunc,
		_ isLocalStateValueFunc,
		_ operationsWorkerMergerBroadcastStateMergeValueFuncs,
		_ stateMergeValueExtractorGetOperationFunc,
		_ func() (newStateFunc, error),
		_ workerSaveSendMergeDoneFuncs,
		_ workerSaveSaveOperationsFunc,
		_ workerSaveStatesFuncs,
		_ workerSaveReasonsFunc,
		_ workerSaveSendSaveDoneFunc,
	) (
		operationsWorkerLocalInsertOpsFuncs,
		operationsworkerInitialInsertOpsFuncs,
		workerMergerPhaseDoneFunc,
		func() error,
		func() error,
		error,
	)
)

type workerSaveArgs struct {
	*logging.Logging
	NewJobWorker newJobWorkerFunc
}

func newWorkerSaveArgs() *workerSaveArgs {
	return &workerSaveArgs{
		Logging:      NewModuleLogging("save-worker", nil),
		NewJobWorker: newFuncBaseJobWorker(1 << 6),
	}
}

func newFuncWorkerSave(
	args *workerSaveArgs,
	iargs *initialValuesWorkerMergerArgs,
	sargs *statesWorkerMergerArgs,
	oargs *operationsWorkerMergerArgs,
	pargs *phaseMergerArgs,
) workerSaveFunc {
	newOperationsWorkerMergerf := newFuncOperationsWorkerMerger(oargs, iargs, sargs, pargs)

	return func(
		ctx context.Context,
		minEnough, maxOps uint64,
		broadcastPhaseDone workerBroadcastPhaseDoneFunc,
		getState getStateFunc,
		isLocalStateValue isLocalStateValueFunc,
		broadcastStateMergeValues operationsWorkerMergerBroadcastStateMergeValueFuncs,
		getOperation stateMergeValueExtractorGetOperationFunc,
		stateFunc func() (newStateFunc, error),
		sendMergeDone workerSaveSendMergeDoneFuncs,
		saveOperations workerSaveSaveOperationsFunc,
		saveStates workerSaveStatesFuncs,
		saveReasons workerSaveReasonsFunc,
		sendSaveDone workerSaveSendSaveDoneFunc,
	) (
		operationsWorkerLocalInsertOpsFuncs,
		operationsworkerInitialInsertOpsFuncs,
		workerMergerPhaseDoneFunc,
		func() error,
		func() error,
		error,
	) {
		wctx, wctxCancel := contextWithCancelCause(ctx)

		var opLocalInsert operationsWorkerLocalInsertOpsFuncs
		var opWorkerInsert operationsworkerInitialInsertOpsFuncs
		var opWorkerPhaseDone workerMergerPhaseDoneFunc
		var opWait operationsWorkerMergerWaitFunc

		switch i, wi, p, w, err := newOperationsWorkerMergerf(
			wctx,
			minEnough,
			maxOps,
			getState,
			isLocalStateValue,
			broadcastStateMergeValues,
			getOperation,
		); {
		case err != nil:
			return nil, nil, nil, nil, nil, err
		default:
			opLocalInsert = i
			opWorkerInsert = wi
			opWorkerPhaseDone = p
			opWait = w
		}

		_, mergeDone, mergeDoneWait := newDoneContextFunc(wctx)
		_, saveDone, saveDoneWait := newDoneContextFunc(wctx)

		go func() {
			_ = wctxCancel(opWait(broadcastPhaseDone, func(
				unmerged []uint64,
				reasons map[uint64]string,
				traverseMergers operationsWorkerMergerTraverseMergersFunc,
				traverseOperations func(operationsWorkerSaveOperationsFunc) error,
			) error {
				var doneState util.CompactAppendSliceDoneFunc[base.State]

				switch m, ds, err := newWorkerSaveMergeDone(
					wctx,
					maxOps,
					unmerged,
					newTraverseState(traverseMergers, stateFunc),
					sendMergeDone,
				); {
				case err != nil:
					return err
				default:
					defer ds(nil)

					if err := mergeDone(m()); err != nil {
						return err
					}

					doneState = ds
				}

				return saveDone(newWorkerSave(
					wctx,
					args,
					reasons,
					doneState,
					traverseOperations,
					saveOperations,
					saveStates,
					saveReasons,
					sendSaveDone,
				))
			}))
		}()

		return opLocalInsert,
			opWorkerInsert,
			opWorkerPhaseDone,
			mergeDoneWait,
			saveDoneWait,
			nil
	}
}

func newWorkerSaveMergeDone(
	ctx context.Context,
	maxOps uint64,
	unmerged []uint64,
	traverseStates workerSaveTraverseStatesFunc,
	sendMergeDone workerSaveSendMergeDoneFuncs,
) (
	func() error,
	util.CompactAppendSliceDoneFunc[base.State],
	error,
) {
	var addState util.CompactAppendSliceAddFunc[base.State]
	var doneState util.CompactAppendSliceDoneFunc[base.State]

	switch a, d, err := util.CompactAppendSliceV1[base.State](maxOps, 1<<9); {
	case err != nil:
		return nil, nil, err
	default:
		addState = a
		doneState = d
	}

	return func() error {
			return sendMergeDone(ctx, func(statesf workerSaveSendStatesFuncs, unmergedf workerSaveUnmergedFunc) error {
				var wg sync.WaitGroup

				mctx, mctxCancel := contextWithCancelCause(ctx)
				defer mctxCancel(nil)

				addJob := func(f func() error) {
					wg.Add(1)

					go func() {
						defer wg.Done()

						if err := f(); err != nil {
							mctxCancel(err)
						}
					}()
				}

				addJob(func() error {
					return statesf(func(f workerSaveMergeStatesFunc) error {
						return traverseStates(func(st base.State) error {
							addState(st)

							return f(st.Key(), st.Hash())
						})
					})
				})
				addJob(func() error {
					return unmergedf(unmerged)
				})

				wg.Wait()

				return context.Cause(mctx)
			})
		},
		doneState,
		nil
}

func newWorkerSave(
	ctx context.Context,
	args *workerSaveArgs,
	reasons map[uint64]string,
	doneState util.CompactAppendSliceDoneFunc[base.State],
	traverseOperations func(operationsWorkerSaveOperationsFunc) error,
	saveOperations workerSaveSaveOperationsFunc,
	saveStates workerSaveStatesFuncs,
	saveReasons workerSaveReasonsFunc,
	sendSaveDone workerSaveSendSaveDoneFunc,
) error {
	wctx, wctxCancel := contextWithCancelCause(ctx)
	defer wctxCancel(nil)

	var wg sync.WaitGroup

	addJob := func(f func() error) {
		wg.Add(1)

		go func() {
			defer wg.Done()

			if err := f(); err != nil {
				wctxCancel(err)
			}
		}()
	}

	chunks := util.NewSingleLockedMap[base.BlockItemType, []blockFileChunk]()
	defer chunks.Close()

	addJob(func() error {
		switch bs, err := saveOperations(wctx, func(f operationsWorkerSaveOperationsFunc) error {
			return traverseOperations(f)
		}); {
		case err != nil, len(bs) < 1:
			return err
		case !chunks.SetValue(base.BlockItemOperations, bs):
			return util.ErrFound.Errorf("%q alreay set", base.BlockItemOperations)
		default:
			return nil
		}
	})
	addJob(func() error {
		switch bs, err := saveStates(wctx, newWorkerSaveStates(wctx, doneState)); {
		case err != nil, len(bs) < 1:
			return err
		case !chunks.SetValue(base.BlockItemStates, bs):
			return util.ErrFound.Errorf("%q alreay set", base.BlockItemOperations)
		default:
			return nil
		}
	})
	addJob(func() error { return saveReasons(reasons) })

	wg.Wait()

	if err := context.Cause(wctx); err != nil {
		return err
	}

	return sendSaveDone(wctx, func(f workerSaveBlockFileChunksFunc) (err error) {
		_ = chunks.Traverse(func(it base.BlockItemType, bs []blockFileChunk) bool {
			err = f(it, bs)

			return err == nil
		})

		return err
	})
}

func newWorkerSaveStates(
	ctx context.Context,
	traverse util.CompactAppendSliceDoneFunc[base.State],
) func(workerSaveStatesFunc) error {
	return func(f workerSaveStatesFunc) error {
		return traverse(func(s []base.State) error {
			return f(uint64(len(s)))(func(sf workerSaveStateFunc) error {
				return util.TraverseSliceV1(s, func(_ int, st base.State) (bool, error) {
					return true, sf(st)
				})
			})
		})
	}
}

func newTraverseState(
	traverseMergers operationsWorkerMergerTraverseMergersFunc,
	stateFunc func() (newStateFunc, error),
) workerSaveTraverseStatesFunc {
	return func(f func(base.State) error) error {
		var newState newStateFunc

		switch n, err := stateFunc(); {
		case err != nil:
			return err
		default:
			newState = n
		}

		return traverseMergers(func(stateKey string, stv base.StateValue, ophs []util.Hash) error {
			if f == nil {
				return nil
			}

			if len(ophs) < 1 {
				return util.ErrNotFound.Errorf("empty state ophs")
			}

			switch st, err := newState(stateKey, stv, ophs); {
			case err != nil:
				return err
			case st == nil:
				return nil
			default:
				return f(st)
			}
		})
	}
}
