package main

import (
	"context"
	"sync"

	"github.com/rs/zerolog"
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
	workerSaveSaveOperationsFunc func(context.Context, func(operationsWorkerSaveOperationsFunc) error) (base.BlockMapItem, error)
	workerSaveStateFunc          func(base.State) error
	workerSaveStatesCallbackFunc func(func(workerSaveStateFunc) error) error
	workerSaveStatesFunc         func(total uint64) workerSaveStatesCallbackFunc
	workerSaveStatesFuncs        func(context.Context, func(workerSaveStatesFunc) error) (base.BlockMapItem, error)
	workerSaveReasonsFunc        func(reasons map[uint64]string) error
	workerSaveTraverseStatesFunc func(func(base.State) error) error
	workerSaveBlockMapItemFunc   func(base.BlockMapItem) error
	workerSaveSendSaveDoneFunc   func(context.Context) (workerSaveBlockMapItemFunc, errorFunc, error)
	workerSaveFunc               func(
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
	*operationsWorkerMergerArgs
}

func (args *workerSaveArgs) SetLogging(l *logging.Logging) *logging.Logging {
	args.operationsWorkerMergerArgs.SetLogging(l)

	return args.Logging.SetLogging(l)
}

func newWorkerSaveArgs(
	args *operationsWorkerMergerArgs,
) *workerSaveArgs {
	return &workerSaveArgs{
		Logging: logging.NewLogging(func(zctx zerolog.Context) zerolog.Context {
			return zctx.Str("module", "save-worker-merger").Str("local", args.local)
		}),
		operationsWorkerMergerArgs: args,
	}
}

func newFuncWorkerSave(
	args *workerSaveArgs,
	eargs *stateMergeValueExtractorArgs,
	wpargs *workerPhaseMergerArgs,
	pargs *phaseMergerArgs,
) workerSaveFunc {
	newOperationsWorkerMergerf := newFuncOperationsWorkerMerger(args.operationsWorkerMergerArgs, eargs, wpargs, pargs)

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
		wctx, wctxCancel := ContextWithCancelCause(ctx)

		var opLocalInsert operationsWorkerLocalInsertOpsFuncs
		var opWorkerInsert operationsworkerInitialInsertOpsFuncs
		var opWorkerPhaseDone workerMergerPhaseDoneFunc
		var opWait operationsWorkerMergerWaitFunc

		switch i, wi, p, w, err := newOperationsWorkerMergerf(
			wctx,
			minEnough,
			maxOps,
			broadcastPhaseDone,
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

		_, mergeDone, mergeDoneWait := NewDoneContextFunc(wctx)
		_, saveDone, saveDoneWait := NewDoneContextFunc(wctx)

		go func() {
			_ = wctxCancel(opWait(func(
				unmerged []uint64,
				reasons map[uint64]string,
				traverseMergers operationsWorkerMergerTraverseMergersFunc,
				traverseOperations func(operationsWorkerSaveOperationsFunc) error,
			) error {
				var doneState util.CompactAppendSliceDoneFunc[base.State]

				switch m, ds, err := newWorkerSaveMergeDone(
					wctx,
					args,
					maxOps,
					unmerged,
					newTraverseState(args, traverseMergers, stateFunc),
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
	args *workerSaveArgs,
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

				mctx, mctxCancel := ContextWithCancelCause(ctx)
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
	wctx, wctxCancel := ContextWithCancelCause(ctx)
	defer wctxCancel(nil)

	var saveDoneBlockMapItem workerSaveBlockMapItemFunc
	var saveDone errorFunc

	switch b, d, err := sendSaveDone(wctx); {
	case err != nil:
		return err
	default:
		saveDoneBlockMapItem = b
		saveDone = d
	}

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

	addJob(func() error {
		switch b, err := saveOperations(wctx, func(f operationsWorkerSaveOperationsFunc) error {
			return traverseOperations(f)
		}); {
		case err != nil, b == nil:
			return err
		default:
			return saveDoneBlockMapItem(b)
		}
	})
	addJob(func() error {
		switch b, err := saveStates(wctx, newWorkerSaveStates(wctx, doneState, args.NewJobWorker)); {
		case err != nil, b == nil:
			return err
		default:
			return saveDoneBlockMapItem(b)
		}
	})
	addJob(func() error { return saveReasons(reasons) })

	wg.Wait()

	return saveDone(context.Cause(wctx))
}

func newWorkerSaveStates(
	ctx context.Context,
	traverse util.CompactAppendSliceDoneFunc[base.State],
	newJobWorker newJobWorkerFunc,
) func(workerSaveStatesFunc) error {
	return func(f workerSaveStatesFunc) error {
		var jw JobWorker

		switch i, err := newJobWorker(ctx); {
		case err != nil:
			return err
		default:
			defer i.Close()

			jw = i
		}

		if err := traverse(func(s []base.State) error {
			return f(uint64(len(s)))(func(sf workerSaveStateFunc) error {
				return util.TraverseSliceV1(s, func(_ int, st base.State) (bool, error) {
					return true, jw.NewJob(func(context.Context) error {
						return sf(st)
					})
				})
			})
		}); err != nil {
			return err
		}

		jw.JobDone()

		return jw.Wait()
	}
}

func newTraverseState(
	args *workerSaveArgs,
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
