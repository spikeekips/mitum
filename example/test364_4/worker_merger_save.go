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
	newStateFunc                           func(stateKey string, _ base.StateValue, ophs []util.Hash) (base.State, error)
	saveWorkerMergerStatesFunc             func(stateKey string, _ util.Hash) error
	saveWorkerMergerStatesFuncs            func(saveWorkerMergerStatesFunc) error
	saveWorkerMergerSendStatesFuncs        func(saveWorkerMergerStatesFuncs) error
	saveWorkerMergerUnmergedFunc           func(unmerged []uint64) error
	saveWorkerMergerMergeDoneFunc          func(saveWorkerMergerSendStatesFuncs, saveWorkerMergerUnmergedFunc) error
	saveWorkerMergerSendMergeDoneFuncs     func(context.Context, saveWorkerMergerMergeDoneFunc) error
	saveWorkerMergerSaveOperationsFunc     func(context.Context, func(operationsWorkerSaveOperationsFunc) error) (base.BlockMapItem, error)
	saveWorkerMergerSaveStateFunc          func(base.State) error
	saveWorkerMergerSaveStatesCallbackFunc func(func(saveWorkerMergerSaveStateFunc) error) error
	saveWorkerMergerSaveStatesFunc         func(total uint64) saveWorkerMergerSaveStatesCallbackFunc
	saveWorkerMergerSaveStatesFuncs        func(context.Context, func(saveWorkerMergerSaveStatesFunc) error) (base.BlockMapItem, error)
	saveWorkerMergerSaveReasonsFunc        func(reasons map[uint64]string) error
	saveWorkerMergerSaveTraverseStatesFunc func(func(base.State) error) error
	saveWorkerMergerSaveBlockMapItemFunc   func(base.BlockMapItem) error
	saveWorkerMergerFunc                   func(
		_ context.Context,
		minEnough,
		maxOps uint64,
		_ workerBroadcastPhaseDoneFunc,
		_ getStateFunc,
		_ isLocalStateValueFunc,
		_ operationsWorkerMergerBroadcastStateMergeValueFuncs,
		_ stateMergeValueExtractorGetOperationFunc,
		_ func() (newStateFunc, error),
		_ saveWorkerMergerSendMergeDoneFuncs,
		_ saveWorkerMergerSaveOperationsFunc,
		_ saveWorkerMergerSaveStatesFuncs,
	) (
		operationsWorkerLocalInsertOpsFuncs,
		operationsworkerInitialInsertOpsFuncs,
		workerMergerPhaseDoneFunc,
		func() error,
		func() error,
		error,
	)
)

type saveWorkerMergerArgs struct {
	*logging.Logging
	*operationsWorkerMergerArgs
	SaveReasons saveWorkerMergerSaveReasonsFunc
	SaveDone    func(context.Context) (saveWorkerMergerSaveBlockMapItemFunc, errorFunc, error)
}

func (args *saveWorkerMergerArgs) SetLogging(l *logging.Logging) *logging.Logging {
	args.operationsWorkerMergerArgs.SetLogging(l)

	return args.Logging.SetLogging(l)
}

func newSaveWorkerMergerArgs(
	args *operationsWorkerMergerArgs,
) *saveWorkerMergerArgs {
	return &saveWorkerMergerArgs{
		Logging: logging.NewLogging(func(zctx zerolog.Context) zerolog.Context {
			return zctx.Str("module", "save-worker-merger").Str("local", args.local)
		}),
		operationsWorkerMergerArgs: args,
		SaveReasons: func(reasons map[uint64]string) error {
			return util.ErrNotImplemented.Errorf("SaveReasons")
		},
		SaveDone: func(context.Context) (saveWorkerMergerSaveBlockMapItemFunc, errorFunc, error) {
			return nil, nil, util.ErrNotImplemented.Errorf("SaveDone")
		},
	}
}

func newFuncSaveWorkerMerger(
	args *saveWorkerMergerArgs,
	eargs *stateMergeValueExtractorArgs,
	wpargs *workerPhaseMergerArgs,
	pargs *phaseMergerArgs,
) saveWorkerMergerFunc {
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
		sendMergeDone saveWorkerMergerSendMergeDoneFuncs,
		saveOperations saveWorkerMergerSaveOperationsFunc,
		saveStates saveWorkerMergerSaveStatesFuncs,
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

				switch m, ds, err := newSaveWorkerMergerMergeDoneFunc(
					wctx,
					args,
					maxOps,
					unmerged,
					newTraverseStateFunc(args, traverseMergers, stateFunc),
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

				return saveDone(newSaveWorkerMergerSaveFunc(wctx, args, reasons, doneState, traverseOperations, saveOperations, saveStates))
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

func newSaveWorkerMergerMergeDoneFunc(
	ctx context.Context,
	args *saveWorkerMergerArgs,
	maxOps uint64,
	unmerged []uint64,
	traverseStates saveWorkerMergerSaveTraverseStatesFunc,
	sendMergeDone saveWorkerMergerSendMergeDoneFuncs,
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
			return sendMergeDone(ctx, func(statesf saveWorkerMergerSendStatesFuncs, unmergedf saveWorkerMergerUnmergedFunc) error {
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
					return statesf(func(f saveWorkerMergerStatesFunc) error {
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

func newSaveWorkerMergerSaveFunc(
	ctx context.Context,
	args *saveWorkerMergerArgs,
	reasons map[uint64]string,
	doneState util.CompactAppendSliceDoneFunc[base.State],
	traverseOperations func(operationsWorkerSaveOperationsFunc) error,
	saveOperations saveWorkerMergerSaveOperationsFunc,
	saveStates saveWorkerMergerSaveStatesFuncs,
) error {
	wctx, wctxCancel := ContextWithCancelCause(ctx)
	defer wctxCancel(nil)

	var saveDoneBlockMapItem saveWorkerMergerSaveBlockMapItemFunc
	var saveDone errorFunc

	switch b, d, err := args.SaveDone(wctx); {
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
		switch b, err := saveStates(wctx, newSaveWorkerMergerSaveStatesFunc(wctx, doneState, args.NewJobWorker)); {
		case err != nil, b == nil:
			return err
		default:
			return saveDoneBlockMapItem(b)
		}
	})
	addJob(func() error { return args.SaveReasons(reasons) })

	wg.Wait()

	return saveDone(context.Cause(wctx))
}

func newSaveWorkerMergerSaveStatesFunc(
	ctx context.Context,
	traverse util.CompactAppendSliceDoneFunc[base.State],
	newJobWorker newJobWorkerFunc,
) func(saveWorkerMergerSaveStatesFunc) error {
	return func(f saveWorkerMergerSaveStatesFunc) error {
		var jw JobWorker

		switch i, err := newJobWorker(ctx); {
		case err != nil:
			return err
		default:
			defer i.Close()

			jw = i
		}

		if err := traverse(func(s []base.State) error {
			return f(uint64(len(s)))(func(sf saveWorkerMergerSaveStateFunc) error {
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

func newTraverseStateFunc(
	args *saveWorkerMergerArgs,
	traverseMergers operationsWorkerMergerTraverseMergersFunc,
	stateFunc func() (newStateFunc, error),
) saveWorkerMergerSaveTraverseStatesFunc {
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
