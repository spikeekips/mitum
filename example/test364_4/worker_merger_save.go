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
	newStateFunc                           func(stateKey string, _ base.StateValue, ops []uint64) (base.State, error)
	saveWorkerMergerStatesFunc             func(stateKey string, _ util.Hash) error
	saveWorkerMergerStatesFuncs            func(saveWorkerMergerStatesFunc) error
	saveWorkerMergerSendStatesFuncs        func(saveWorkerMergerStatesFuncs) error
	saveWorkerMergerUnmergedFunc           func(unmerged []uint64) error
	saveWorkerMergerMergeDoneFuncs         func(saveWorkerMergerSendStatesFuncs, saveWorkerMergerUnmergedFunc) error
	saveWorkerMergerSendMergeDoneFuncs     func(context.Context, saveWorkerMergerMergeDoneFuncs) error
	saveWorkerMergerSaveOperationsFuncs    func(func(operationsWorkerSaveOperationFunc) error) error
	saveWorkerMergerSaveStateFunc          func(base.State) error
	saveWorkerMergerSaveStatesFuncs        func(func(saveWorkerMergerSaveStateFunc) error) error
	saveWorkerMergerSaveReasonsFunc        func(reasons map[uint64]string) error
	saveWorkerMergerSaveTraverseStatesFunc func(func(base.State) error) error
)

type saveWorkerMergerArgs struct {
	*logging.Logging
	*operationsWorkerMergerArgs
	NewStateFunc   func() (newStateFunc, error)
	MergeDone      saveWorkerMergerSendMergeDoneFuncs
	SaveOperations saveWorkerMergerSaveOperationsFuncs
	SaveStates     saveWorkerMergerSaveStatesFuncs
	SaveReasons    saveWorkerMergerSaveReasonsFunc
	SaveDone       func() error
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
		NewStateFunc: func() (newStateFunc, error) {
			return nil, util.ErrNotImplemented.Errorf("NewStateFunc")
		},
		MergeDone: func(context.Context, saveWorkerMergerMergeDoneFuncs) error {
			return util.ErrNotImplemented.Errorf("MergeDone")
		},
		SaveOperations: func(func(operationsWorkerSaveOperationFunc) error) error {
			return util.ErrNotImplemented.Errorf("SaveOperations")
		},
		SaveStates: func(func(saveWorkerMergerSaveStateFunc) error) error {
			return util.ErrNotImplemented.Errorf("SaveStates")
		},
		SaveReasons: func(reasons map[uint64]string) error {
			return util.ErrNotImplemented.Errorf("SaveReasons")
		},
		SaveDone: func() error {
			return util.ErrNotImplemented.Errorf("SaveDone")
		},
	}
}

func newSaveWorkerMerger(ctx context.Context, args *saveWorkerMergerArgs) (
	operationsWorkerLocalInsertOpsFuncs,
	workerInitialInsertOpsFuncs,
	workerMergerPhaseDoneFunc,
	func() error,
	func() error,
	error,
) {
	wctx, wctxCancel := ContextWithCancelCause(ctx)

	var opLocalInsert operationsWorkerLocalInsertOpsFuncs
	var opWorkerInsert workerInitialInsertOpsFuncs
	var opWorkerPhaseDone workerMergerPhaseDoneFunc
	var opWait operationsWorkerMergerWaitFunc

	switch i, wi, p, w, err := newOperationsWorkerMerger(wctx, args.operationsWorkerMergerArgs); {
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
			traverseMergers statesWorkerMergerTraverseMergers,
			traverseOperations func(operationsWorkerSaveOperationFunc) error,
		) error {
			var doneState util.CompactAppendSliceDoneFunc[base.State]

			switch m, d, err := newSaveWorkerMergerMergeDoneFunc(wctx, args, unmerged, newTraverseStateFunc(args, traverseMergers)); {
			case err != nil:
				return err
			default:
				defer d(nil)

				if err := mergeDone(m()); err != nil {
					return err
				}

				doneState = d
			}

			return saveDone(newSaveWorkerMergerSaveFunc(wctx, args, reasons, traverseOperations, doneState))
		}))
	}()

	return opLocalInsert,
		opWorkerInsert,
		opWorkerPhaseDone,
		mergeDoneWait,
		saveDoneWait,
		nil
}

func newSaveWorkerMergerMergeDoneFunc(
	ctx context.Context,
	args *saveWorkerMergerArgs,
	unmerged []uint64,
	traverseStates saveWorkerMergerSaveTraverseStatesFunc,
) (
	func() error,
	util.CompactAppendSliceDoneFunc[base.State],
	error,
) {
	var jw JobWorker

	switch i, err := NewBaseJobWorker(ctx, int64(args.MaxOpsSize)); {
	case err != nil:
		return nil, nil, err
	default:
		jw = i
	}

	var addState util.CompactAppendSliceAddFunc[base.State]
	var doneState util.CompactAppendSliceDoneFunc[base.State]

	switch a, d, err := util.CompactAppendSliceV1[base.State](uint64(args.MaxOpsSize)*2, 1<<9); {
	case err != nil:
		return nil, nil, err
	default:
		addState = a
		doneState = d
	}

	return func() error {
			defer jw.Close()

			if err := args.MergeDone(ctx, func(statesf saveWorkerMergerSendStatesFuncs, unmergedf saveWorkerMergerUnmergedFunc) error {
				if err := statesf(func(f saveWorkerMergerStatesFunc) error {
					return traverseStates(func(st base.State) error {
						addState(st)

						return jw.NewJob(func(context.Context) error {
							return f(st.Key(), st.Hash())
						})
					})
				}); err != nil {
					return err
				}

				if err := jw.NewJob(func(context.Context) error {
					return unmergedf(unmerged)
				}); err != nil {
					return err
				}

				jw.JobDone()

				return jw.Wait()
			}); err != nil {
				return err
			}

			return nil
		},
		doneState,
		nil
}

func newSaveWorkerMergerSaveFunc(
	ctx context.Context,
	args *saveWorkerMergerArgs,
	reasons map[uint64]string,
	traverseOperations func(operationsWorkerSaveOperationFunc) error,
	doneState util.CompactAppendSliceDoneFunc[base.State],
) error {
	wctx, wctxCancel := ContextWithCancelCause(ctx)
	defer wctxCancel(nil)

	saveOperations := newSaveWorkerMergerSaveOperationsFunc(wctx, traverseOperations, int64(args.MaxOpsSize))
	saveStates := newSaveWorkerMergerSaveStatesFunc(wctx, doneState, int64(args.MaxOpsSize))

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

	addJob(func() error { return args.SaveOperations(saveOperations) })
	addJob(func() error { return args.SaveStates(saveStates) })
	addJob(func() error { return args.SaveReasons(reasons) })

	wg.Wait()

	return args.SaveDone()
}

func newSaveWorkerMergerSaveOperationsFunc(
	ctx context.Context,
	traverse func(operationsWorkerSaveOperationFunc) error,
	semSize int64,
) func(operationsWorkerSaveOperationFunc) error {
	return func(f operationsWorkerSaveOperationFunc) error {
		var jw JobWorker

		switch i, err := NewBaseJobWorker(ctx, semSize); {
		case err != nil:
			return err
		default:
			defer i.Close()

			jw = i
		}

		if err := traverse(func(opindex uint64, op Operation) error {
			return jw.NewJob(func(context.Context) error {
				return f(opindex, op)
			})
		}); err != nil {
			return err
		}

		jw.JobDone()

		return jw.Wait()
	}
}

func newSaveWorkerMergerSaveStatesFunc(
	ctx context.Context,
	traverse util.CompactAppendSliceDoneFunc[base.State],
	semSize int64,
) func(saveWorkerMergerSaveStateFunc) error {
	return func(f saveWorkerMergerSaveStateFunc) error {
		var jw JobWorker

		switch i, err := NewBaseJobWorker(ctx, semSize); {
		case err != nil:
			return err
		default:
			defer i.Close()

			jw = i
		}

		if err := traverse(func(s []base.State) error {
			return util.TraverseSliceV1(s, func(_ int, st base.State) (bool, error) {
				return true, jw.NewJob(func(context.Context) error {
					return f(st)
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
	traverseMergers statesWorkerMergerTraverseMergers,
) saveWorkerMergerSaveTraverseStatesFunc {
	return func(f func(base.State) error) error {
		var newState newStateFunc

		switch n, err := args.NewStateFunc(); {
		case err != nil:
			return err
		default:
			newState = n
		}

		return traverseMergers(func(stateKey string, stv base.StateValue, ops []uint64) error {
			if f == nil {
				return nil
			}

			if len(ops) < 1 {
				return util.ErrNotFound.Errorf("empty state ops")
			}

			switch st, err := newState(stateKey, stv, ops); {
			case err != nil:
				return err
			default:
				return f(st)
			}
		})
	}
}
