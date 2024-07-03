package main

import (
	"context"

	"github.com/spikeekips/mitum/base"
	"github.com/spikeekips/mitum/util"
)

type (
	getStateFunc                 func(stateKey string) (base.State, bool, error)
	statesWorkerMergerResultFunc func(
		unmerged []uint64,
		reasons map[uint64]string,
		_ func(func(base.State) error) error,
	) error
	statesWorkerMergerWaitFunc func(statesWorkerMergerResultFunc) error
	newStateFunc               func(stateKey string, _ base.StateValue, ops []uint64) (base.State, error)
)

type statesWorkerMergerArgs struct {
	*initialValuesWorkerMergerArgs
	GetState     getStateFunc
	NewStateFunc func() (newStateFunc, error)
}

func newStatesWorkerMergerArgs(args *initialValuesWorkerMergerArgs) *statesWorkerMergerArgs {
	return &statesWorkerMergerArgs{
		initialValuesWorkerMergerArgs: args,
		GetState: func(string) (base.State, bool, error) {
			return nil, false, util.ErrNotImplemented.Errorf("GetState")
		},
		NewStateFunc: func() (newStateFunc, error) {
			return nil, util.ErrNotImplemented.Errorf("NewStateFunc")
		},
	}
}

func newStatesWorkerMerger(
	ctx context.Context,
	args *statesWorkerMergerArgs,
) (
	workerInitialInsertValuesFuncs,
	workerMergerPhaseDoneFunc,
	statesWorkerMergerWaitFunc,
	error,
) {
	var statesPool util.LockedMap[string, StateMerger]

	// NOTE maximum number of states approximately max ops * 2.
	switch i, err := util.NewShardedMap[string, StateMerger](args.MaxOpsSize*2, nil); {
	case err != nil:
		return nil, nil, nil, err
	default:
		statesPool = i
	}

	var stateOps stateOpsFuncs
	var closeMergers func()

	switch s, c, err := patchStatesWorkerMergerArgs(args, statesPool); {
	case err != nil:
		return nil, nil, nil, err
	default:
		stateOps = s
		closeMergers = c
	}

	var initialValuesInsert workerInitialInsertValuesFuncs
	var initialValuesPhaseDone workerMergerPhaseDoneFunc
	var initialValuesWait initialValuesWorkerMergerWaitFunc

	switch i, p, w, err := newInitialValuesWorkerMerger(ctx, args.initialValuesWorkerMergerArgs); {
	case err != nil:
		return nil, nil, nil, err
	default:
		initialValuesInsert = i
		initialValuesPhaseDone = p
		initialValuesWait = w
	}

	traverseStates := func(f func(st base.State) error) (gerr error) {
		getStateOp, closeStateOps := stateOps()
		defer func() {
			closeStateOps()
			statesPool.Close()
		}()

		var newState newStateFunc

		switch n, err := args.NewStateFunc(); {
		case err != nil:
			return err
		default:
			newState = n
		}

		_ = statesPool.Traverse(func(stateKey string, merger StateMerger) bool {
			var v base.StateValue

			if v, gerr = merger.Close(); gerr != nil || v == nil {
				return true
			}

			ops := getStateOp(stateKey)
			if len(ops) < 1 {
				gerr = util.ErrNotFound.Errorf("empty state ops, state key=%q", stateKey)

				return false
			}

			var st base.State

			switch st, gerr = newState(stateKey, v, ops); {
			case gerr != nil:
			default:
				gerr = f(st)
			}

			return gerr == nil
		})

		return gerr
	}

	return initialValuesInsert,
		initialValuesPhaseDone,
		func(f statesWorkerMergerResultFunc) error {
			defer closeMergers()

			return initialValuesWait(func(unmerged []uint64, reasons map[uint64]string) error {
				return f(unmerged, reasons, traverseStates)
			})
		},
		nil
}

func patchStatesWorkerMergerArgs(
	args *statesWorkerMergerArgs,
	statesPool util.LockedMap[string, StateMerger],
) (
	stateOps stateOpsFuncs,
	closeMergers func(),
	_ error,
) {
	var wpargs *workerPhaseMergerArgs
	var wpnewf newWorkerPhaseMergerFunc

	switch a, n, err := args.NewWorkerPhaseMergerFuncs(); {
	case err != nil:
		return nil, nil, err
	default:
		wpargs = a
		wpnewf = n
	}

	var pargs *PhaseMergerArgs
	var pnewf newPhaseMergerFunc

	switch a, n, err := wpargs.NewPhaseMergerFuncs(); {
	case err != nil:
		return nil, nil, err
	default:
		pargs = a
		pnewf = n
	}

	pargs.GetMergersFunc, closeMergers = newGetMergersFunc(statesPool, args.GetState)

	wpargs.NewPhaseMergerFuncs = func() (
		*PhaseMergerArgs,
		newPhaseMergerFunc,
		error,
	) {
		return pargs, pnewf, nil
	}

	args.NewWorkerPhaseMergerFuncs = func() (
		*workerPhaseMergerArgs,
		newWorkerPhaseMergerFunc,
		error,
	) {
		return wpargs, wpnewf, nil
	}

	switch s, o, err := newSetStateOpFuncs(args.MaxOpsSize); {
	case err != nil:
		return nil, nil, err
	default:
		args.SetStateOpFunc = func() (setStateOpFunc, error) { return s, nil }
		stateOps = o
	}

	return stateOps, closeMergers, nil
}
