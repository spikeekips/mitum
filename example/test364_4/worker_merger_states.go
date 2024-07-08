package main

import (
	"context"

	"github.com/rs/zerolog"
	"github.com/spikeekips/mitum/base"
	"github.com/spikeekips/mitum/util"
	"github.com/spikeekips/mitum/util/logging"
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
	*logging.Logging
	*initialValuesWorkerMergerArgs
	GetState     getStateFunc
	NewStateFunc func() (newStateFunc, error)
}

func (args *statesWorkerMergerArgs) SetLogging(l *logging.Logging) *logging.Logging {
	args.initialValuesWorkerMergerArgs.SetLogging(l)

	return args.Logging.SetLogging(l)
}

func newStatesWorkerMergerArgs(args *initialValuesWorkerMergerArgs) *statesWorkerMergerArgs {
	return &statesWorkerMergerArgs{
		Logging: logging.NewLogging(func(zctx zerolog.Context) zerolog.Context {
			return zctx.Str("module", "states-worker-merger")
		}),
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
	workerInitialInsertOpsFuncs,
	workerMergerPhaseDoneFunc,
	statesWorkerMergerWaitFunc,
	error,
) {
	var mergersPool util.LockedMap[string, StateMerger]

	// NOTE maximum number of states approximately max ops * 2.
	switch i, err := util.NewShardedMap[string, StateMerger](args.MaxOpsSize*2, nil); {
	case err != nil:
		return nil, nil, nil, err
	default:
		mergersPool = i
	}

	var stateOps stateOpsFuncs
	var closeMergers func()

	switch s, c, err := patchStatesWorkerMergerArgs(args, mergersPool); {
	case err != nil:
		return nil, nil, nil, err
	default:
		stateOps = s
		closeMergers = c
	}

	var initialValuesInsert workerInitialInsertOpsFuncs
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
			mergersPool.Close()
		}()

		var newState newStateFunc

		if newState, gerr = args.NewStateFunc(); gerr != nil {
			return gerr
		}

		_ = mergersPool.Traverse(func(stateKey string, merger StateMerger) bool {
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
			case f == nil:
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
	mergersPool util.LockedMap[string, StateMerger],
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

	// FIXME set logging

	var pargs *PhaseMergerArgs
	var pnewf newPhaseMergerFunc

	switch a, n, err := wpargs.NewPhaseMergerFuncs(); {
	case err != nil:
		return nil, nil, err
	default:
		pargs = a
		pnewf = n
	}

	pargs.GetMergersFunc, closeMergers = newGetMergersFunc(mergersPool, args.GetState)

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
