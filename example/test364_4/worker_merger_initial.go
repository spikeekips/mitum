package main

import (
	"context"
	"slices"

	"github.com/spikeekips/mitum/util"
)

type (
	workerInitialInsertValuesFunc  func(opindex uint64, _ []StateMergeValue) error
	workerInitialInsertValuesFuncs func(worker string) (
		workerInitialInsertValuesFunc,
		func() error,
		error,
	)
	workerInitialInsertOpFuncs func(worker string) (
		phaseMergerXMergeFunc,
		func() error,
		error,
	)
	workerInitialPhaseMergerResultFunc func(unmerged []uint64, reasons map[uint64]string) error
	workerInitialPhaseMergerWaitFunc   func(workerInitialPhaseMergerResultFunc) error
)

type initialValuesWorkerMergerArgs struct {
	*baseWorkerMergerArgs
	NewValuesPool func() (util.LockedMap[uint64, []StateMergeValue], error)
}

func newInitialValuesWorkerMergerArgs(args *baseWorkerMergerArgs) *initialValuesWorkerMergerArgs {
	return &initialValuesWorkerMergerArgs{
		baseWorkerMergerArgs: args,
		NewValuesPool: func() (util.LockedMap[uint64, []StateMergeValue], error) {
			return nil, util.ErrNotImplemented.Errorf("NewValuesPool")
		},
	}
}

func newInitialValuesWorkerMerger(
	ctx context.Context,
	args *initialValuesWorkerMergerArgs,
) (
	_ workerInitialInsertValuesFuncs,
	workerPhaseDone workerMergerPhaseDoneFunc,
	wait workerInitialPhaseMergerWaitFunc,
	_ error,
) {
	var vp util.LockedMap[uint64, []StateMergeValue]

	switch i, err := args.NewValuesPool(); {
	case err != nil:
		return nil, nil, nil, err
	default:
		vp = i
	}

	var pargs *PhaseMergerArgs
	var enoughWorkers uint64

	switch p, w, err := patchWorkerMergerArgsWithPhaseMergerArgs(args, vp); {
	case err != nil:
		return nil, nil, nil, err
	default:
		pargs = p
		enoughWorkers = w.EnoughWorkers
	}

	var initialWorkerInsert workerInitialInsertOpFuncs
	var initialWait workerInitialPhaseMergerWaitFunc

	switch i, w, err := newInitialPhaseMerger(ctx, args, pargs, enoughWorkers); {
	case err != nil:
		return nil, nil, nil, err
	default:
		initialWorkerInsert = i
		initialWait = w
	}

	var workerMergerWait workerMergerWaitFunc

	switch i, w, err := newBaseWorkerMerger(ctx, args.baseWorkerMergerArgs); {
	case err != nil:
		return nil, nil, nil, err
	default:
		workerPhaseDone = i
		workerMergerWait = w
	}

	return func(worker string) (
			workerInitialInsertValuesFunc,
			func() error,
			error,
		) {
			switch i, d, err := initialWorkerInsert(worker); {
			case err != nil:
				return nil, nil, err
			case i == nil:
				return nil, nil, nil
			default:
				return func(opindex uint64, values []StateMergeValue) error {
					if _, created, _ := vp.Set(opindex, func(_ []StateMergeValue, found bool) ([]StateMergeValue, error) {
						if found {
							return nil, util.ErrLockedSetIgnore
						}

						return values, nil
					}); !created {
						return nil
					}

					return i(opindex)
				}, d, nil
			}
		},
		workerPhaseDone,
		func(f workerInitialPhaseMergerResultFunc) error {
			defer vp.Close()

			var reasons map[uint64]string

			if err := initialWait(func(unmerged []uint64, reasons map[uint64]string) error {
				unmerged = slices.Clone(unmerged)

				reasons = reasons

				if err := workerPhaseDone(localWorkerName, unmerged); err != nil {
					return err
				}

				return args.BroadcastPhaseDone(ctx, unmerged)
			}); err != nil {
				return err
			}

			return workerMergerWait(func(unmerged []uint64) error {
				if f == nil {
					return nil
				}

				return f(unmerged, reasons)
			})
		},
		nil
}

func newInitialPhaseMerger(
	ctx context.Context,
	args *initialValuesWorkerMergerArgs,
	pargs *PhaseMergerArgs,
	enoughWorkers uint64,
) (
	_ workerInitialInsertOpFuncs,
	wait workerInitialPhaseMergerWaitFunc,
	_ error,
) {
	var initialMerge phaseMergerXMergeFunc
	var initialDone func()
	var initialWait phaseMergerWaitFunc

	switch m, _, d, w, err := NewPhaseMerger(ctx, pargs); {
	case err != nil:
		return nil, nil, err
	default:
		initialMerge = m
		initialDone = d
		initialWait = w
	}

	var checkWorker func(string, bool, func(_, _ bool) error) error
	var closeCheckWorker func()

	switch c, d, err := newIsEnoughkWokerPhaseMergerFuncs(enoughWorkers); {
	case err != nil:
		return nil, nil, err
	default:
		checkWorker = c
		closeCheckWorker = func() { d() }
	}

	wait = func(f workerInitialPhaseMergerResultFunc) error {
		defer closeCheckWorker()

		return initialWait(func(_, newUnmerged []uint64, reasons map[uint64]string) error {
			if f == nil {
				return nil
			}

			return f(newUnmerged, reasons)
		})
	}

	return func(worker string) (
			phaseMergerXMergeFunc,
			func() error,
			error,
		) {
			return initialMerge,
				func() error {
					return checkWorker(worker, false, func(enough, ok bool) error {
						switch {
						case !ok:
						case enough:
							initialDone()
						}

						return nil
					})
				},
				nil
		},
		wait,
		nil
}

func patchWorkerMergerArgsWithPhaseMergerArgs(
	args *initialValuesWorkerMergerArgs,
	vp util.LockedMap[uint64, []StateMergeValue],
) (
	pargs *PhaseMergerArgs,
	wpargs *workerPhaseMergerArgs,
	_ error,
) {
	var wpnewf newWorkerPhaseMergerFunc

	switch a, n, err := args.NewWorkerPhaseMergerFuncs(); {
	case err != nil:
		return nil, nil, err
	default:
		wpargs = a
		wpnewf = n
	}

	var pnewf newPhaseMergerFunc

	switch a, n, err := wpargs.NewPhaseMergerFuncs(); {
	case err != nil:
		return nil, nil, err
	default:
		pargs = a
		pnewf = n
	}

	pargs.GetValuesFunc = func(opindex uint64) ([]StateMergeValue, error) {
		switch i, found := vp.Value(opindex); {
		case !found:
			return nil, util.ErrNotFound.Errorf("state merge values, opindex=%d", opindex)
		default:
			return i, nil
		}
	}

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

	args.IsLocalOp = func(opindex uint64) bool {
		return vp.Exists(opindex)
	}

	return pargs, wpargs, nil
}
