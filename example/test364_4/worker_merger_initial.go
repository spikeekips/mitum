package main

import (
	"context"
	"slices"

	"github.com/spikeekips/mitum/util"
)

type (
	workerBroadcastPhaseDoneFunc   func(_ context.Context, newUnmerged []uint64) error
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
	initialValuesWorkerMergerResultFunc func(unmerged []uint64, reasons map[uint64]string) error
	initialValuesWorkerMergerWaitFunc   func(initialValuesWorkerMergerResultFunc) error
	setStateOpFunc                      func(opindex uint64, stateKeys []string, _ xMergeFlag)
	getStateOpsFunc                     func(stateKey string) (ops []uint64)
	stateOpsFuncs                       func() (getStateOpsFunc, func())
)

type initialValuesWorkerMergerArgs struct {
	*baseWorkerMergerArgs
	BroadcastPhaseDone  workerBroadcastPhaseDoneFunc
	NewValuesPool       func() (util.LockedMap[uint64, []StateMergeValue], error)
	SetStateOpFunc      func() (setStateOpFunc, error)
	MaxOpsSize          uint64
	MaxJobWorkerSemSize uint64
}

func newInitialValuesWorkerMergerArgs(args *baseWorkerMergerArgs) *initialValuesWorkerMergerArgs {
	return &initialValuesWorkerMergerArgs{
		baseWorkerMergerArgs: args,
		BroadcastPhaseDone: func(_ context.Context, newUnmerged []uint64) error {
			return util.ErrNotImplemented.Errorf("BroadcastPhaseDone")
		},
		NewValuesPool: func() (util.LockedMap[uint64, []StateMergeValue], error) {
			return nil, util.ErrNotImplemented.Errorf("NewValuesPool")
		},
		SetStateOpFunc: func() (setStateOpFunc, error) {
			return nil, util.ErrNotImplemented.Errorf("SetStateOpFunc")
		},
		MaxOpsSize:          1 << 9,
		MaxJobWorkerSemSize: 1 << 9,
	}
}

func newInitialValuesWorkerMerger(
	ctx context.Context,
	args *initialValuesWorkerMergerArgs,
) (
	_ workerInitialInsertValuesFuncs,
	workerPhaseDone workerMergerPhaseDoneFunc,
	wait initialValuesWorkerMergerWaitFunc,
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
	var closeArgs func()

	switch p, w, c, err := patchWorkerMergerArgsWithPhaseMergerArgs(args, vp); {
	case err != nil:
		return nil, nil, nil, err
	default:
		pargs = p
		enoughWorkers = w.EnoughWorkers
		closeArgs = c
	}

	var initialWorkerInsert workerInitialInsertOpFuncs
	var initialWait initialValuesWorkerMergerWaitFunc

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

	var setStateOp func(uint64, []StateMergeValue, xMergeFlag)

	switch i, err := args.SetStateOpFunc(); {
	case err != nil:
		return nil, nil, nil, err
	default:
		setStateOp = func(opindex uint64, values []StateMergeValue, x xMergeFlag) {
			i(opindex, pargs.ExtractStateMergeValueKeysFunc(opindex, values), x)
		}
	}

	_, initialStateOpsDone, initialStateOpsWait := NewDoneContextFunc(ctx)

	setInitialUnmergedState := func(unmerged []uint64) error {
		jw, _ := NewBaseJobWorker(ctx, int64(args.MaxJobWorkerSemSize))

		if err := util.TraverseSliceV1(unmerged, func(_ int, opindex uint64) (bool, error) {
			return true, jw.NewJob(func(context.Context) error {
				if values, found := vp.Value(opindex); found {
					setStateOp(opindex, values, unmergeFlag)
				}

				return nil
			})
		}); err != nil {
			return err
		}

		jw.JobDone()

		return jw.Wait()
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

					setStateOp(opindex, values, mergeFlag)

					return i(opindex)
				}, d, nil
			}
		},
		workerPhaseDone,
		func(f initialValuesWorkerMergerResultFunc) error {
			defer func() {
				vp.Close()
				closeArgs()
			}()

			var reasons map[uint64]string

			if err := initialWait(func(unmerged []uint64, r map[uint64]string) error {
				unmerged = slices.Clone(unmerged)

				reasons = r

				if err := workerPhaseDone(localWorkerName, unmerged); err != nil {
					return err
				}

				if err := args.BroadcastPhaseDone(ctx, unmerged); err != nil {
					return err
				}

				go initialStateOpsDone(setInitialUnmergedState(unmerged))

				return nil
			}); err != nil {
				return err
			}

			return workerMergerWait(func(unmerged []uint64) error {
				if err := initialStateOpsWait(); err != nil {
					return err
				}

				if f == nil {
					return nil
				}

				if err := setInitialUnmergedState(unmerged); err != nil {
					return err
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
	wait initialValuesWorkerMergerWaitFunc,
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

	wait = func(f initialValuesWorkerMergerResultFunc) error {
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
	_ func(),
	_ error,
) {
	var wpnewf newWorkerPhaseMergerFunc

	switch a, n, err := args.NewWorkerPhaseMergerFuncs(); {
	case err != nil:
		return nil, nil, nil, err
	default:
		wpargs = a
		wpnewf = n
	}

	var pnewf newPhaseMergerFunc

	switch a, n, err := wpargs.NewPhaseMergerFuncs(); {
	case err != nil:
		return nil, nil, nil, err
	default:
		pargs = a
		pnewf = n
	}

	extractKeys, closeExtractKeys := newExtractStateMergeValueKeysFunc(int(args.MaxOpsSize))
	pargs.ExtractStateMergeValueKeysFunc = extractKeys

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

	return pargs, wpargs, closeExtractKeys, nil
}

func newSetStateOpFuncs(mapSize uint64) (
	setStateOpFunc,
	stateOpsFuncs,
	error,
) {
	var m util.LockedMap[string, map[uint64]int]

	switch i, err := util.NewShardedMap[string, map[uint64]int](mapSize, nil); {
	case err != nil:
		return nil, nil, err
	default:
		m = i
	}

	return func(opindex uint64, stateKeys []string, x xMergeFlag) {
			_ = util.TraverseSliceV1(stateKeys, func(_ int, stateKey string) (bool, error) {
				_, _, _ = m.Set(stateKey, func(prev map[uint64]int, found bool) (map[uint64]int, error) {
					c := 1
					if x == unmergeFlag {
						c = -1
					}

					if !found {
						return map[uint64]int{opindex: c}, nil
					}

					prev[opindex] += c

					return prev, nil
				})

				return true, nil
			})
		},
		func() (getStateOpsFunc, func()) {
			return func(stateKey string) (ops []uint64) {
					var mops map[uint64]int

					switch i, found := m.Value(stateKey); {
					case !found, len(i) < 1:
						return nil
					default:
						defer clear(i)

						mops = i
					}

					var add util.CompactAppendSliceAddFunc[uint64]
					var done util.CompactAppendSliceDoneFunc[uint64]

					switch a, d, err := util.CompactAppendSliceV1[uint64](uint64(len(mops))); {
					case err != nil:
						return nil
					default:
						defer d(nil)

						add = a
						done = d
					}

					_ = TraverseMap(mops, func(opindex uint64, count int) (bool, error) {
						if count > 0 {
							add(opindex)
						}

						return true, nil
					})

					_ = done(func(s []uint64) error {
						ops = slices.Clone(s)

						return nil
					})

					return ops
				},
				m.Close
		},
		nil
}
