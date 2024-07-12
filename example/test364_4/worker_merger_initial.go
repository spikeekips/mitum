package main

import (
	"context"
	"fmt"
	"slices"

	"github.com/pkg/errors"
	"github.com/rs/zerolog"
	"github.com/spikeekips/mitum/util"
	"github.com/spikeekips/mitum/util/logging"
)

type (
	workerBroadcastPhaseDoneFunc func(_ context.Context, newUnmerged []uint64) error
	workerInitialInsertOpFunc    func(opindex uint64, _ []StateMergeValue) error
	workerInitialInsertOpsFuncs  func(worker string) (
		workerInitialInsertOpFunc,
		func() error,
		error,
	)
	workerNewInitialXMergeFuncs func(worker string) (
		phaseMergerXMergeFunc,
		func() error,
		error,
	)
	initialValuesWorkerMergerResultFunc func(unmerged []uint64, reasons map[uint64]string) error
	initialValuesWorkerMergerWaitFunc   func(initialValuesWorkerMergerResultFunc) error
	setStateOpFunc                      func(phase int, opindex uint64, stateKeys []string, _ xMergeFlag)
	getStateOpsFunc                     func(stateKey string) (ops []uint64)
	stateOpsFuncs                       func() (getStateOpsFunc, func())
)

type initialValuesWorkerMergerArgs struct {
	*logging.Logging
	*baseWorkerMergerArgs
	BroadcastPhaseDone  workerBroadcastPhaseDoneFunc
	NewValuesPool       func() (util.LockedMap[uint64, []StateMergeValue], error)
	SetStateOpFunc      func() (setStateOpFunc, error)
	MaxOpsSize          uint64
	MaxJobWorkerSemSize uint64
}

func (args *initialValuesWorkerMergerArgs) SetLogging(l *logging.Logging) *logging.Logging {
	args.baseWorkerMergerArgs.SetLogging(l)

	return args.Logging.SetLogging(l)
}

func newInitialValuesWorkerMergerArgs(args *baseWorkerMergerArgs) *initialValuesWorkerMergerArgs {
	return &initialValuesWorkerMergerArgs{
		Logging: logging.NewLogging(func(zctx zerolog.Context) zerolog.Context {
			return zctx.Str("module", "initial-worker-merger")
		}),
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
	workerInitialInsertOpsFuncs,
	workerMergerPhaseDoneFunc,
	initialValuesWorkerMergerWaitFunc,
	error,
) {
	wctx, wctxCancel := context.WithCancelCause(ctx)

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

	var initialWorkerInsert workerNewInitialXMergeFuncs
	var initialWait initialValuesWorkerMergerWaitFunc

	switch i, w, err := newInitialPhaseMerger(wctx, args, pargs, enoughWorkers); {
	case err != nil:
		return nil, nil, nil, err
	default:
		initialWorkerInsert = i
		initialWait = w
	}

	var workerPhaseDone workerMergerPhaseDoneFunc
	var workerMergerWait workerMergerWaitFunc

	switch i, w, err := newBaseWorkerMerger(wctx, args.baseWorkerMergerArgs); {
	case err != nil:
		return nil, nil, nil, err
	default:
		workerPhaseDone = i
		workerMergerWait = w
	}

	var setStateOp func(int, uint64, []StateMergeValue, xMergeFlag)

	switch i, err := args.SetStateOpFunc(); {
	case err != nil:
		return nil, nil, nil, err
	default:
		setStateOp = func(phase int, opindex uint64, values []StateMergeValue, x xMergeFlag) {
			stateKeys := pargs.ExtractStateMergeValueKeysFunc(opindex, values)
			if len(stateKeys) < 1 { // FIXME remove
				keys := extractStateMergeValueKeys(values)
				panic(fmt.Errorf("empty state keys: local=%q opindex=%d state keys=%v", args.local, opindex, keys))
			}

			i(phase, opindex, stateKeys, x)
		}
	}

	_, initialStateOpsDone, initialStateOpsWait := NewDoneContextFunc(wctx)

	setInitialUnmergedState := func(unmerged []uint64) error {
		if len(unmerged) < 1 {
			return nil
		}

		jw, _ := NewBaseJobWorker(wctx, int64(args.MaxJobWorkerSemSize))

		if err := util.TraverseSliceV1(unmerged, func(_ int, opindex uint64) (bool, error) {
			return true, jw.NewJob(func(context.Context) error {
				if values, found := vp.Value(opindex); found {
					setStateOp(1, opindex, values, unmergeFlag)
				}

				return nil
			})
		}); err != nil {
			return err
		}

		jw.JobDone()

		return jw.Wait()
	}

	initialInsertOps := func(worker string) (
		workerInitialInsertOpFunc,
		func() error,
		error,
	) {
		var insert phaseMergerXMergeFunc
		var done func() error

		switch i, d, err := initialWorkerInsert(worker); {
		case err != nil:
			return nil, nil, err
		case i == nil:
			return nil, nil, nil
		default:
			insert = i
			done = d
		}

		fmt.Printf("initial funcs: local=%q worker=%q\n", args.local, worker)

		return func(opindex uint64, values []StateMergeValue) error {
				stateKeys := extractStateMergeValueKeys(values) // FIXME remove
				fmt.Printf("initial: > local=%q worker=%q opindex=%d state keys=%v\n", args.local, worker, opindex, stateKeys)
				// switch _, created, err := vp.Set(opindex, func(_ []StateMergeValue, found bool) ([]StateMergeValue, error) {
				//	if found {
				//		return nil, util.ErrLockedSetIgnore
				//	}

				//	return values, nil
				//}); {
				//case err != nil:
				//	fmt.Printf("initial: < local=%q worker=%q opindex=%d state keys=%v err=%+v\n", args.local, worker, opindex, stateKeys, err)

				//	return nil
				// case created:
				//	fmt.Printf("initial: < local=%q worker=%q opindex=%d state keys=%v\n", args.local, worker, opindex, stateKeys)

				//	setStateOp(opindex, values, mergeFlag)
				//default:
				//	fmt.Printf("initial: < local=%q worker=%q opindex=%d state keys=%v not created\n", args.local, worker, opindex, stateKeys)
				//}

				switch added := vp.SetValue(opindex, values); {
				case added:
					setStateOp(0, opindex, values, mergeFlag)

					fmt.Printf("initial: < local=%q worker=%q opindex=%d state keys=%v\n", args.local, worker, opindex, stateKeys)
				default:
					setStateOp(0, opindex, values, mergeFlag) // FIXME review

					fmt.Printf("initial: < local=%q worker=%q opindex=%d state keys=%v not created\n", args.local, worker, opindex, stateKeys)
				}

				return insert(opindex)
			},
			done,
			nil
	}

	waitf := func(f initialValuesWorkerMergerResultFunc) error {
		defer func() {
			vp.Close()
			closeArgs()
		}()

		var reasons map[uint64]string

		if err := initialWait(func(unmerged []uint64, r map[uint64]string) error {
			unmerged = slices.Clone(unmerged)

			reasons = r

			if err := args.BroadcastPhaseDone(wctx, unmerged); err != nil {
				return err
			}

			go initialStateOpsDone(setInitialUnmergedState(unmerged))

			return workerPhaseDone(localWorkerName, unmerged)
		}); err != nil {
			return err
		}

		return workerMergerWait(func(unmerged []uint64) error {
			if err := initialStateOpsWait(); err != nil {
				return err
			}

			if err := setInitialUnmergedState(unmerged); err != nil {
				return err
			}

			if f == nil {
				return nil
			}

			return f(unmerged, reasons)
		})
	}

	return func(worker string) (
			workerInitialInsertOpFunc,
			func() error,
			error,
		) {
			i, d, err := initialInsertOps(worker)

			switch {
			case errors.Is(err, util.ErrJobWorkerDone):
			case err != nil:
				fmt.Printf("initial phase merger.insert: local=%q err=%+v\n", args.local, err)
				wctxCancel(err)
			}

			return i, d, err
		},
		workerPhaseDone,
		OnceFuncCallback(func(f initialValuesWorkerMergerResultFunc) error {
			err := waitf(f)
			wctxCancel(err)

			return err
		}),
		nil
}

func newInitialPhaseMerger(
	ctx context.Context,
	args *initialValuesWorkerMergerArgs,
	pargs *PhaseMergerArgs,
	enoughWorkers uint64,
) (
	_ workerNewInitialXMergeFuncs,
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
			fmt.Printf("initial wait: local=%q\n", args.local)
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
						fmt.Printf("ipm: local=%q worker=%q enough=%v ok=%v\n", args.local, worker, enough, ok)
						switch {
						case !ok:
						case enough:
							initialDone()
							fmt.Printf("ipm: local=%q worker=%q enough=%v ok=%v done\n", args.local, worker, enough, ok)
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

	wpargs.SetLogging(args.Logging)

	var pnewf newPhaseMergerFunc

	switch a, n, err := wpargs.NewPhaseMergerFuncs(); {
	case err != nil:
		return nil, nil, nil, err
	default:
		pargs = a
		pnewf = n
	}

	pargs.local = args.local
	wpargs.SetLogging(args.Logging)

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

func newSetStateOpFuncs(local string, mapSize uint64) (
	setStateOpFunc,
	stateOpsFuncs,
	error,
) {
	fmt.Printf("new set state op func: local=%q\n", local)
	var m util.LockedMap[string, map[uint64] /* {phase, count} */ [2]int]

	switch i, err := util.NewShardedMap[string, map[uint64][2]int](mapSize, nil); {
	case err != nil:
		return nil, nil, err
	default:
		m = i
	}

	return func(phase int, opindex uint64, stateKeys []string, x xMergeFlag) {
			fmt.Printf("set state keys op: local=%q phase=%d opindex=%d state keys=%v x=%v p=%p >\n", local, phase, opindex, stateKeys, x, m)
			err := util.TraverseSliceV1(stateKeys, func(_ int, stateKey string) (bool, error) {
				_, _, err := m.Set(stateKey, func(prev map[uint64][2]int, found bool) (map[uint64][2]int, error) {
					c := 1
					if x == unmergeFlag {
						c = -1
					}

					switch {
					case !found:
						prev = map[uint64][2]int{opindex: {phase, c}}
					default:
						switch v, opfound := prev[opindex]; {
						case !opfound:
							prev[opindex] = [2]int{phase, c}
						case phase <= v[0]:
							return nil, util.ErrLockedSetIgnore
						default:
							prev[opindex] = [2]int{phase, v[1] + c}
						}
					}

					fmt.Printf("set state op: local=%q phase=%d opindex=%d state key=%q x=%v c=%d v=%v p=%p\n", local, phase, opindex, stateKey, x, c, prev, m)

					return prev, nil
				})

				return true, err
			})

			fmt.Printf("set state keys op: local=%q phase=%d opindex=%d state keys=%v x=%v p=%p < err=%+v\n", local, phase, opindex, stateKeys, x, m, err)
		},
		func() (getStateOpsFunc, func()) {
			return func(stateKey string) (ops []uint64) {
					_ = m.Get(stateKey, func(mops map[uint64][2]int, found bool) error {
						fmt.Printf("get state op: local=%q state key=%q ops=%v found=%v\n", local, stateKey, mops, found)

						if !found || len(mops) < 1 {
							return nil
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

						_ = TraverseMap(mops, func(opindex uint64, v [2]int) (bool, error) {
							if v[1] > 0 {
								add(opindex)
							}

							return true, nil
						})

						_ = done(func(s []uint64) error {
							ops = slices.Clone(s)

							return nil
						})

						return nil
					})

					return ops
				},
				m.Close
		},
		nil
}
