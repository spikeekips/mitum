package main

import (
	"context"
	"slices"

	"github.com/spikeekips/mitum/base"
	"github.com/spikeekips/mitum/util"
	"github.com/spikeekips/mitum/util/logging"
)

type (
	getStateFunc                                  func(stateKey string) (base.State, bool, error)
	statesWorkerMergerTraverseMergersNewStateFunc func(stateKey string, _ base.StateValue, ops []uint64) error
	statesWorkerMergerTraverseMergers             func(statesWorkerMergerTraverseMergersNewStateFunc) error
	statesWorkerMergerResultFunc                  func(
		unmerged []uint64,
		reasons map[uint64]string,
		_ statesWorkerMergerTraverseMergers,
	) error
	statesWorkerMergerWaitFunc func(statesWorkerMergerResultFunc) error
	statesWorkerMergerFunc     func(
		_ context.Context,
		minEnough,
		maxOps uint64,
		_ workerBroadcastPhaseDoneFunc,
		_ getStateFunc,
	) (
		workerInitialInsertOpsFuncs,
		workerMergerPhaseDoneFunc,
		statesWorkerMergerWaitFunc,
		error,
	)
)

type statesWorkerMergerArgs struct {
	*logging.Logging
	NewJobWorker newJobWorkerFunc
}

func newStatesWorkerMergerArgs() *statesWorkerMergerArgs {
	return &statesWorkerMergerArgs{
		Logging:      NewModuleLogging("states-worker-merger", nil),
		NewJobWorker: newFuncBaseJobWorker(1 << 6),
	}
}

func newFuncStatesWorkerMerger(
	args *statesWorkerMergerArgs,
	iargs *initialValuesWorkerMergerArgs,
	pargs *phaseMergerArgs,
) statesWorkerMergerFunc {
	newInitialValuesWorkerMergerf := newFuncInitialValuesWorkerMerger(iargs, pargs)

	return func(
		ctx context.Context,
		minEnough, maxOps uint64,
		broadcastPhaseDone workerBroadcastPhaseDoneFunc,
		getState getStateFunc,
	) (
		workerInitialInsertOpsFuncs,
		workerMergerPhaseDoneFunc,
		statesWorkerMergerWaitFunc,
		error,
	) {
		wctx, wctxCancel := contextWithCancelCause(ctx)

		var mergersPool util.LockedMap[string, StateMerger]

		// NOTE maximum number of states approximately max ops * 2.
		switch i, err := util.NewShardedMap[string, StateMerger](maxOps*2, nil); {
		case err != nil:
			return nil, nil, nil, err
		default:
			mergersPool = i
		}

		var stateOps stateOpsFuncs
		var setStateOp setStateOpFunc

		switch s, o, err := newSetStateOpFuncsWithLogger(maxOps); {
		case err != nil:
			return nil, nil, nil, err
		default:
			setStateOp = s
			stateOps = o
		}

		getMergers, closeMergers := newGetMergers(mergersPool, getState)

		closef := func(err error) error {
			wctxCancel(err)
			closeMergers()
			mergersPool.Close()

			return err
		}

		var initialValuesInsert workerInitialInsertOpsFuncs
		var initialValuesPhaseDone workerMergerPhaseDoneFunc
		var initialValuesWait initialValuesWorkerMergerWaitFunc

		switch i, p, w, err := newInitialValuesWorkerMergerf(wctx, minEnough, maxOps, getMergers, setStateOp, broadcastPhaseDone); {
		case err != nil:
			return nil, nil, nil, err
		default:
			initialValuesInsert = func(worker string) (
				workerInitialInsertOpFunc,
				func() error,
				error,
			) {
				switch j, d, err := i(worker); {
				case err != nil:
					args.Log().Error().Err(err).Msg("worker initial insert error")

					return nil, nil, closef(err)
				default:
					return j, d, nil
				}
			}
			initialValuesPhaseDone = func(worker string, unmerge []uint64) error {
				err := p(worker, unmerge)
				if err != nil {
					args.Log().Error().Err(err).Str("worker", worker).Msg("worker phase done error")
				}

				return err
			}
			initialValuesWait = w
		}

		return initialValuesInsert,
			func(worker string, unmerge []uint64) error {
				return contextCauseError(wctx, ifErrErrorFunc(closef), func(context.Context) error {
					return initialValuesPhaseDone(worker, unmerge)
				})
			},
			func(f statesWorkerMergerResultFunc) error {
				return contextCauseError(wctx, closef, func(context.Context) error {
					return initialValuesWait(func(unmerged []uint64, reasons map[uint64]string) error {
						if f == nil {
							return nil
						}

						return f(unmerged, reasons, newTraverseMergers(wctx, mergersPool, stateOps, args.NewJobWorker))
					})
				})
			},
			nil
	}
}

func newSetStateOpFuncsWithLogger(maxOpsSize uint64) (
	setStateOpFunc,
	stateOpsFuncs,
	error,
) {
	var m util.LockedMap[string, map[uint64] /* {phase, count} */ [2]int]

	switch i, err := util.NewShardedMap[string, map[uint64][2]int](maxOpsSize, nil); {
	case err != nil:
		return nil, nil, err
	default:
		m = i
	}

	return func(phase int, opindex uint64, stateKeys []string, x xMergeFlag) {
			_ = util.TraverseSliceV1(stateKeys, func(_ int, stateKey string) (bool, error) {
				_, _, _ = m.Set(stateKey, func(prev map[uint64][2]int, found bool) (map[uint64][2]int, error) {
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

					return prev, nil
				})

				return true, nil
			})
		},
		func() (getStateOpsFunc, func()) {
			return func(stateKey string) (ops []uint64) {
					_ = m.Get(stateKey, func(mops map[uint64][2]int, found bool) error {
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

func newTraverseMergers(
	ctx context.Context,
	mergersPool util.LockedMap[string, StateMerger],
	stateOps stateOpsFuncs,
	newJobWorker newJobWorkerFunc,
) statesWorkerMergerTraverseMergers {
	return func(f statesWorkerMergerTraverseMergersNewStateFunc) error {
		var jw JobWorker

		switch i, err := newJobWorker(ctx); {
		case err != nil:
			return err
		default:
			defer i.Close()

			jw = i
		}

		getStateOp, closeStateOps := stateOps()
		defer closeStateOps()

		var gerr error

		_ = mergersPool.Traverse(func(stateKey string, merger StateMerger) bool {
			gerr = jw.NewJob(func(context.Context) error {
				ops := getStateOp(stateKey)
				if len(ops) < 1 {
					return nil
				}

				switch stv, err := merger.Close(); {
				case err != nil:
					return err
				case stv == nil:
					return nil
				default:
					return f(stateKey, stv, ops)
				}
			})

			return gerr == nil
		})

		if gerr != nil {
			return gerr
		}

		jw.JobDone()

		return jw.Wait()
	}
}
