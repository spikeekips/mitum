package main

import (
	"context"
	"slices"

	"github.com/pkg/errors"
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
	initialValuesWorkerMergerFunc       func(
		_ context.Context,
		minEnough,
		maxOps uint64,
		_ phaseMergerGetMergers,
		_ setStateOpFunc,
		_ workerBroadcastPhaseDoneFunc,
	) (
		workerInitialInsertOpsFuncs,
		workerMergerPhaseDoneFunc,
		initialValuesWorkerMergerWaitFunc,
		error,
	)
)

type initialValuesWorkerMergerArgs struct {
	*logging.Logging
	*baseWorkerMergerArgs
	NewValuesPool func() (util.LockedMap[uint64, []StateMergeValue], error)
}

func (args *initialValuesWorkerMergerArgs) SetLogging(l *logging.Logging) *logging.Logging {
	args.baseWorkerMergerArgs.SetLogging(l)

	return args.Logging.SetLogging(l)
}

func newInitialValuesWorkerMergerArgs(args *baseWorkerMergerArgs) *initialValuesWorkerMergerArgs {
	return &initialValuesWorkerMergerArgs{
		Logging:              NewModuleLogging("initial-worker-merger", nil),
		baseWorkerMergerArgs: args,
		NewValuesPool: func() (util.LockedMap[uint64, []StateMergeValue], error) {
			return nil, util.ErrNotImplemented.Errorf("NewValuesPool")
		},
	}
}

func newFuncInitialValuesWorkerMerger(
	args *initialValuesWorkerMergerArgs,
	wpargs *workerPhaseMergerArgs,
	pargs *phaseMergerArgs,
) initialValuesWorkerMergerFunc {
	newBaseWorkerMergerf := newFuncBaseWorkerMerger(args.baseWorkerMergerArgs, wpargs, pargs)

	return func(
		ctx context.Context,
		minEnough, maxOps uint64,
		getMergers phaseMergerGetMergers,
		setStateOp setStateOpFunc,
		broadcastPhaseDone workerBroadcastPhaseDoneFunc,
	) (
		workerInitialInsertOpsFuncs,
		workerMergerPhaseDoneFunc,
		initialValuesWorkerMergerWaitFunc,
		error,
	) {
		if maxOps < 1 {
			return nil, nil, nil, errors.Errorf("zero max ops")
		}

		if broadcastPhaseDone == nil {
			broadcastPhaseDone = func(context.Context, []uint64) error { return nil }
		}

		wctx, wctxCancel := ContextWithCancelCause(ctx)

		var vp util.LockedMap[uint64, []StateMergeValue]

		switch i, err := args.NewValuesPool(); {
		case err != nil:
			return nil, nil, nil, err
		default:
			vp = i
		}

		extractStateMergeValueKeys, closeExtractStateMergeValueKeys := newExtractStateMergeValueKeys(int(maxOps))

		getStateMergeValues := getStateMergeValuesFuncFromPool(vp)

		var initialWorkerInsert workerNewInitialXMergeFuncs
		var initialWait initialValuesWorkerMergerWaitFunc

		switch i, w, err := newInitialPhaseMerger(
			wctx,
			args,
			pargs,
			minEnough,
			getStateMergeValues,
			getMergers,
			extractStateMergeValueKeys,
		); {
		case err != nil:
			return nil, nil, nil, err
		default:
			initialWorkerInsert = i
			initialWait = w
		}

		var workerPhaseDone workerMergerPhaseDoneFunc
		var workerMergerWait workerMergerWaitFunc

		switch i, w, err := newBaseWorkerMergerf(
			wctx,
			minEnough,
			getStateMergeValues,
			getMergers,
			extractStateMergeValueKeys,
			newWorkerMergerIsLocalOp(vp.Exists),
		); {
		case err != nil:
			return nil, nil, nil, err
		default:
			workerPhaseDone = i
			workerMergerWait = w
		}

		mSetStateOp := func(phase int, opindex uint64, values []StateMergeValue, x xMergeFlag) error {
			switch stateKeys, err := extractStateMergeValueKeys(opindex, values); {
			case err != nil:
				return err
			default:
				setStateOp(phase, opindex, stateKeys, x)

				return nil
			}
		}

		_, initialStateOpsDone, initialStateOpsWait := NewDoneContextFunc(wctx)

		setInitialUnmergedState := func(unmerged []uint64) error {
			if len(unmerged) < 1 {
				return nil
			}

			var jw JobWorker

			switch i, err := args.NewJobWorker(wctx); {
			case err != nil:
				return err
			default:
				defer i.Close()

				jw = i
			}

			if err := util.TraverseSliceV1(unmerged, func(_ int, opindex uint64) (bool, error) {
				return true, jw.NewJob(func(context.Context) error {
					if values, found := vp.Value(opindex); found {
						return mSetStateOp(1, opindex, values, unmergeFlag)
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

			return func(opindex uint64, values []StateMergeValue) error {
					l := args.Log().With().
						Str("worker", worker).
						Uint64("opindex", opindex).
						Strs("state_keys", extractStateMergeValueKeysFromValues(values)).
						Logger()

					l.Debug().Msg("initial insert ops")

					switch added := vp.SetValue(opindex, values); {
					case added:
						l.Debug().Msg("initial insert ops done")
					default:
						l.Debug().Msg("initial insert ops done, but known values")
					}

					mSetStateOp(0, opindex, values, mergeFlag)

					return insert(opindex)
				},
				done,
				nil
		}

		closef := func(err error) error {
			wctxCancel(err)
			vp.Close()
			closeExtractStateMergeValueKeys()

			return err
		}

		waitf := func(f initialValuesWorkerMergerResultFunc) error {
			defer func() {
				vp.Close()
				closeExtractStateMergeValueKeys()
			}()

			var reasons map[uint64]string

			if err := initialWait(func(unmerged []uint64, r map[uint64]string) error {
				unmerged = slices.Clone(unmerged)

				reasons = r

				if err := broadcastPhaseDone(wctx, unmerged); err != nil {
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
				if err != nil {
					args.Log().Error().
						Err(err).
						Str("worker", worker).
						Msg("initial phase merger error")

					return nil, nil, closef(err)
				}

				return i, d, err
			},
			func(worker string, unmerge []uint64) error {
				return contextCauseError(wctx, ifErrErrorFunc(closef), func(context.Context) error {
					return workerPhaseDone(worker, unmerge)
				})
			},
			func(f initialValuesWorkerMergerResultFunc) error {
				return contextCauseError(wctx, closef, func(context.Context) error {
					return waitf(f)
				})
			},
			nil
	}
}

func newInitialPhaseMerger(
	ctx context.Context,
	args *initialValuesWorkerMergerArgs,
	pargs *phaseMergerArgs,
	minEnough uint64,
	getStateMergeValues phaseMergerGetStateMergeValuesFunc,
	getMergers phaseMergerGetMergers,
	extractStateMergeValueKeys extractStateMergeValueKeysFunc,
) (
	_ workerNewInitialXMergeFuncs,
	wait initialValuesWorkerMergerWaitFunc,
	_ error,
) {
	wctx, wctxCancel := ContextWithCancelCause(ctx)

	var initialMerge phaseMergerXMergeFunc
	var initialDone func()
	var initialWait phaseMergerWaitFunc

	switch m, _, d, w, err := newPhaseMerger(wctx, pargs, getStateMergeValues, getMergers, extractStateMergeValueKeys); {
	case err != nil:
		return nil, nil, err
	default:
		initialMerge = m
		initialDone = d
		initialWait = w
	}

	var checkWorker func(string, bool, func(_, _ bool) error) error
	var closeCheckWorker func()

	switch c, d, err := newIsEnoughkWokerPhaseMergerFuncs(minEnough); {
	case err != nil:
		return nil, nil, err
	default:
		checkWorker = c
		closeCheckWorker = func() { d() }
	}

	initialMergeDone := func(worker string) error {
		return checkWorker(worker, false, func(enough, ok bool) error {
			args.Log().Debug().
				Str("worker", worker).
				Bool("enough", enough).
				Bool("ok", ok).
				Msg("worker phase done received")

			switch {
			case !ok:
			case enough:
				initialDone()
			}

			return nil
		})
	}

	closef := func(err error) error {
		wctxCancel(err)
		closeCheckWorker()

		return err
	}

	return func(worker string) (
			phaseMergerXMergeFunc,
			func() error,
			error,
		) {
			return initialMerge,
				func() error {
					return contextCauseError(wctx, ifErrErrorFunc(closef), func(context.Context) error {
						return initialMergeDone(worker)
					})
				},
				nil
		},
		func(f initialValuesWorkerMergerResultFunc) error {
			return contextCauseError(wctx, closef, func(context.Context) error {
				return initialWait(func(_, newUnmerged []uint64, reasons map[uint64]string) error {
					args.Log().Debug().
						Uints64("new_unmerged", newUnmerged).
						Interface("reasons", reasons).
						Msg("initial wait done")

					if f == nil {
						return nil
					}

					return f(newUnmerged, reasons)
				})
			})
		},
		nil
}

func getStateMergeValuesFuncFromPool(vp util.LockedMap[uint64, []StateMergeValue]) phaseMergerGetStateMergeValuesFunc {
	return func(opindex uint64) ([]StateMergeValue, error) {
		switch i, found := vp.Value(opindex); {
		case !found:
			return nil, util.ErrNotFound.Errorf("state merge values, opindex=%d", opindex)
		default:
			return i, nil
		}
	}
}
