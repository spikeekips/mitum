package main

import (
	"context"
	"fmt"
	"slices"

	"github.com/pkg/errors"
	"github.com/rs/zerolog"
	"github.com/spikeekips/mitum/util"
	"github.com/spikeekips/mitum/util/logging"
	"golang.org/x/sync/singleflight"
)

type (
	phaseMergerXMergeFunc func(opindex uint64) error
	phaseMergerGetMergers func(
		[]string,
		func(func(StateMergeValue) (StateMerger, error)) error,
	) error
	phaseMergerWaitCallbackFunc func(newMerged, newUnmerged []uint64, reasons map[uint64]string) error
	phaseMergerWaitFunc         func(phaseMergerWaitCallbackFunc) error
	phaseMergerGetValuesFunc    func(opindex uint64) ([]StateMergeValue, error)
)

type PhaseMergerArgs struct {
	*logging.Logging
	NewPhaseJobWorkerFunc          NewPhaseJobWorkerFunc
	GetMergersFunc                 phaseMergerGetMergers
	GetValuesFunc                  phaseMergerGetValuesFunc
	ExtractStateMergeValueKeysFunc func(opindex uint64, values []StateMergeValue) []string
	XMergedOpsMapSize              uint64
	ReasonsMapSize                 uint64
	local                          string
}

func NewPhaseMergerArgs(local string) *PhaseMergerArgs {
	args := &PhaseMergerArgs{
		Logging: logging.NewLogging(func(zctx zerolog.Context) zerolog.Context {
			return zctx.Str("module", "phase-merger").Str("local", local)
		}),
		GetMergersFunc: func(
			[]string,
			func(func(StateMergeValue) (StateMerger, error)) error,
		) error {
			return util.ErrNotImplemented.Errorf("GetMergersFunc")
		},
		GetValuesFunc: func(uint64) ([]StateMergeValue, error) {
			return nil, util.ErrNotImplemented.Errorf("GetValuesFunc")
		},
		ExtractStateMergeValueKeysFunc: func(opindex uint64, values []StateMergeValue) []string {
			panic("ExtractStateMergeValueKeys")

			return nil
		},
		XMergedOpsMapSize: 1 << 9,
		ReasonsMapSize:    1 << 9,
		local:             local,
	}

	args.NewPhaseJobWorkerFunc = func(ctx context.Context) (
		PhaseJobWorkerNewJobFunc,
		func(),
		func() error,
		error,
	) {
		cargs := newConcPhaseJobWorkerArgs(args.local)

		return NewConcPhaseJobWorker(ctx, cargs)
	}

	return args
}

func NewPhaseMerger(ctx context.Context, args *PhaseMergerArgs) (
	merge, unmerge phaseMergerXMergeFunc,
	done func(),
	wait phaseMergerWaitFunc,
	_ error,
) {
	pctx, pctxCancel := context.WithCancelCause(ctx)

	var addXMergedOp func(phase int, opindex uint64, _ xMergeFlag)
	var doneXMergedOps func(func(mergedOps, unmergedOps []uint64) error) error

	switch a, d, err := isXMergedOpsFuncs(args.local, args.XMergedOpsMapSize); {
	case err != nil:
		return nil, nil, nil, nil, err
	default:
		addXMergedOp = a
		doneXMergedOps = d
	}

	var newJob PhaseJobWorkerNewJobFunc
	var jobDone func()
	var jobWait func() error

	switch n, d, w, err := args.NewPhaseJobWorkerFunc(pctx); {
	case err != nil:
		return nil, nil, nil, nil, err
	default:
		newJob = func(jobID uint64, c PhaseJobWorkerCallback) error {
			switch err := n(jobID, c); {
			case errors.Is(err, util.ErrJobWorkerDone):
				return err
			case err != nil:
				pctxCancel(err)

				return err
			default:
				return nil
			}
		}
		jobDone = d
		jobWait = w
	}

	addReason, doneReason := newAddReasonsFuncs(args.ReasonsMapSize)

	mergeCallback, unmergeCallback := newXMergeCallbackFuncs(args, addXMergedOp, addReason)

	waitf := func(f phaseMergerWaitCallbackFunc) error {
		if err := jobWait(); err != nil {
			return err
		}

		return doneXMergedOps(func(newMerged, newUnmerged []uint64) error {
			if f == nil {
				return nil
			}

			return f(newMerged, newUnmerged, doneReason(newUnmerged))
		})
	}

	return func(opindex uint64) error {
			addXMergedOp(-1, opindex, mergeFlag)

			switch err := newJob(opindex, mergeCallback(opindex)); {
			case errors.Is(err, util.ErrJobWorkerDone):
				return nil
			default:
				return err
			}
		},
		func(opindex uint64) error {
			addXMergedOp(-1, opindex, unmergeFlag)

			switch err := newJob(opindex, unmergeCallback(opindex)); {
			case errors.Is(err, util.ErrJobWorkerDone):
				return nil
			default:
				return err
			}
		},
		jobDone,
		OnceFuncCallback(func(f phaseMergerWaitCallbackFunc) error {
			err := waitf(f)
			pctxCancel(err)

			return err
		}),
		nil
}

func newXMergeCallbackFuncs(
	args *PhaseMergerArgs,
	addXMergedOp func(int, uint64, xMergeFlag),
	addReason func(reasons map[uint64]string) error,
) (
	merge func(opindex uint64) PhaseJobWorkerCallback,
	unmerge func(opindex uint64) PhaseJobWorkerCallback,
) {
	var mergeCallback func(opindex uint64) PhaseJobWorkerCallback
	var unmergeCallback func(opindex uint64) PhaseJobWorkerCallback

	mergeCallback = func(opindex uint64) PhaseJobWorkerCallback {
		var values []StateMergeValue

		switch i, err := args.GetValuesFunc(opindex); {
		case err != nil:
			return func(context.Context, int, PhaseJobWorkerNewJobFunc) error {
				panic(fmt.Errorf("worker=%q opindex=%d err=%w", args.local, opindex, err))

				return err
			}
		default:
			values = i
		}

		stateKeys := args.ExtractStateMergeValueKeysFunc(opindex, values)
		if len(stateKeys) < 1 {
			panic(fmt.Errorf("xmerge: local=%q state keys=%v", args.local, extractStateValuesKeys(values)))
		}

		l := args.Log().With().
			Uint64("opindex", opindex).
			Strs("state_keys", stateKeys).
			Logger()

		return func(ctx context.Context, phase int, j PhaseJobWorkerNewJobFunc) error {
			return args.GetMergersFunc(
				stateKeys,
				func(getMerger func(StateMergeValue) (StateMerger, error)) error {
					ll := l.With().Int("phase", phase).Logger()
					ll.Debug().Msg("> merge")

					switch committed, nexts, reasons, err := mergeOp(
						ctx,
						args.local,
						opindex,
						values,
						getMerger,
					); {
					case err != nil:
						panic(fmt.Errorf("mergeop: local=%q err=%w", args.local, err))
						return err
					default:
						ll.Debug().
							Uints64("nexts", nexts).
							Interface("reasons", reasons).
							Bool("is_committed", committed).
							Msg("< merge")

						if committed {
							addXMergedOp(phase, opindex, mergeFlag)
						} else {
							addXMergedOp(phase, opindex, unmergeFlag)
						}

						if err := addReason(reasons); err != nil {
							return err
						}

						return util.TraverseSliceV1(nexts, func(_ int, next uint64) (bool, error) {
							return true, j(next, unmergeCallback(next))
						})
					}
				},
			)
		}
	}

	unmergeCallback = func(opindex uint64) PhaseJobWorkerCallback {
		var values []StateMergeValue

		switch i, err := args.GetValuesFunc(opindex); {
		case err != nil:
			return func(context.Context, int, PhaseJobWorkerNewJobFunc) error {
				return err
			}
		default:
			values = i
		}

		stateKeys := args.ExtractStateMergeValueKeysFunc(opindex, values)

		l := args.Log().With().
			Uint64("opindex", opindex).
			Strs("state_keys", stateKeys).
			Logger()

		return func(ctx context.Context, phase int, j PhaseJobWorkerNewJobFunc) error {
			return args.GetMergersFunc(
				stateKeys,
				func(getMerger func(StateMergeValue) (StateMerger, error)) error {
					ll := l.With().Int("phase", phase).Logger()
					ll.Debug().Msg("> unmerge")

					switch committed, nexts, err := unmergeOp(
						ctx,
						args.local,
						opindex,
						values,
						getMerger,
					); {
					case err != nil:
						return err
					default:
						ll.Debug().
							Uints64("nexts", nexts).
							Bool("is_committed", committed).
							Msg("< unmerge")

						if committed {
							addXMergedOp(phase, opindex, unmergeFlag)
						}

						return util.TraverseSliceV1(nexts, func(_ int, next uint64) (bool, error) {
							return true, j(next, mergeCallback(next))
						})
					}
				},
			)
		}
	}

	return mergeCallback, unmergeCallback
}

func extractStateMergeValueKeys(values []StateMergeValue) (stateKeys []string) {
	if len(values) < 2 {
		return []string{values[0].Key()}
	}

	add, done, _ := UniqueCompactAppendSliceV1[string](uint64(len(values)))

	_ = util.TraverseSliceV1(values, func(_ int, value StateMergeValue) (bool, error) {
		add(value.Key())

		return true, nil
	})

	_ = done(func(s []string) error {
		stateKeys = slices.Clone(s)

		return nil
	})

	return stateKeys
}

func newExtractStateMergeValueKeysFunc(cacheSize int) (
	func(uint64, []StateMergeValue) []string,
	func(),
) {
	cache := util.NewLFUGCache[uint64, []string](cacheSize)

	var sg singleflight.Group

	return func(opindex uint64, values []StateMergeValue) []string {
			stateKeys, _, _ := util.SingleflightDo[[]string](
				&sg,
				string(util.Uint64ToBytes(opindex)),
				func() (stateKeys []string, _ error) {
					switch i, found := cache.Get(opindex); {
					case found:
						stateKeys = i
					default:
						stateKeys = extractStateMergeValueKeys(values)

						cache.Set(opindex, stateKeys, 0)
					}

					return stateKeys, nil
				},
			)

			return stateKeys
		},
		cache.Close
}

func newGetMergersFunc(
	m util.LockedMap[string, StateMerger],
	getState getStateFunc,
) (
	phaseMergerGetMergers,
	func(),
) {
	mmergers := util.NewMultiKeysLockedMap[string, StateMerger](m)

	return func(
			stateKeys []string,
			f func(func(StateMergeValue) (StateMerger, error)) error,
		) error {
			return mmergers.LockFunc(
				stateKeys,
				func(m util.LockedMap[string, StateMerger]) error {
					return f(getOrCreateMergerFunc(m, getState))
				},
			)
		},
		mmergers.Close
}

type xMergeFlag string

const (
	mergeFlag   xMergeFlag = "merge"
	unmergeFlag xMergeFlag = "unmerge"
)

func isXMergedOpsFuncs(
	local string,
	mapSize uint64,
) (
	func(phase int, opindex uint64, _ xMergeFlag),
	func(func(mergedOps, unmergedOps []uint64) error) error,
	error,
) {
	var baseMergedOps util.LockedMap[uint64, xMergeFlag]

	switch i, err := util.NewShardedMap[uint64, xMergeFlag](mapSize, nil); {
	case err != nil:
		return nil, nil, err
	default:
		baseMergedOps = i
	}

	var ops util.LockedMap[ /* opindex */ uint64 /* count */, int]

	switch i, err := util.NewShardedMap[uint64, int](mapSize, nil); {
	case err != nil:
		return nil, nil, err
	default:
		ops = i
	}

	return func(phase int, opindex uint64, x xMergeFlag) {
			switch {
			case phase < 0:
				baseMergedOps.SetValue(opindex, x)

				return
			case phase == 0:
				if y, _ := baseMergedOps.Value(opindex); x == y {
					return
				}
			}

			_, _, _ = ops.Set(opindex, func(prev int, found bool) (int, error) {
				var i int = 1

				if x == unmergeFlag {
					i = -1
				}

				prev += i

				return prev, nil
			})
		},
		func(f func([]uint64, []uint64) error) error {
			defer func() {
				baseMergedOps.Close()
				ops.Close()
			}()

			if f == nil {
				return nil
			}

			var addM, addU util.CompactAppendSliceAddFunc[uint64]
			var doneM, doneU util.CompactAppendSliceDoneFunc[uint64]

			switch a, d, err := util.CompactAppendSliceV1[uint64](mapSize, mapSize); {
			case err != nil:
				return err
			default:
				defer d(nil)

				addM = a
				doneM = d
			}

			switch a, d, err := util.CompactAppendSliceV1[uint64](mapSize, mapSize); {
			case err != nil:
				return err
			default:
				defer d(nil)

				addU = a
				doneU = d
			}

			_ = ops.Traverse(func(opindex uint64, i int) bool {
				switch {
				case i == 0:
				case i > 0:
					if x, _ := baseMergedOps.Value(opindex); x != mergeFlag {
						addM(opindex)
					}
				default:
					if x, _ := baseMergedOps.Value(opindex); x != unmergeFlag {
						addU(opindex)
					}
				}

				return true
			})

			return doneM(func(merged []uint64) error {
				return doneU(func(unmerged []uint64) error {
					return f(merged, unmerged)
				})
			})
		},
		nil
}

func newAddReasonsFuncs(mapSize uint64) (
	add func(reasons map[uint64]string) error,
	done func(unmergedOps []uint64) map[uint64]string,
) {
	var m util.LockedMap[uint64, string]

	newMap := OnceFunc(func() error {
		switch i, err := util.NewShardedMap[uint64, string](mapSize, nil); {
		case err != nil:
			return err
		default:
			m = i

			return nil
		}
	})

	return func(reasons map[uint64]string) error {
			if len(reasons) < 1 {
				return nil
			}

			if err := newMap(); err != nil {
				return err
			}

			for i := range reasons {
				r := reasons[i]

				_, _, _ = m.Set(i, func(prev string, _ bool) (string, error) {
					switch {
					case prev == r:
						return "", util.ErrLockedSetIgnore
					default:
						return r, nil
					}
				})
			}

			return nil
		},
		func(unmergedOps []uint64) map[uint64]string {
			if m == nil {
				return nil
			}

			defer m.Close()

			if len(unmergedOps) < 1 {
				return nil
			}

			r := m.Map()

			for i := range r {
				if slices.Index(unmergedOps, i) < 0 {
					delete(r, i)
				}
			}

			return r
		}
}
