package main

import (
	"context"
	"slices"
	"sync"

	"github.com/rs/zerolog"
	"github.com/spikeekips/mitum/base"
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
	phaseMergerWaitCallbackFunc func(newlyMergedOps, newlyUnmergedOps []uint64, reasons map[uint64]string) error
	phaseMergerWaitFunc         func(phaseMergerWaitCallbackFunc) error
)

type PhaseMergerArgs struct {
	*logging.Logging
	NewPhaseJobWorkerFunc          NewPhaseJobWorkerFunc
	GetMergersFunc                 phaseMergerGetMergers
	GetValuesFunc                  func(opindex uint64) ([]StateMergeValue, error)
	ExtractStateMergeValueKeysFunc func(opindex uint64, values []StateMergeValue) []string
	XBaseMergedOps                 util.LockedMap[uint64, xMergedFlag]
	XMergedOpsMapSize              uint64
	ReasonsMapSize                 uint64
}

func NewPhaseMergerArgs() *PhaseMergerArgs {
	return &PhaseMergerArgs{
		Logging: logging.NewLogging(func(zctx zerolog.Context) zerolog.Context {
			return zctx.Str("module", "phase-merger")
		}),
		NewPhaseJobWorkerFunc: func(ctx context.Context) (
			PhaseJobWorkerNewJobFunc,
			func(),
			func() error,
			error,
		) {
			return NewOneStepPhaseJobWorker(
				ctx,
				func(ctx context.Context) (JobWorker, error) {
					return NewBaseJobWorker(ctx, 1<<9)
				},
				1<<9,
			)
		},
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
	}
}

func NewPhaseMerger(ctx context.Context, args *PhaseMergerArgs) (
	merge, unmerge phaseMergerXMergeFunc,
	done func(),
	wait phaseMergerWaitFunc,
	_ error,
) {
	switch i, err := util.NewShardedMap[uint64, xMergedFlag](args.XMergedOpsMapSize, nil); {
	case err != nil:
		return nil, nil, nil, nil, err
	default:
		args.XBaseMergedOps = i
	}

	var addXMergedOp func(phase int, opindex uint64, _ xMergedFlag)
	var doneXMergedOps func(func(mergedOps, unmergedOps []uint64) error) error

	switch a, d, err := isXMergedOpsFuncs(args.XBaseMergedOps, args.XMergedOpsMapSize); {
	case err != nil:
		return nil, nil, nil, nil, err
	default:
		addXMergedOp = a
		doneXMergedOps = d
	}

	var newJob PhaseJobWorkerNewJobFunc
	var jobDone func()
	var jobWait func() error

	switch n, d, w, err := args.NewPhaseJobWorkerFunc(ctx); {
	case err != nil:
		return nil, nil, nil, nil, err
	default:
		newJob = n
		jobDone = d
		jobWait = w
	}

	addReason, doneReason := newUpdateReasonsFuncs(args.ReasonsMapSize)

	var mergeCallback func(opindex uint64) PhaseJobWorkerCallback
	var unmergeCallback func(opindex uint64) PhaseJobWorkerCallback

	mergeCallback = func(opindex uint64) PhaseJobWorkerCallback {
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
					ll.Debug().Msg("> merge")

					switch committed, nexts, reasons, err := mergeOp(
						ctx,
						opindex,
						values,
						getMerger,
					); {
					case err != nil:
						return err
					default:
						ll.Debug().
							Uints64("nexts", nexts).
							Interface("reasons", reasons).
							Bool("is_committed", committed).
							Msg("< merge")

						if committed {
							addXMergedOp(phase, opindex, MergedFlag)
						} else {
							addXMergedOp(phase, opindex, UnmergedFlag)
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
							addXMergedOp(phase, opindex, UnmergedFlag)
						}

						return util.TraverseSliceV1(nexts, func(_ int, next uint64) (bool, error) {
							return true, j(next, mergeCallback(next))
						})
					}
				},
			)
		}
	}

	return func(opindex uint64) error {
			addXMergedOp(-1, opindex, MergedFlag)

			return newJob(opindex, mergeCallback(opindex))
		},
		func(opindex uint64) error {
			addXMergedOp(-1, opindex, UnmergedFlag)

			return newJob(opindex, unmergeCallback(opindex))
		},
		jobDone,
		func(f phaseMergerWaitCallbackFunc) error {
			if err := jobWait(); err != nil {
				return err
			}

			return doneXMergedOps(func(newlyMergedOps, newlyUnmergedOps []uint64) error {
				if f == nil {
					return nil
				}

				return f(newlyMergedOps, newlyUnmergedOps, doneReason(newlyUnmergedOps))
			})
		},
		nil
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

	return func(opindex uint64, values []StateMergeValue) (stateKeys []string) {
			sg.Do(
				string(util.Uint64ToBytes(opindex)),
				func() (interface{}, error) {
					switch i, found := cache.Get(opindex); {
					case found:
						stateKeys = i
					default:
						stateKeys = extractStateMergeValueKeys(values)

						cache.Set(opindex, stateKeys, 0)
					}

					return nil, nil
				},
			)

			return stateKeys
		},
		func() {
			cache.Close()
		}
}

func newGetMergersFunc(
	m util.LockedMap[string, StateMerger],
	getState func(stateKey string) (base.State, bool, error),
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

type xMergedFlag string

const (
	MergedFlag   xMergedFlag = "merged"
	UnmergedFlag xMergedFlag = "unmerged"
)

func addXMergedOpsFuncs(
	previousOps func(opindex uint64) xMergedFlag,
	mapSize uint64,
) (
	func(opindex uint64, _ xMergedFlag),
	func(func(mergedOps, unmergedOps []uint64) error) error,
	error,
) {
	var newlyXMergedOps util.LockedMap[uint64, xMergedFlag]

	switch i, err := util.NewShardedMap[uint64, xMergedFlag](mapSize, nil); {
	case err != nil:
		return nil, nil, err
	default:
		newlyXMergedOps = i
	}

	return func(opindex uint64, x xMergedFlag) {
			_ = newlyXMergedOps.SetValue(opindex, x)
		},
		func(f func([]uint64, []uint64) error) error {
			defer newlyXMergedOps.Close()

			if f == nil {
				return nil
			}

			if newlyXMergedOps.Len() < 1 {
				return f(nil, nil)
			}

			var addM, addU util.CompactAppendSliceAddFunc[uint64]
			var doneM, doneU util.CompactAppendSliceDoneFunc[uint64]

			switch a, d, err := util.CompactAppendSliceV1[uint64](mapSize, mapSize); {
			case err != nil:
				return nil
			default:
				defer d(nil)

				addM = a
				doneM = d
			}

			switch a, d, err := util.CompactAppendSliceV1[uint64](mapSize, mapSize); {
			case err != nil:
				return nil
			default:
				defer d(nil)

				addU = a
				doneU = d
			}

			_ = newlyXMergedOps.Traverse(func(opindex uint64, nx xMergedFlag) bool {
				switch ix := previousOps(opindex); {
				case nx == ix:
				case nx == MergedFlag:
					addM(opindex)
				default:
					addU(opindex)
				}

				return true
			})

			return doneM(func(mergedOps []uint64) error {
				return doneU(func(unmergedOps []uint64) error {
					return f(mergedOps, unmergedOps)
				})
			})
		},
		nil
}

func isXMergedOpsFuncs(
	baseOps util.LockedMap[uint64, xMergedFlag],
	mapSize uint64,
) (
	func(phase int, opindex uint64, _ xMergedFlag),
	func(func(mergedOps, unmergedOps []uint64) error) error,
	error,
) {
	var add func(opindex uint64, _ xMergedFlag)
	var done func(func(mergedOps, unmergedOps []uint64) error) error

	switch a, d, err := addXMergedOpsFuncs(func(opindex uint64) xMergedFlag {
		x, _ := baseOps.Value(opindex)

		return x
	}, mapSize); {
	case err != nil:
		return nil, nil, err
	default:
		add = a
		done = d
	}

	var ops util.LockedMap[uint64, int]

	switch i, err := util.NewShardedMap[uint64, int](mapSize, nil); {
	case err != nil:
		return nil, nil, err
	default:
		ops = i
	}

	return func(phase int, opindex uint64, x xMergedFlag) {
			_, _, _ = ops.Set(opindex, func(prev int, found bool) (int, error) {
				if found && phase <= prev {
					return 0, util.ErrLockedSetIgnore
				}

				if phase == -1 {
					baseOps.SetValue(opindex, x)

					return phase, nil
				}

				add(opindex, x)

				return phase, nil
			})
		},
		func(f func([]uint64, []uint64) error) error {
			defer ops.Close()

			if f == nil {
				return done(nil)
			}

			return done(func(mergedOps, unmergedOps []uint64) error {
				if err := f(mergedOps, unmergedOps); err != nil {
					return err
				}

				_ = util.TraverseSliceV1(mergedOps, func(_ int, opindex uint64) (bool, error) {
					_ = baseOps.SetValue(opindex, MergedFlag)

					return true, nil
				})

				_ = util.TraverseSliceV1(unmergedOps, func(_ int, opindex uint64) (bool, error) {
					_ = baseOps.SetValue(opindex, UnmergedFlag)

					return true, nil
				})

				return nil
			})
		},
		nil
}

func newUpdateReasonsFuncs(mapSize uint64) (
	add func(reasons map[uint64]string) error,
	done func(unmergedOps []uint64) map[uint64]string,
) {
	var m util.LockedMap[uint64, string]

	var once sync.Once

	return func(reasons map[uint64]string) error {
			if len(reasons) < 1 {
				return nil
			}

			var gerr error

			once.Do(func() {
				switch i, err := util.NewShardedMap[uint64, string](mapSize, nil); {
				case err != nil:
					gerr = err
				default:
					m = i
				}
			})

			if gerr != nil {
				return gerr
			}

			for i := range reasons {
				_ = m.SetValue(i, reasons[i])
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
