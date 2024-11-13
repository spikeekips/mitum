package main

import (
	"context"
	"fmt"
	"slices"

	"github.com/pkg/errors"
	"github.com/rs/zerolog"
	"github.com/spikeekips/mitum/base"
	"github.com/spikeekips/mitum/util"
	"golang.org/x/sync/singleflight"
)

const (
	basePhase  int = 0
	startPhase int = 1
	lastPhase  int = -1

	mergeFlag   xMergeFlag = "m"
	unmergeFlag xMergeFlag = "u"
)

type xMergeFlag string

func (x xMergeFlag) String() string {
	switch x {
	case mergeFlag:
		return "merge"
	case unmergeFlag:
		return "unmerge"
	default:
		return fmt.Sprintf("<unknown: %q>", string(x))
	}
}

type (
	getStateFunc func(stateKey string) (base.State, bool, error)
	xMergeOpFunc func(uint64, base.StateValue) (
		commit func() error,
		next uint64,
		reason string,
		_ error,
	)
	xMergeOpValuesFunc func(
		opindex uint64,
		_ []StateMergeValue,
		getMerger func(StateMergeValue) (StateMerger, error),
	) (
		committed bool,
		nextOps []uint64,
		reasons map[uint64]string,
		_ error,
	)
	xMergeOpCallback     func(opindex uint64) phaseJobWorkerCallback
	mergerGetMergersFunc func(
		opindex uint64,
		_ []StateMergeValue,
		_ func(func(StateMergeValue) (StateMerger, error)) error,
	) error
	mergerGetStateMergeValuesFunc  func(opindex uint64) ([]StateMergeValue, error)
	extractStateMergeValueKeysFunc func(opindex uint64, _ []StateMergeValue) ([]string, error)
	phaseMergerXMergeFunc          func(opindex uint64) error
	phaseMergerWaitCallbackFunc    func(newMerged, newUnmerged []uint64, reasons map[uint64]string) error
	phaseMergerWaitFunc            func(phaseMergerWaitCallbackFunc) error
)

func newPhaseMerger(
	ctx context.Context,
	newJobWorker newJobWorkerFunc,
	xMergedOpsMapSize uint64,
	getStateMergeValues mergerGetStateMergeValuesFunc,
	getMergers mergerGetMergersFunc,
) (
	merge,
	unmerge phaseMergerXMergeFunc,
	done func(),
	wait phaseMergerWaitFunc,
	_ error,
) {
	var addXMergedOp func(phase int, opindex uint64, _ xMergeFlag)
	var doneXMergedOps func(func(mergedOps, unmergedOps []uint64) error) error

	switch a, d, err := newCollectXmergedOpsFuncs(xMergedOpsMapSize); {
	case err != nil:
		return nil, nil, nil, nil, err
	default:
		addXMergedOp = a
		doneXMergedOps = d
	}

	var newJob phaseJobWorkerNewJobFunc
	var jobDone func()
	var jobWait func() error

	switch n, d, w, err := newPhaseJobWorker(ctx, newJobWorker, xMergedOpsMapSize, startPhase); {
	case err != nil:
		return nil, nil, nil, nil, err
	default:
		newJob = n
		jobDone = d
		jobWait = w
	}

	addReason, doneReason := newAddOpUnmergeReasons(xMergedOpsMapSize)

	mergeCallback, unmergeCallback := newXMergeCallbacks(
		ctx,
		addXMergedOp,
		addReason,
		getStateMergeValues,
		getMergers,
	)

	return func(opindex uint64) error {
			addXMergedOp(basePhase, opindex, mergeFlag)

			return newJob(opindex, mergeCallback(opindex))
		},
		func(opindex uint64) error {
			addXMergedOp(basePhase, opindex, unmergeFlag)

			return newJob(opindex, unmergeCallback(opindex))
		},
		jobDone,
		func(f phaseMergerWaitCallbackFunc) error {
			if err := jobWait(); err != nil {
				return err
			}

			return doneXMergedOps(func(newMerged, newUnmerged []uint64) error {
				if f == nil {
					return nil
				}

				return f(newMerged, newUnmerged, doneReason(newUnmerged))
			})
		},
		nil
}

func newXMergeCallbacks(
	ctx context.Context,
	addXMergedOp func(int, uint64, xMergeFlag),
	addReason func(reasons map[uint64]string) error,
	getStateMergeValues mergerGetStateMergeValuesFunc,
	getMergers mergerGetMergersFunc,
) (
	merge,
	unmerge xMergeOpCallback,
) {
	var mergeCallback xMergeOpCallback
	var unmergeCallback xMergeOpCallback

	mergeCallback = newXMergeCallback(
		ctx,
		mergeFlag,
		func() xMergeOpCallback { return unmergeCallback },
		addXMergedOp,
		addReason,
		getStateMergeValues,
		getMergers,
	)

	unmergeCallback = newXMergeCallback(
		ctx,
		unmergeFlag,
		func() xMergeOpCallback { return mergeCallback },
		addXMergedOp,
		addReason,
		getStateMergeValues,
		getMergers,
	)

	return mergeCallback, unmergeCallback
}

func newXMergeCallback(
	ctx context.Context,
	x xMergeFlag,
	nextCallback func() xMergeOpCallback,
	addXMergedOp func(int, uint64, xMergeFlag),
	addReason func(reasons map[uint64]string) error,
	getStateMergeValues mergerGetStateMergeValuesFunc,
	getMergers mergerGetMergersFunc,
) xMergeOpCallback {
	log := newModuleLoggerFromContext(ctx, "phase-merger", func(zctx zerolog.Context) zerolog.Context {
		return zctx.Stringer("x", x)
	})

	return func(opindex uint64) phaseJobWorkerCallback {
		var xMergeOpValues xMergeOpValuesFunc

		switch x {
		case mergeFlag:
			xMergeOpValues = mergeOpValues
		case unmergeFlag:
			xMergeOpValues = unmergeOpValues
		default:
			return func(context.Context, int, phaseJobWorkerNewJobFunc) error {
				return util.ErrInvalid.Errorf("unknown merge flag, %q", x)
			}
		}

		var values []StateMergeValue

		switch i, err := getStateMergeValues(opindex); {
		case err != nil:
			return func(context.Context, int, phaseJobWorkerNewJobFunc) error {
				return err
			}
		default:
			values = i
		}

		l := log.With().Uint64("opindex", opindex).Logger()

		return func(_ context.Context, phase int, j phaseJobWorkerNewJobFunc) error {
			return getMergers(
				opindex,
				values,
				func(getMerger func(StateMergeValue) (StateMerger, error)) error {
					ll := l.With().Int("phase", phase).Logger()
					ll.Debug().Msgf("> %s", x)

					switch committed, nexts, reasons, err := xMergeOpValues(
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
							Msgf("< %s", x)

						if committed {
							addXMergedOp(phase, opindex, x)
						}

						if !committed && x == mergeFlag {
							addXMergedOp(phase, opindex, unmergeFlag)
						}

						if err := addReason(reasons); err != nil {
							return err
						}

						return traverseSlice(nexts, func(_ int, next uint64) (bool, error) {
							return true, j(next, nextCallback()(next))
						})
					}
				},
			)
		}
	}
}

func unmergeValues(
	getStateMergeValues mergerGetStateMergeValuesFunc,
	getMergers mergerGetMergersFunc,
	opindex uint64,
) (
	committed bool,
	nextMergeOps []uint64,
	_ error,
) {
	var values []StateMergeValue

	switch i, err := getStateMergeValues(opindex); {
	case err != nil:
		return false, nil, err
	default:
		values = i
	}

	return committed, nextMergeOps, getMergers(
		opindex,
		values,
		func(getMerger func(StateMergeValue) (StateMerger, error)) error {
			var err error

			switch committed, nextMergeOps, _, err = unmergeOpValues(
				opindex,
				values,
				getMerger,
			); {
			case err != nil:
				return err
			default:
				return nil
			}
		},
	)
}

func extractStateMergeValueKeysFromValues(values []StateMergeValue) (stateKeys []string) {
	switch i := len(values); {
	case i < 1:
		return nil
	case i < 2:
		return []string{values[0].Key()}
	}

	add, done, _ := uniqueCompactAppendSlice[string](uint64(len(values)), 0)

	_ = traverseSlice(values, func(_ int, value StateMergeValue) (bool, error) {
		add(value.Key())

		return true, nil
	})

	_ = done(func(s []string) error {
		stateKeys = slices.Clone(s)

		return nil
	})

	return stateKeys
}

func newExtractStateMergeValueKeys(cacheSize int) (
	extractStateMergeValueKeysFunc,
	func(),
) {
	var cache util.GCache[uint64, []string]

	switch {
	case cacheSize < 1:
		cache = newEmptyGCache[uint64, []string]()
	default:
		cache = util.NewLFUGCache[uint64, []string](cacheSize)
	}

	var sg singleflight.Group

	return func(opindex uint64, values []StateMergeValue) ([]string, error) {
			if len(values) < 1 {
				return nil, errors.Errorf("empty state values")
			}

			stateKeys, _, _ := util.SingleflightDo[[]string](
				&sg,
				string(util.Uint64ToBytes(opindex)),
				func() (stateKeys []string, _ error) {
					switch i, found := cache.Get(opindex); {
					case found:
						stateKeys = i
					default:
						stateKeys = extractStateMergeValueKeysFromValues(values)
						if len(stateKeys) < 1 {
							return nil, util.ErrNotFound.Errorf("empty state keys")
						}

						cache.Set(opindex, stateKeys, 0)
					}

					return stateKeys, nil
				},
			)

			if len(stateKeys) < 1 {
				return nil, errors.Errorf("empty extracted state keys")
			}

			return stateKeys, nil
		},
		cache.Close
}

func newGetMergers(
	m util.LockedMap[string, StateMerger],
	getState getStateFunc,
	extractStateMergeValueKeys extractStateMergeValueKeysFunc,
) (
	mergerGetMergersFunc,
	func(),
) {
	mmergers := util.NewMultiKeysLockedMap[string, StateMerger](m)

	return func(
			opindex uint64,
			values []StateMergeValue,
			f func(func(StateMergeValue) (StateMerger, error)) error,
		) error {
			switch i, err := extractStateMergeValueKeys(opindex, values); {
			case err != nil:
				return err
			case len(i) < 1:
				return nil
			default:
				return mmergers.LockFunc(
					i,
					func(m util.LockedMap[string, StateMerger]) error {
						return f(getOrCreateMergerFunc(m, getState))
					},
				)
			}
		},
		mmergers.Close
}

func newCollectXmergedOpsFuncs(
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
			case phase == basePhase:
				baseMergedOps.SetValue(opindex, x)

				return
			case phase == startPhase:
				if y, _ := baseMergedOps.Value(opindex); x == y {
					return
				}
			}

			_, _, _ = ops.Set(opindex, func(prev int, _ bool) (int, error) {
				i := 1

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

			var addM, addU compactAppendSliceAddFunc[uint64]
			var doneM, doneU compactAppendSliceDoneFunc[uint64]

			if err := rCompactAppendSlice[uint64](mapSize, mapSize, &addM, &doneM); err != nil {
				return err
			}

			defer func() {
				_ = doneM(nil)
			}()

			if err := rCompactAppendSlice[uint64](mapSize, mapSize, &addU, &doneU); err != nil {
				return err
			}

			defer func() {
				_ = doneU(nil)
			}()

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

func newAddOpUnmergeReasons(mapSize uint64) (
	add func(reasons map[uint64]string) error,
	done func(unmergedOps []uint64) map[uint64]string,
) {
	var m util.LockedMap[uint64, string]

	newMap := onceFunc(func() error {
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

func xMergeOpValues(
	opindex uint64,
	values []StateMergeValue,
	getMerger func(StateMergeValue) (StateMerger, error),
	xmergeFunc func(StateMerger) xMergeOpFunc,
) (
	committed bool,
	nextOps []uint64,
	reasons map[uint64]string,
	_ error,
) {
	if len(values) < 1 {
		return false, nil, nil, errors.Errorf("xmergeOp; empty values")
	}

	var (
		addNext    compactAppendSliceAddFunc[uint64]
		doneNext   compactAppendSliceDoneFunc[uint64]
		addCommit  compactAppendSliceAddFunc[func() error]
		doneCommit compactAppendSliceDoneFunc[func() error]
	)

	if err := rUniqueCompactAppendSlice[uint64](
		uint64(len(values)), 0, &addNext, &doneNext); err != nil {
		return false, nil, nil, err
	}

	defer func() {
		_ = doneNext(nil)
	}()

	if err := rCompactAppendSlice[func() error](
		uint64(len(values)), 0, &addCommit, &doneCommit); err != nil {
		return false, nil, nil, err
	}

	defer func() {
		_ = doneCommit(nil)
	}()

	addReason := func() func(uint64, string) {
		c := onceFunc(func() error {
			reasons = map[uint64]string{}

			return nil
		})

		return func(o uint64, r string) {
			if len(r) < 1 {
				return
			}

			_ = c()

			reasons[o] = r
		}
	}()

	var notCommitted bool

	if err := traverseSlice(values, func(_ int, value StateMergeValue) (bool, error) {
		var merger StateMerger

		switch i, err := getMerger(value); {
		case err != nil:
			return false, err
		default:
			merger = i
		}

		switch c, nextOp, r, err := xmergeFunc(merger)(opindex, value.Value()); {
		case err != nil:
			return false, err
		case c == nil:
			addReason(opindex, r)

			notCommitted = true

			return false, nil
		default:
			reasonOp := opindex

			if nextOp != opindex {
				addNext(nextOp)
				reasonOp = nextOp
			}

			addReason(reasonOp, r)
			addCommit(c)

			return true, nil
		}
	}); err != nil {
		return false, nil, nil, err
	}

	if notCommitted {
		return false, nil, reasons, nil
	}

	_ = doneNext(func(s []uint64) error {
		nextOps = slices.Clone(s)

		return nil
	})

	return committed, nextOps, reasons, doneCommit(func(s []func() error) error {
		if len(s) < 1 {
			return nil
		}

		committed = true

		return traverseSlice(s, func(_ int, f func() error) (bool, error) {
			return true, f()
		})
	})
}

func mergeOpValues(
	opindex uint64,
	values []StateMergeValue,
	getMerger func(StateMergeValue) (StateMerger, error),
) (
	committed bool,
	nextUnmergeOps []uint64,
	reasons map[uint64]string,
	_ error,
) {
	return xMergeOpValues(
		opindex,
		values,
		getMerger,
		func(merger StateMerger) xMergeOpFunc {
			return merger.Merge
		},
	)
}

func unmergeOpValues(
	opindex uint64,
	values []StateMergeValue,
	getMerger func(StateMergeValue) (StateMerger, error),
) (
	committed bool,
	nextMergeOps []uint64,
	_ map[uint64]string,
	_ error,
) {
	committed, nextMergeOps, reasons, err := xMergeOpValues(
		opindex,
		values,
		getMerger,
		func(merger StateMerger) xMergeOpFunc {
			return func(opindex uint64, value base.StateValue) (
				func() error,
				uint64,
				string,
				error,
			) {
				c, n, err := merger.Unmerge(opindex, value)

				return c, n, "", err
			}
		},
	)

	return committed, nextMergeOps, reasons, err
}

func getOrCreateMergerFunc(
	m util.LockedMap[string, StateMerger],
	getState getStateFunc,
) func(StateMergeValue) (StateMerger, error) {
	return func(value StateMergeValue) (merger StateMerger, _ error) {
		return merger, m.GetOrCreate(
			value.Key(),
			func(i StateMerger, _ bool) error {
				merger = i

				return nil
			},
			func() (StateMerger, error) {
				var stv base.StateValue

				switch i, found, err := getState(value.Key()); {
				case err != nil:
					return nil, err
				case found:
					stv = i.Value()
				}

				return value.Merger(stv)
			},
		)
	}
}

func getStateMergeValuesFuncFromPool(vp util.LockedMap[uint64, []StateMergeValue]) mergerGetStateMergeValuesFunc {
	return func(opindex uint64) ([]StateMergeValue, error) {
		switch i, found := vp.Value(opindex); {
		case !found:
			return nil, util.ErrNotFound.Errorf("state merge values, opindex=%d", opindex)
		default:
			return i, nil
		}
	}
}
