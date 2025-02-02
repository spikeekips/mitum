package main

import (
	"context"
	"fmt"
	"slices"
	"sync"

	"github.com/pkg/errors"
	"github.com/rs/zerolog"
	"github.com/spikeekips/mitum/base"
	dutil "github.com/spikeekips/mitum/example/test364_5/util"
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
	getStateFunc = func(stateKey string) (base.State, bool, error)
	xMergeOpFunc = func(uint64, base.StateValue) (
		commit func() error,
		next uint64,
		reason string,
		_ error,
	)
	xMergeOpValuesFunc = func(
		opindex uint64,
		_ []stateMergeValue,
		getMerger func(stateMergeValue) (stateValueMerger, error),
	) (
		committed bool,
		nextOps []uint64,
		reasons map[uint64]string,
		_ error,
	)
	opindexPhaseJobWorkerCallback  = dutil.InFunc[uint64 /* opindex */, phaseJobWorkerCallback]
	mergerGetStateValueMergersFunc = func(
		opindex uint64,
		_ []stateMergeValue,
		_ func(func(stateMergeValue) (stateValueMerger, error)) error,
	) error
	opindexGetStateMergeValuesFunc = func(opindex uint64) ([]stateMergeValue, error)
	opindexSetStateMergeValuesFunc = func(opindex uint64, _ []stateMergeValue) (bool, error)
	extractStateMergeValueKeysFunc = func(opindex uint64, _ []stateMergeValue) ([]string /* state keys */, error)
	phaseMergerWaitFunc            func() (newMerged, newUnmerged []uint64, reasons map[uint64]string, _ error)
)

func newFuncPhaseMerger(
	ctx context.Context,
	newJobWorker dutil.NewJobWorkerFunc,
	maxOpsSize uint64,
	getStateMergeValues opindexGetStateMergeValuesFunc,
	getStateValueMergers mergerGetStateValueMergersFunc,
) (
	merge,
	unmerge dutil.InErrorFunc[uint64],
	done func(),
	_ phaseMergerWaitFunc,
	_ error,
) {
	getStateMergeValues = mustFuncGetStateMergeValues(getStateMergeValues)

	var addXMergedOp func(phase int, opindex uint64, _ xMergeFlag)
	var doneXMergedOps func() (merged, unmerged []uint64, _ error)

	switch a, d, err := newFuncCollectXmergedOps(maxOpsSize); {
	case err != nil:
		return nil, nil, nil, nil, err
	default:
		addXMergedOp, doneXMergedOps = a, d
	}

	var newJob phaseJobWorkerNewJobFunc
	var jobDone func()
	var jobWait dutil.ErrorFunc

	switch n, d, w, err := newFuncPhaseJobWorker(ctx, newJobWorker, maxOpsSize, startPhase); {
	case err != nil:
		return nil, nil, nil, nil, err
	default:
		newJob, jobDone, jobWait = n, d, w
	}

	var addReason func(reasons map[uint64]string) error
	var doneReason func() (map[uint64]string, error)

	switch a, d, err := newFuncAddOpUnmergeReasons(maxOpsSize); {
	case err != nil:
		return nil, nil, nil, nil, err
	default:
		addReason, doneReason = a, d
	}

	mergeCallback, unmergeCallback := newFuncXMergeCallbacks(
		ctx,
		addXMergedOp,
		addReason,
		getStateMergeValues,
		getStateValueMergers,
	)

	return func(opindex uint64) error {
			addXMergedOp(basePhase, opindex, mergeFlag)

			return newJob(opindex, mergeCallback(opindex))
		},
		func(opindex uint64) error {
			// NOTE for testing
			// addXMergedOp(basePhase, opindex, unmergeFlag)

			return newJob(opindex, unmergeCallback(opindex))
		},
		jobDone,
		func() ([]uint64, []uint64, map[uint64]string, error) {
			defer func() {
				_, _ = doneReason()
			}()

			if err := jobWait(); err != nil {
				return nil, nil, nil, err
			}

			switch m, u, err := doneXMergedOps(); {
			case err != nil:
				return nil, nil, nil, err
			default:
				switch r, err := doneReason(); {
				case err != nil:
					return nil, nil, nil, err
				default:
					return m, u, r, nil
				}
			}
		},
		nil
}

func newFuncXMergeCallbacks(
	ctx context.Context,
	addXMergedOp func(int, uint64, xMergeFlag),
	addReason func(reasons map[uint64]string) error,
	getStateMergeValues opindexGetStateMergeValuesFunc,
	getStateValueMergers mergerGetStateValueMergersFunc,
) (
	merge,
	unmerge opindexPhaseJobWorkerCallback,
) {
	var mergeCallback opindexPhaseJobWorkerCallback
	var unmergeCallback opindexPhaseJobWorkerCallback

	mergeCallback = newFuncXMergeCallback(
		ctx,
		mergeFlag,
		func() opindexPhaseJobWorkerCallback { return unmergeCallback },
		addXMergedOp,
		addReason,
		getStateMergeValues,
		getStateValueMergers,
	)

	unmergeCallback = newFuncXMergeCallback(
		ctx,
		unmergeFlag,
		func() opindexPhaseJobWorkerCallback { return mergeCallback },
		addXMergedOp,
		addReason,
		getStateMergeValues,
		getStateValueMergers,
	)

	return mergeCallback, unmergeCallback
}

func newFuncXMergeCallback(
	ctx context.Context,
	x xMergeFlag,
	nextCallback func() opindexPhaseJobWorkerCallback,
	addXMergedOp func(int, uint64, xMergeFlag),
	addReason func(reasons map[uint64]string) error,
	getStateMergeValues opindexGetStateMergeValuesFunc,
	getStateValueMergers mergerGetStateValueMergersFunc,
) opindexPhaseJobWorkerCallback {
	log := dutil.NewModuleLoggerFromContext(ctx, "phase-merger", func(zctx zerolog.Context) zerolog.Context {
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

		var values []stateMergeValue

		switch i, err := getStateMergeValues(opindex); {
		case err != nil:
			return func(context.Context, int, phaseJobWorkerNewJobFunc) error {
				return err
			}
		case len(i) < 1:
			return func(context.Context, int, phaseJobWorkerNewJobFunc) error {
				return util.ErrNotFound.Errorf("unknown opindex, %d", opindex)
			}
		default:
			values = i
		}

		l := log.With().Uint64("opindex", opindex).Logger()

		return func(_ context.Context, phase int, j phaseJobWorkerNewJobFunc) error {
			return getStateValueMergers(
				opindex,
				values,
				func(getMerger func(stateMergeValue) (stateValueMerger, error)) error {
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

						for next := range slices.Values(nexts) {
							if err := j(next, nextCallback()(next)); err != nil {
								return err
							}
						}

						return nil
					}
				},
			)
		}
	}
}

func unmergeValues(
	getStateMergeValues opindexGetStateMergeValuesFunc,
	getStateValueMergers mergerGetStateValueMergersFunc,
	opindex uint64,
) (
	committed bool,
	nextMergeOps []uint64,
	_ error,
) {
	var values []stateMergeValue

	switch i, err := getStateMergeValues(opindex); {
	case err != nil:
		return false, nil, err
	case len(i) < 1:
		return false, nil, util.ErrNotFound.Errorf("unknown opindex, %d", opindex)
	default:
		values = i
	}

	return committed, nextMergeOps, getStateValueMergers(
		opindex,
		values,
		func(getMerger func(stateMergeValue) (stateValueMerger, error)) error {
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

func extractStateMergeValueKeysFromValues(values []stateMergeValue) (stateKeys []string) {
	switch i := len(values); {
	case i < 1:
		return nil
	case i < 2:
		return []string{values[0].Key()}
	}

	stateKeys = make([]string, len(values))

	for i, value := range slices.All(values) {
		stateKeys[i] = value.Key()
	}

	return stateKeys
}

func newFuncExtractStateMergeValueKeys(maxSize, size int) (
	extractStateMergeValueKeysFunc,
	func(),
) {
	if size > maxSize {
		size = maxSize
	}

	var cache util.GCache[uint64, []string]

	switch {
	case size < 1:
		cache = dutil.NewEmptyGCache[uint64, []string]()
	default:
		cache = util.NewLFUGCache[uint64, []string](size)
	}

	var sg singleflight.Group

	return func(opindex uint64, values []stateMergeValue) ([]string, error) {
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

func newFuncGetStateValueMergers(
	m util.LockedMap[string, stateValueMerger],
	getState getStateFunc,
	extractStateMergeValueKeys extractStateMergeValueKeysFunc,
) (
	mergerGetStateValueMergersFunc,
	func(),
) {
	mmergers := util.NewMultiKeysLockedMap[string, stateValueMerger](m)

	return func(
			opindex uint64,
			values []stateMergeValue,
			f func(func(stateMergeValue) (stateValueMerger, error)) error,
		) error {
			switch i, err := extractStateMergeValueKeys(opindex, values); {
			case err != nil:
				return err
			case len(i) < 1:
				return nil
			default:
				return mmergers.LockFunc(
					i,
					func(m util.LockedMap[string, stateValueMerger]) error {
						return f(newFuncGetOrCreateMerger(m, getState))
					},
				)
			}
		},
		mmergers.Close
}

func newFuncCollectXmergedOps(
	mapSize uint64,
) (
	func(phase int, opindex uint64, _ xMergeFlag),
	func() (merged, unmerged []uint64, _ error),
	error,
) {
	var baseMergedOps util.LockedMap[uint64, xMergeFlag]

	switch i, err := util.NewLockedMap[uint64, xMergeFlag](mapSize, nil); {
	case err != nil:
		return nil, nil, err
	default:
		baseMergedOps = i
	}

	var ops util.LockedMap[uint64 /* opindex */, int /* count */]

	switch i, err := util.NewLockedMap[uint64, int](mapSize, nil); {
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
		func() ([]uint64, []uint64, error) {
			defer func() {
				baseMergedOps.Close()
				ops.Close()
			}()

			var addM, addU dutil.AppendSliceAddFunc[uint64]
			var doneM, doneU dutil.OutFunc[[]uint64]

			switch a, d, err := dutil.NewAppendSlice[uint64](mapSize, mapSize); {
			case err != nil:
				return nil, nil, err
			default:
				addM, doneM = a, d
			}

			switch a, d, err := dutil.NewAppendSlice[uint64](mapSize, mapSize); {
			case err != nil:
				return nil, nil, err
			default:
				addU, doneU = a, d
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

			return doneM(), doneU(), nil
		},
		nil
}

func newFuncAddOpUnmergeReasons(mapSize uint64) (
	add func(reasons map[uint64]string) error,
	done func() (reasons map[uint64]string, _ error),
	_ error,
) {
	var m util.LockedMap[uint64, string]

	switch i, err := util.NewLockedMap[uint64, string](mapSize, nil); {
	case err != nil:
		return nil, nil, err
	default:
		m = i
	}

	return func(reasons map[uint64]string) error {
			if len(reasons) < 1 {
				return nil
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
		func() (map[uint64]string, error) {
			defer m.Close()

			return m.Map(), nil
		},
		nil
}

func xMergeOpValues(
	opindex uint64,
	values []stateMergeValue,
	getMerger func(stateMergeValue) (stateValueMerger, error),
	xmergeFunc func(stateValueMerger) xMergeOpFunc,
) (
	committed bool,
	nextOps []uint64,
	reasons map[uint64]string,
	_ error,
) {
	valuesSize := uint64(len(values))

	if valuesSize < 1 {
		return false, nil, nil, errors.Errorf("xmergeOp; empty values")
	}

	var (
		addNext    dutil.AppendSliceAddFunc[uint64]
		doneNext   dutil.OutFunc[[]uint64]
		addCommit  dutil.AppendSliceAddFunc[func() error]
		doneCommit dutil.OutFunc[[]func() error]
	)

	switch a, d, err := dutil.NewComparableUniqueAppendSlice[uint64](valuesSize, 0); {
	case err != nil:
		return false, nil, nil, err
	default:
		addNext, doneNext = a, d
	}

	switch a, d, err := dutil.NewLockAppendSlice[func() error](valuesSize, 0); {
	case err != nil:
		return false, nil, nil, err
	default:
		addCommit, doneCommit = a, d
	}

	addReason := func() func(uint64, string) {
		c := sync.OnceFunc(func() {
			reasons = map[uint64]string{}
		})

		return func(o uint64, r string) {
			if len(r) < 1 {
				return
			}

			c()

			reasons[o] = r
		}
	}()

	var notCommitted bool

end:
	for value := range slices.Values(values) {
		var merger stateValueMerger

		switch i, err := getMerger(value); {
		case err != nil:
			return false, nil, nil, err
		default:
			merger = i
		}

		switch c, nextOp, r, err := xmergeFunc(merger)(opindex, value.Value()); {
		case err != nil:
			return false, nil, nil, err
		case c == nil:
			addReason(opindex, r)

			notCommitted = true

			break end
		default:
			reasonOp := opindex

			if nextOp != opindex {
				addNext(nextOp)
				reasonOp = nextOp
			}

			addReason(reasonOp, r)
			addCommit(c)
		}
	}

	if notCommitted {
		return false, nil, reasons, nil
	}

	return committed, doneNext(), reasons, func() error {
		for i, f := range slices.All(doneCommit()) {
			if i == 0 {
				committed = true
			}

			if err := f(); err != nil {
				return err
			}
		}

		return nil
	}()
}

func mergeOpValues(
	opindex uint64,
	values []stateMergeValue,
	getMerger func(stateMergeValue) (stateValueMerger, error),
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
		func(merger stateValueMerger) xMergeOpFunc {
			return merger.Merge
		},
	)
}

func unmergeOpValues(
	opindex uint64,
	values []stateMergeValue,
	getMerger func(stateMergeValue) (stateValueMerger, error),
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
		func(merger stateValueMerger) xMergeOpFunc {
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

func newFuncGetOrCreateMerger(
	m util.LockedMap[string, stateValueMerger],
	getState getStateFunc,
) func(stateMergeValue) (stateValueMerger, error) {
	return func(value stateMergeValue) (merger stateValueMerger, _ error) {
		return merger, m.GetOrCreate(
			value.Key(),
			func(i stateValueMerger, _ bool) error {
				merger = i

				return nil
			},
			func() (stateValueMerger, error) {
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

func newFuncGetStateMergeValuesFromPool(vp util.LockedMap[uint64, []stateMergeValue]) opindexGetStateMergeValuesFunc {
	return func(opindex uint64) ([]stateMergeValue, error) {
		switch i, found := vp.Value(opindex); {
		case !found:
			return nil, nil
		default:
			return i, nil
		}
	}
}

func mustFuncGetStateMergeValues(get opindexGetStateMergeValuesFunc) opindexGetStateMergeValuesFunc {
	return func(opindex uint64) ([]stateMergeValue, error) {
		switch i, err := get(opindex); {
		case err != nil:
			return nil, err
		case len(i) < 1:
			return nil, util.ErrNotFound.Errorf("opindex=%d", opindex)
		default:
			return i, nil
		}
	}
}
