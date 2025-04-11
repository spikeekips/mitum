package dop5

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
)

const (
	basePhase  int = 0
	startPhase int = 1
	lastPhase  int = -1

	mergeFlag   xMergeFlag = "m"
	unmergeFlag xMergeFlag = "u"
	mergeName              = "merge"
	unmergeName            = "unmerge"
)

type xMergeFlag string

func (x xMergeFlag) String() string {
	switch x {
	case mergeFlag:
		return mergeName
	case unmergeFlag:
		return unmergeName
	default:
		return fmt.Sprintf("<unknown: %q>", string(x))
	}
}

type (
	GetStateFunc = func(stateKey string) (base.State, bool, error)
	xMergeOpFunc = func(uint64, base.StateValue) (
		commit func() error,
		next uint64,
		reason string,
		_ error,
	)
	xMergeOpValuesFunc = func(
		opindex uint64,
		_ []SMV,
		getMerger func(SMV) (SVM, error),
	) (
		committed bool,
		nextOps []uint64,
		reasons map[uint64]string,
		_ error,
	)
	opindexPhaseJobWorkerCallback = dutil.InFunc[uint64 /* opindex */, phaseJobWorkerCallback]
	mergerGetSVMsFunc             = func(
		opindex uint64,
		_ []SMV,
		_ func(func(SMV) (SVM, error)) error,
	) error
	opindexGetSMVsFunc  = func(opindex uint64) ([]SMV, error)
	opindexSetSMVsFunc  = func(opindex uint64, _ []SMV) (bool, error)
	extractSMVKeysFunc  = func(opindex uint64, _ []SMV) ([]string /* state keys */, error)
	phaseMergerWaitFunc func() (newMerged, newUnmerged []uint64, reasons map[uint64]string, _ error)
)

func newFuncPhaseMerger(
	ctx context.Context,
	newJobWorker dutil.NewJobWorkerFunc,
	maxOpsSize uint64,
	getSMVs opindexGetSMVsFunc,
	getSVMs mergerGetSVMsFunc,
) (
	dutil.InErrorFunc[uint64], /* merge */
	dutil.InErrorFunc[uint64], /* unmerge */
	func(), /* done */
	phaseMergerWaitFunc,
	error,
) {
	getSMVs = mustFuncGetSMVs(getSMVs)

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
		getSMVs,
		getSVMs,
	)

	return func(opindex uint64) error {
			addXMergedOp(basePhase, opindex, mergeFlag)

			return newJob(opindex, mergeCallback(opindex))
		},
		func(opindex uint64) error {
			// NOTE for testing addXMergedOp(basePhase, opindex, unmergeFlag)
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
	getSMVs opindexGetSMVsFunc,
	getSVMs mergerGetSVMsFunc,
) (
	opindexPhaseJobWorkerCallback, /* merge */
	opindexPhaseJobWorkerCallback, /* unmerge */
) {
	var mergeCallback opindexPhaseJobWorkerCallback
	var unmergeCallback opindexPhaseJobWorkerCallback

	mergeCallback = newFuncXMergeCallback(
		ctx,
		mergeFlag,
		func() opindexPhaseJobWorkerCallback { return unmergeCallback },
		addXMergedOp,
		addReason,
		getSMVs,
		getSVMs,
	)

	unmergeCallback = newFuncXMergeCallback(
		ctx,
		unmergeFlag,
		func() opindexPhaseJobWorkerCallback { return mergeCallback },
		addXMergedOp,
		addReason,
		getSMVs,
		getSVMs,
	)

	return mergeCallback, unmergeCallback
}

func newFuncXMergeCallback(
	ctx context.Context,
	x xMergeFlag,
	nextCallback func() opindexPhaseJobWorkerCallback,
	addXMergedOp func(int, uint64, xMergeFlag),
	addReason func(reasons map[uint64]string) error,
	getSMVs opindexGetSMVsFunc,
	getSVMs mergerGetSVMsFunc,
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

		var values []SMV

		switch i, err := getSMVs(opindex); {
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
			return getSVMs(
				opindex,
				values,
				func(getMerger func(SMV) (SVM, error)) error {
					ll := l.With().Int("phase", phase).Logger()
					ll.Trace().Msgf("> %s", x)

					switch committed, nexts, reasons, err := xMergeOpValues(
						opindex,
						values,
						getMerger,
					); {
					case err != nil:
						return err
					default:
						ll.Trace().
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
	getSMVs opindexGetSMVsFunc,
	getSVMs mergerGetSVMsFunc,
	opindex uint64,
) (
	bool, /* committed */
	[]uint64, /* next merge ops */
	error,
) {
	var values []SMV

	switch i, err := getSMVs(opindex); {
	case err != nil:
		return false, nil, err
	case len(i) < 1:
		return false, nil, util.ErrNotFound.Errorf("unknown opindex, %d", opindex)
	default:
		values = i
	}

	var committed bool
	var nextMergeOps []uint64

	return committed, nextMergeOps, getSVMs(
		opindex,
		values,
		func(getMerger func(SMV) (SVM, error)) error {
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

func extractSMVKeysFromValues(values []SMV) []string {
	switch i := len(values); {
	case i < 1:
		return nil
	case i < 2: //nolint:mnd
		return []string{values[0].Key()}
	}

	stateKeys := make([]string, len(values))

	for i, value := range slices.All(values) {
		stateKeys[i] = value.Key()
	}

	return stateKeys
}

func newFuncExtractSMVKeys(ctx context.Context, maxSize, size uint64) (
	extractSMVKeysFunc,
	func(),
) {
	if size > maxSize {
		size = maxSize
	}

	get, _, lazySet, closef := dutil.NewLastFirstLazyCache[uint64, []string](
		ctx,
		dutil.SafeUint64toInt(size),
		maxSize, maxSize,
	)

	return func(opindex uint64, values []SMV) ([]string, error) {
			if len(values) < 1 {
				return nil, errors.New("empty state values")
			}

			var stateKeys []string

			switch i, found := get(opindex); {
			case found:
				stateKeys = i
			default:
				stateKeys = extractSMVKeysFromValues(values)
				if len(stateKeys) < 1 {
					return nil, util.ErrNotFound.Errorf("empty state keys")
				}

				lazySet(opindex, stateKeys, 0)
			}

			if len(stateKeys) < 1 {
				return nil, errors.New("empty extracted state keys")
			}

			return stateKeys, nil
		},
		closef
}

func newFuncGetSVMs(
	m dutil.LockedMap[string, SVM],
	getState GetStateFunc,
	extractSMVKeys extractSMVKeysFunc,
) (
	mergerGetSVMsFunc,
	func(),
) {
	mmergers := dutil.NewMultiKeysLockedMap[string, SVM](m)

	return func(
			opindex uint64,
			values []SMV,
			f func(func(SMV) (SVM, error)) error,
		) error {
			switch i, err := extractSMVKeys(opindex, values); {
			case err != nil:
				return err
			case len(i) < 1:
				return nil
			default:
				return mmergers.LockFunc(
					i,
					func(m dutil.LockedMap[string, SVM]) error {
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
	var baseMergedOps dutil.LockedMap[uint64, xMergeFlag]

	switch i, err := dutil.NewLockedMap[uint64, xMergeFlag](nil, mapSize); {
	case err != nil:
		return nil, nil, err
	default:
		baseMergedOps = i
	}

	var ops dutil.LockedMap[uint64 /* opindex */, int /* count */]

	switch i, err := dutil.NewLockedMap[uint64, int](nil, mapSize); {
	case err != nil:
		return nil, nil, err
	default:
		ops = i
	}

	return func(phase int, opindex uint64, x xMergeFlag) {
			switch phase {
			case basePhase:
				baseMergedOps.SetValue(opindex, x)

				return
			case startPhase:
				if y, _ := baseMergedOps.Value(opindex); x == y {
					return
				}
			}

			_, _ = ops.Set(opindex, func(prev int, _ bool) (int, error) {
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

			merged, unmerged := make([]uint64, 0, mapSize), make([]uint64, 0, mapSize)

			for opindex, i := range ops.Seq2 {
				switch {
				case i == 0:
				case i > 0:
					if x, _ := baseMergedOps.Value(opindex); x != mergeFlag {
						merged = append(merged, opindex)
					}
				default:
					if x, _ := baseMergedOps.Value(opindex); x != unmergeFlag {
						unmerged = append(unmerged, opindex)
					}
				}
			}

			return merged, unmerged, nil
		},
		nil
}

func newFuncAddOpUnmergeReasons(mapSize uint64) (
	func(reasons map[uint64]string) error, /* add */
	func() (reasons map[uint64]string, _ error), /* done */
	error,
) {
	var m dutil.LockedMap[uint64, string]

	switch i, err := dutil.NewLockedMap[uint64, string](nil, mapSize); {
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

				_, _ = m.Set(i, func(prev string, _ bool) (string, error) {
					switch r {
					case prev:
						return "", dutil.ErrLockedSetIgnore
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
	values []SMV,
	getMerger func(SMV) (SVM, error),
	xmergeFunc func(SVM) xMergeOpFunc,
) (
	bool, /* committed */
	[]uint64, /* next ops */
	map[uint64]string, /* reasons */
	error,
) {
	valuesSize := uint64(len(values))

	if valuesSize < 1 {
		return false, nil, nil, errors.New("xmergeOp; empty values")
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

	switch a, d, err := dutil.NewAppendSlice[func() error](valuesSize, 0); {
	case err != nil:
		return false, nil, nil, err
	default:
		addCommit, doneCommit = a, d
	}

	var reasons map[uint64]string

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
		var merger SVM

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

	var committed bool

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
	values []SMV,
	getMerger func(SMV) (SVM, error),
) (
	bool, /* committed */
	[]uint64, /* next unmerge ops */
	map[uint64]string, /* reasons */
	error,
) {
	return xMergeOpValues(
		opindex,
		values,
		getMerger,
		func(merger SVM) xMergeOpFunc {
			return merger.Merge
		},
	)
}

func unmergeOpValues(
	opindex uint64,
	values []SMV,
	getMerger func(SMV) (SVM, error),
) (
	bool, /* committed */
	[]uint64, /* next merge ops */
	map[uint64]string,
	error,
) {
	return xMergeOpValues(
		opindex,
		values,
		getMerger,
		func(merger SVM) xMergeOpFunc {
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
}

func newFuncGetOrCreateMerger(
	m dutil.LockedMap[string, SVM],
	getState GetStateFunc,
) func(SMV) (SVM, error) {
	return func(value SMV) (SVM, error) {
		var merger SVM

		return merger, m.GetOrCreate(
			value.Key(),
			func() (SVM, error) {
				var stv base.StateValue

				switch i, found, err := getState(value.Key()); {
				case err != nil:
					return nil, err
				case found:
					stv = i.Value()
				}

				return value.Merger(stv)
			},
			func(i SVM, _ bool) error {
				merger = i

				return nil
			},
		)
	}
}

func newFuncGetSMVsFromPool(vp dutil.LockedMap[uint64, []SMV]) opindexGetSMVsFunc {
	return func(opindex uint64) ([]SMV, error) {
		switch i, found := vp.Value(opindex); {
		case !found:
			return nil, nil
		default:
			return i, nil
		}
	}
}

func mustFuncGetSMVs(get opindexGetSMVsFunc) opindexGetSMVsFunc {
	return func(opindex uint64) ([]SMV, error) {
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
