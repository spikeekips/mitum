package m2

import (
	"context"
	"maps"
	"slices"

	"github.com/pkg/errors"
	dutil "github.com/spikeekips/mitum/example/test364_5/util"
)

type (
	mergerNewJobFunc     = func(dutil.InErrorFunc[context.Context], func() bool) error
	mergerWaitResultFunc = func(int, getStateOpsFunc) error
)

var (
	MergerMinSemSize  int64  = 1 << 9
	MergerMinPoolSize uint64 = 1 << 9
)

func NewMerger(
	ctx context.Context,
	approxSize uint64,
	getSMVs func(uint64) (SMV, []SMV, bool),
	getSVM MergeSVMFunc,
	extractSMVKeys extractSMVKeysFunc,
	reportUnmerged func(context.Context, uint64, string) error,
) (
	func(uint64) error,
	func(),
	func(uint64) error,
	func(),
	func(mergerWaitResultFunc) error,
	error,
) {
	semSize := dutil.SafeUint64ToInt64(approxSize)
	poolSize := approxSize

	if approxSize < 1 {
		semSize, poolSize = MergerMinSemSize, MergerMinPoolSize //nolint:mnd
	}

	var newBgJob dutil.JobWorkerNewJobFunc
	var bgJobDone func()
	var bgJobWait dutil.InErrorFunc[error]

	switch n, j, w, err := dutil.NewFuncJobWorker(ctx, semSize); {
	case err != nil:
		return nil, nil, nil, nil, nil, err
	default:
		newBgJob, bgJobDone, bgJobWait = n, j, w
	}

	var newJob mergerNewJobFunc
	var jobDone func()
	var jobWait dutil.InErrorFunc[error]

	switch n, j, w, err := dutil.NewFuncJobWorker(ctx, semSize); {
	case err != nil:
		return nil, nil, nil, nil, nil, err
	default:
		jobDone, jobWait = j, w
		newJob = newFuncMergerNewJob(n, jobDone)
	}

	setReason := func(opindex uint64, reason string) error {
		return newBgJob(func(ctx context.Context) error {
			return reportUnmerged(ctx, opindex, reason)
		})
	}

	var collectOpsAddSMVs func(uint64, SMV, []SMV) bool
	var collectOpsUnmerge func(uint64) bool
	var collectOpsTraverse dutil.InErrorFunc[collectOpsTraverseFunc]

	switch a, u, t, err := newFuncCollectStateOps(poolSize, extractSMVKeys); {
	case err != nil:
		return nil, nil, nil, nil, nil, err
	default:
		collectOpsAddSMVs, collectOpsUnmerge, collectOpsTraverse = a, u, t
	}

	var mergeCountDone func() (uint64, bool)
	var merge func(uint64) error
	var unmergeCountDone func() (uint64, bool)
	var unmerge func(uint64) error
	var closef func()

	switch d, i, ud, u, c, err := newMergeValuesFuncs(
		poolSize,
		getSVM,
		getSMVs,
		newJob,
		jobDone,
		setReason,
		collectOpsAddSMVs,
		collectOpsUnmerge,
	); {
	case err != nil:
		return nil, nil, nil, nil, nil, err
	default:
		mergeCountDone, merge, unmergeCountDone, unmerge, closef = d, i, ud, u, c
	}

	return func(opindex uint64) error {
			if err := merge(opindex); err != nil {
				return jobWait(err)
			}

			return nil
		},
		func() {
			if _, done := mergeCountDone(); done {
				jobDone()
			}
		},
		func(opindex uint64) error {
			if err := unmerge(opindex); err != nil {
				return jobWait(err)
			}

			return nil
		},
		func() {
			if _, done := unmergeCountDone(); done {
				jobDone()
			}
		},
		func(result mergerWaitResultFunc) error {
			defer closef()

			if err := jobWait(nil); err != nil {
				return err
			}

			if err := collectOpsTraverse(func(size int, getStateOps getStateOpsFunc) error {
				return result(
					size,
					getStateOps,
				)
			}); err != nil {
				return err
			}

			bgJobDone()

			return bgJobWait(nil)
		},
		nil
}

func newFuncSubMergeJob(
	mergeStateValueMerger MergeSVMFunc,
) func(uint64, SMV) (uint64, string, error) {
	return func(opindex uint64, value SMV) (uint64, string, error) {
		var next uint64
		var reason string

		return next, reason, mergeStateValueMerger(value, func(merger SVM) error {
			var err error

			next, reason, err = merger.SubMerge(opindex, value.Value())

			return err
		})
	}
}

func newFuncSubMerge(
	poolSize uint64,
	mergeStateValueMerger MergeSVMFunc,
	getSMVs func(uint64) (SMV, []SMV, bool),
	newJob mergerNewJobFunc,
	countJob func(uint64) bool,
	endCountJob func() bool,
	jobDone func(),
	setReason func(uint64, string) error,
	unmerge func(uint64) (uint64, error),
) func(opindex uint64, value SMV) error {
	f := newFuncSubMergeJob(mergeStateValueMerger)

	return func(opindex uint64, value SMV) error {
		var next uint64

		switch n, reason, err := f(opindex, value); {
		case err != nil:
			return err
		case reason == "":
			if countJob(0) {
				jobDone()
			}

			return nil
		default:
			if err := setReason(n, reason); err != nil {
				return err
			}

			next = n
		}

		switch l, err := unmerge(next); {
		case err != nil:
			return err
		default:
			if countJob(l) {
				jobDone()
			}

			return nil
		}
	}
}

func newMergeValuesFuncs(
	poolSize uint64,
	mergeStateValueMerger MergeSVMFunc,
	getSMVs func(uint64) (SMV, []SMV, bool),
	newJob mergerNewJobFunc,
	jobDone func(),
	setReason func(uint64, string) error,
	collectOpsAddSMVs func(uint64, SMV, []SMV) bool,
	collectOpsUnmerge func(uint64) bool,
) (
	func() (uint64, bool),
	func(uint64) error,
	func() (uint64, bool),
	func(uint64) error,
	func(),
	error,
) {
	var setMerged, setUnmerged, setIgnored, isIgnored func(uint64) bool
	var closePool func()

	switch m, u, si, ii, c, err := newMergedPool(poolSize); {
	case err != nil:
		return nil, nil, nil, nil, nil, err
	default:
		setMerged, setUnmerged, setIgnored, isIgnored, closePool = m, u, si, ii, c
	}

	im, dm, cm, em, iu, du, cu := newXMergeJobCounter()

	unmergef := newFuncUnmerge(getSMVs, mergeStateValueMerger, newJob)
	emUnmerge := unmergef(em)
	cuUnmerge := unmergef(cu)

	subMerge := newFuncSubMerge(
		poolSize,
		mergeStateValueMerger,
		getSMVs,
		newJob,
		cm,
		em,
		jobDone,
		setReason,
		func(opindex uint64) (uint64, error) {
			if !setUnmerged(opindex) {
				return 0, nil
			}

			_ = collectOpsUnmerge(opindex)

			return emUnmerge(opindex)
		},
	)

	return dm,
		func(opindex uint64) error {
			if !setMerged(opindex) {
				return nil
			}

			var sub SMV
			var adds []SMV
			switch s, a, found := getSMVs(opindex); {
			case !found:
				return errors.Errorf("unknown opindex, %d", opindex)
			default:
				sub, adds = s, a
			}

			_ = collectOpsAddSMVs(opindex, sub, adds)

			if sub != nil {
				im()

				if err := newJob(func(context.Context) error {
					return subMerge(opindex, sub)
				}, nil); err != nil {
					return err
				}
			}

			if isIgnored(opindex) {
				return nil
			}

			for value := range slices.Values(adds) {
				im()

				if err := newJob(
					func(context.Context) error {
						return mergeStateValueMerger(value, func(merger SVM) error {
							return merger.AddMerge(opindex, value.Value())
						})
					},
					func() bool {
						return cm(0)
					},
				); err != nil {
					return err
				}
			}

			return nil
		},
		du,
		func(opindex uint64) error {
			_ = collectOpsUnmerge(opindex)

			if !setUnmerged(opindex) {
				return nil
			}

			if _, _, found := getSMVs(opindex); !found {
				_ = setIgnored(opindex)

				return nil
			}

			i, err := cuUnmerge(opindex)
			if err != nil {
				return err
			}

			for range i {
				iu()
			}

			return nil
		},
		closePool,
		nil
}

func newFuncUnmerge(
	getSMVs func(uint64) (SMV, []SMV, bool),
	mergeStateValueMerger MergeSVMFunc,
	newJob mergerNewJobFunc,
) func(func() bool) func(uint64) (uint64, error) {
	return func(countJob func() bool) func(uint64) (uint64, error) {
		return func(opindex uint64) (uint64, error) {
			var adds []SMV
			switch _, vs, found := getSMVs(opindex); {
			case !found:
				return 0, nil
			default:
				adds = vs
			}

			for value := range slices.Values(adds) {
				if err := newJob(
					func(context.Context) error {
						return mergeStateValueMerger(value, func(merger SVM) error {
							return merger.Unmerge(opindex, value.Value())
						})
					},
					countJob,
				); err != nil {
					return 0, err
				}
			}

			return uint64(len(adds)), nil
		}
	}
}

func newFuncAddValuePool(size uint64) (
	func(uint64, SMV, []SMV) bool,
	func(uint64) (SMV, []SMV, bool),
	func(), /* close */
	error,
) {
	var pool dutil.LockedMap[uint64, [2][]SMV]

	switch p, err := dutil.NewLockedMap[uint64, [2][]SMV](nil, size); {
	case err != nil:
		return nil, nil, nil, err
	default:
		pool = p
	}

	return func(opindex uint64, sub SMV, adds []SMV) bool {
			created, _ := pool.Set(opindex, func(v [2][]SMV, found bool) ([2][]SMV, error) {
				if found {
					return v, dutil.ErrLockedSetIgnore
				}

				return [2][]SMV{{sub}, adds}, nil
			})

			return created
		},
		func(opindex uint64) (SMV, []SMV, bool) {
			vs, found := pool.Value(opindex)
			if !found {
				return nil, nil, false
			}

			var sub SMV
			if len(vs[0]) > 0 {
				sub = vs[0][0]
			}

			return sub, vs[1], true
		},
		pool.Close,
		nil
}

func newFuncUnmergedReason(size uint64) (
	func(uint64, string) error,
	func() map[uint64]string,
	error,
) {
	var pool dutil.LockedMap[uint64, string]

	switch p, err := dutil.NewLockedMap[uint64, string](nil, size); {
	case err != nil:
		return nil, nil, err
	default:
		pool = p
	}

	return func(opindex uint64, reason string) error {
			if reason == "" {
				return errors.Errorf("empty reason: %d", opindex)
			}

			_, _ = pool.Set(opindex, func(_ string, found bool) (string, error) {
				if found {
					return "", dutil.ErrLockedSetIgnore
				}

				return reason, nil
			})

			return nil
		},
		func() map[uint64]string {
			defer pool.Close()

			return maps.Clone(pool.Map())
		},
		nil
}

func newFuncMergerNewJob(
	newJob dutil.JobWorkerNewJobFunc,
	jobDone func(),
) mergerNewJobFunc {
	return func(f dutil.InErrorFunc[context.Context], g func() bool) error {
		return newJob(func(ctx context.Context) error {
			if err := f(ctx); err != nil {
				return err
			}

			if g != nil && g() {
				jobDone()
			}

			return nil
		})
	}
}

func newMergedPool(poolSize uint64) (
	func(uint64) bool, /* set merged */
	func(uint64) bool, /* set unmerged */
	func(uint64) bool, /* set ignored */
	func(uint64) bool, /* ignored */
	func(),
	error,
) {
	var pool dutil.LockedMap[uint64, [3]bool]

	switch p, err := dutil.NewLockedMap[uint64, [3]bool](nil, poolSize); {
	case err != nil:
		return nil, nil, nil, nil, nil, err
	default:
		pool = p
	}

	return func(opindex uint64) bool {
			var created bool

			_, _ = pool.Set(opindex, func(v [3]bool, found bool) ([3]bool, error) {
				if v[0] {
					return v, dutil.ErrLockedSetIgnore
				}

				created = true

				v[0] = true

				return v, nil
			})

			return created
		},
		func(opindex uint64) bool {
			var created bool

			_, _ = pool.Set(opindex, func(v [3]bool, found bool) ([3]bool, error) {
				if v[1] || v[2] {
					return v, dutil.ErrLockedSetIgnore
				}

				created = true

				v[1] = true

				return v, nil
			})

			return created
		},
		func(opindex uint64) bool {
			var created bool

			_, _ = pool.Set(opindex, func(v [3]bool, found bool) ([3]bool, error) {
				if v[2] {
					return v, dutil.ErrLockedSetIgnore
				}

				created = true

				v[2] = true

				return v, nil
			})

			return created
		},
		func(opindex uint64) bool {
			switch i, found := pool.Value(opindex); {
			case !found:
				return false
			default:
				return i[2]
			}
		},
		pool.Close,
		nil
}
