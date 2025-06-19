package dop6

import (
	"context"
	"slices"
	"sync/atomic"

	"github.com/pkg/errors"
	dutil "github.com/spikeekips/mitum/example/test364_5/util"
)

type (
	mergerUnmergedFunc       = func(uint64, string) error
	mergerReportUnmergedFunc = func(context.Context) (mergerUnmergedFunc, func())
	mergerMergeSMVsFunc      = func(uint64, SMV, []SMV) error
	mergerWaitResultFunc     = dutil.In2ErrorFunc[uint64, collectOpsSeqFunc]
	mergerWaitFunc           = func(mergerWaitResultFunc) error
)

var (
	MergerMinSemSize  int64  = 1 << 9
	MergerMinPoolSize uint64 = 1 << 9
)

func newMerger(
	ctx context.Context,
	approxSize uint64,
	makeSVM MergeSVMFunc,
	reportUnmerged mergerReportUnmergedFunc,
) (
	mergerMergeSMVsFunc,
	func(),
	dutil.InErrorFunc[uint64],
	func(),
	mergerWaitFunc,
	error,
) {
	wctx, wctxCancel := dutil.ContextWithCancelCause(ctx)

	semSize, poolSize := dutil.SafeUint64ToInt64(approxSize), approxSize

	if approxSize < 1 {
		semSize, poolSize = MergerMinSemSize, MergerMinPoolSize
	}

	var newJob dutil.JobWorkerNewJobFunc
	var jobDone func()
	var jobWait dutil.InErrorFunc[error]

	switch n, j, w, err := dutil.NewFuncJobWorker(wctx, semSize); {
	case err != nil:
		return nil, nil, nil, nil, nil, err
	default:
		newJob, jobDone, jobWait = n, j, w
	}

	var setSMVs func(uint64, SMV, []SMV) bool
	var getSMVs func(uint64) (SMV, []SMV, bool)
	var closeSMVPool func()

	switch s, g, c, err := newFuncSMVPool(poolSize); {
	case err != nil:
		return nil, nil, nil, nil, nil, err
	default:
		setSMVs, getSMVs, closeSMVPool = s, g, c
	}

	var collectOpsMerge func(uint64, string, string) bool
	var collectOpsUnmerge func(uint64, string, string) bool
	var collectOpsSeq collectOpsSeqFunc

	switch a, u, t, err := newFuncCollectStateOps(poolSize); {
	case err != nil:
		return nil, nil, nil, nil, nil, err
	default:
		collectOpsMerge, collectOpsUnmerge, collectOpsSeq = a, u, t
	}

	var merge func(uint64, SMV, []SMV) error
	var mergeDone func()
	var unmerge func(uint64) error
	var unmergeDone func()
	var closeXMerge func()

	switch m, md, u, ud, c, err := newXMergeFuncs(
		wctx,
		poolSize,
		getSMVs,
		makeSVM,
		collectOpsMerge,
		collectOpsUnmerge,
		reportUnmerged,
		newJob,
		jobDone,
	); {
	case err != nil:
		return nil, nil, nil, nil, nil, err
	default:
		merge, mergeDone, unmerge, unmergeDone, closeXMerge = m, md, u, ud, c
	}

	iferrCancel := dutil.NewIfErrErrorInFunc(wctxCancel)

	var inserted uint64

	return func(opindex uint64, subv SMV, addvs []SMV) error {
			if !setSMVs(opindex, subv, addvs) {
				return errors.Errorf("known, %d", opindex)
			}

			_ = atomic.AddUint64(&inserted, 1)

			return iferrCancel(merge(opindex, subv, addvs))
		},
		mergeDone,
		func(opindex uint64) error {
			return iferrCancel(unmerge(opindex))
		},
		unmergeDone,
		func(result mergerWaitResultFunc) error {
			defer func() {
				_ = wctxCancel(nil)

				closeSMVPool()
				closeXMerge()
			}()

			if err := jobWait(nil); err != nil {
				return wctxCancel(err)
			}

			if result != nil {
				if err := result(atomic.LoadUint64(&inserted), collectOpsSeq); err != nil {
					return err
				}
			}

			return nil
		},
		nil
}

func newXMergeFuncs(
	ctx context.Context,
	poolSize uint64,
	getSMVs func(uint64) (SMV, []SMV, bool),
	makeSVM MergeSVMFunc,
	collectOpsMerge func(uint64, string, string) bool,
	collectOpsUnmerge func(uint64, string, string) bool,
	reportUnmergedf mergerReportUnmergedFunc,
	newJob dutil.JobWorkerNewJobFunc,
	jobDone func(),
) (
	func(opindex uint64, subv SMV, addvs []SMV) error,
	func(),
	func(opindex uint64) error,
	func(),
	func(), /* close */
	error,
) {
	var foundSubvs atomic.Value

	reportUnmerged, reportUnmergedDone := reportUnmergedf(ctx)

	initialCountJob, doneInitialCount, countJob, endCountJob, doneInitialUnmergeCountJob := newXMergeJobCounter(
		func() {
			if i := foundSubvs.Load(); i != nil {
				reportUnmergedDone()
			}
		},
		jobDone,
	)

	var mergeLock func(uint64) (bool, bool)
	var unmergeLock func(uint64) int
	var ignoreLock func(uint64) bool
	var closeMergeLock func()

	switch m, u, i, c, err := newFuncMergeLock(poolSize); {
	case err != nil:
		return nil, nil, nil, nil, nil, err
	default:
		mergeLock, unmergeLock, ignoreLock, closeMergeLock = m, u, i, c
	}

	subMerge := newFuncSubMerge(makeSVM, collectOpsMerge, collectOpsUnmerge)

	addMerge := func(opindex uint64, addv SMV) error {
		return makeSVM(addv, func(merger SVM) error {
			_ = collectOpsMerge(opindex, addv.Key(), "add")

			return merger.AddMerge(opindex, addv.Value())
		})
	}

	unmergeValue := func(opindex uint64, addv SMV) error {
		return makeSVM(addv, func(merger SVM) error {
			_ = collectOpsUnmerge(opindex, addv.Key(), "unm")

			return merger.Unmerge(opindex, addv.Value())
		})
	}

	unmergeJobAfterSubMerge := func(opindex, next uint64, addvs []SMV) error {
		var uaddvs []SMV

		switch next {
		case opindex:
			uaddvs = addvs
		default:
			switch _, a, found := getSMVs(next); {
			case !found:
				return errors.Errorf("unknown next, %d", next)
			default:
				uaddvs = a
			}
		}

		switch unmergeLock(next) {
		case -1:
			countJob(0)
		case 1:
			countJob(uint64(len(uaddvs)))

			for addv := range slices.Values(uaddvs) {
				if err := unmergeValue(next, addv); err != nil {
					return err
				}

				endCountJob()
			}
		}

		return nil
	}

	subMergeJob := func(opindex uint64, subv SMV, addvs []SMV, ignored bool) error {
		initialCountJob()

		return newJob(func(context.Context) error {
			var next uint64

			switch n, reason, err := subMerge(opindex, subv); {
			case err != nil:
				return err
			case reason == "":
				countJob(0)

				return nil
			default:
				if err := reportUnmerged(n, reason); err != nil {
					return err
				}

				next = n
			}

			if ignored && next == opindex {
				countJob(0)

				return nil
			}

			return unmergeJobAfterSubMerge(opindex, next, addvs)
		})
	}

	addMergeJob := func(opindex uint64, addvs []SMV) error {
		for addv := range slices.Values(addvs) {
			initialCountJob()

			if err := newJob(
				func(context.Context) error {
					defer countJob(0)

					return addMerge(opindex, addv)
				},
			); err != nil {
				return err
			}
		}

		return nil
	}

	unmergeJob := func(opindex uint64) error {
		if !ignoreLock(opindex) {
			return nil
		}

		switch _, addvs, found := getSMVs(opindex); {
		case !found:
			return errors.Errorf("wrong unmerge(ignore), %d", opindex)
		default:
			return newJob(func(context.Context) error {
				for addv := range slices.Values(addvs) {
					if err := unmergeValue(opindex, addv); err != nil {
						return err
					}
				}

				return nil
			})
		}
	}

	return func(opindex uint64, subv SMV, addvs []SMV) error {
			var ignored bool

			switch i, ok := mergeLock(opindex); {
			case !ok:
				return nil
			default:
				ignored = i
			}

			if subv != nil {
				_ = foundSubvs.CompareAndSwap(nil, true)

				if err := subMergeJob(opindex, subv, addvs, ignored); err != nil {
					return err
				}
			}

			if !ignored {
				if err := addMergeJob(opindex, addvs); err != nil {
					return err
				}
			}

			return nil
		},
		func() {
			_, _ = doneInitialCount()
		},
		unmergeJob,
		func() {
			_ = doneInitialUnmergeCountJob()
		},
		closeMergeLock,
		nil
}

func newFuncMergeLock(
	poolSize uint64,
) (
	func(uint64) (bool /* ignored */, bool), /* merge */
	func(uint64) int, /* unmerge */
	func(uint64) bool, /* ignore */
	func(),
	error,
) {
	var pool dutil.LockedMap[uint64, [3]bool]

	switch p, err := dutil.NewLockedMap[uint64, [3]bool](nil, poolSize); {
	case err != nil:
		return nil, nil, nil, nil, err
	default:
		pool = p
	}

	return func(opindex uint64) (bool, bool) {
			var ignored, ok bool

			_, _ = pool.Set(opindex, func(b [3]bool, _ bool) ([3]bool, error) {
				switch {
				case b[0], b[1]:
					return b, dutil.ErrLockedSetIgnore
				default:
					b[0] = true

					ignored = b[2]
					ok = true

					return b, nil
				}
			})

			return ignored, ok
		},
		func(opindex uint64) int {
			var r int

			_, _ = pool.Set(opindex, func(b [3]bool, _ bool) ([3]bool, error) {
				switch {
				case !b[0], b[1]:
					return b, dutil.ErrLockedSetIgnore
				case b[2]:
					r = -1

					return b, dutil.ErrLockedSetIgnore
				default:
					b[1] = true

					r = 1

					return b, nil
				}
			})

			return r
		},
		func(opindex uint64) bool {
			var ok bool

			_, _ = pool.Set(opindex, func(b [3]bool, _ bool) ([3]bool, error) {
				switch {
				case b[2]:
					return b, dutil.ErrLockedSetIgnore
				case !b[0], b[1]:
					b[2] = true

					return b, nil
				default:
					b[2] = true

					ok = true

					return b, nil
				}
			})

			return ok
		},
		pool.Close,
		nil
}

func newFuncSubMerge(
	makeSVM MergeSVMFunc,
	collectOpsMerge func(uint64, string, string) bool,
	collectOpsUnmerge func(uint64, string, string) bool,
) func(uint64, SMV) (uint64, string, error) {
	return func(opindex uint64, subv SMV) (uint64, string, error) {
		var next uint64
		var reason string

		return next, reason, makeSVM(subv, func(merger SVM) error {
			var err error

			next, reason, err = merger.SubMerge(opindex, subv.Value())
			if err == nil {
				switch {
				case reason == "":
					_ = collectOpsMerge(opindex, subv.Key(), "sub")
				case next == opindex:
					_ = collectOpsUnmerge(opindex, subv.Key(), "sub")
				default:
					_ = collectOpsMerge(opindex, subv.Key(), "sub")
					_ = collectOpsUnmerge(next, subv.Key(), "sub")
				}
			}

			return err
		})
	}
}
