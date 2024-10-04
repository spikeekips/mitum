package main

import (
	"context"
	"slices"
	"sync"

	"github.com/spikeekips/mitum/util"
	"github.com/spikeekips/mitum/util/logging"
)

type (
	workerPhaseMergerFuncs func() (
		*workerPhaseMergerArgs,
		workerPhaseMergerFunc,
		error,
	)
	workerMergerPhaseDoneFunc   func(worker string, unmerge []uint64) error
	workerMergerUnmergedOpsFunc func(unmerged []uint64) error
	workerMergerWaitFunc        func(workerMergerUnmergedOpsFunc) error
	workerMergerIsLocalOpFunc   func(opindex uint64) bool
	baseWorkerMergerFunc        func(
		_ context.Context,
		minEnough uint64,
		_ phaseMergerGetStateMergeValuesFunc,
		_ phaseMergerGetMergers,
		_ extractStateMergeValueKeysFunc,
		_ workerMergerIsLocalOpFunc,
	) (
		workerMergerPhaseDoneFunc,
		workerMergerWaitFunc,
		error,
	)
)

type baseWorkerMergerArgs struct {
	*logging.Logging
	PoolMapSize  uint64
	NewJobWorker newJobWorkerFunc
}

func newBaseWorkerMergerArgs() *baseWorkerMergerArgs {
	return &baseWorkerMergerArgs{
		Logging:      NewModuleLogging("base-worker-merger", nil),
		PoolMapSize:  1 << 3,
		NewJobWorker: NewFuncBaseJobWorker(1 << 6),
	}
}

func newFuncBaseWorkerMerger(
	args *baseWorkerMergerArgs,
	wpargs *workerPhaseMergerArgs,
	pargs *phaseMergerArgs,
) baseWorkerMergerFunc {
	newWorkerPhaseMergerf := newFuncWorkerPhaseMerger(wpargs, pargs)

	return func(
		ctx context.Context,
		minEnough uint64,
		getStateMergeValues phaseMergerGetStateMergeValuesFunc,
		getMergers phaseMergerGetMergers,
		extractStateMergeValueKeys extractStateMergeValueKeysFunc,
		isLocalOp workerMergerIsLocalOpFunc,
	) (
		workerMergerPhaseDoneFunc,
		workerMergerWaitFunc,
		error,
	) {
		wctx, wctxCancel := ContextWithCancelCause(ctx)

		var insertPool func(worker string, unmerge []uint64) bool
		var exhaustPool func(func(worker string, unmerge []uint64) error) error

		switch i, e, err := newWorkerMergerPhasePoolFuncs(args.PoolMapSize); {
		case err != nil:
			return nil, nil, err
		default:
			insertPool = i
			exhaustPool = e
		}

		var phaseInsertFunc func(worker string, unmerge []uint64) error
		var phaseWait workerMergerWaitFunc

		switch i, w, err := newWorkerPhaseMergerFuncsForWorkerMerger(
			wctx,
			newWorkerPhaseMergerf,
			minEnough,
			getStateMergeValues,
			getMergers,
			extractStateMergeValueKeys,
			isLocalOp,
		); {
		case err != nil:
			return nil, nil, err
		default:
			phaseInsertFunc = func(worker string, unmerge []uint64) error {
				args.Log().Debug().
					Str("worker", worker).
					Uints64("unmerge", unmerge).
					Msg("phase done")

				return i(worker, nil, unmerge)
			}
			phaseWait = w
		}

		localIsReady := newLocalWorkerIsReady()

		workerPhaseDone := func(worker string, unmerge []uint64) error {
			var ok bool

			if err := localIsReady(worker, func(isReady bool) error {
				if !isReady {
					_ = insertPool(worker, unmerge)

					return nil
				}

				if worker == localWorkerName {
					_ = insertPool(worker, unmerge)

					return exhaustPool(phaseInsertFunc)
				}

				ok = true

				return nil
			}); err != nil {
				return err
			}

			if ok {
				return phaseInsertFunc(worker, unmerge)
			}

			return nil
		}

		return func(worker string, unmerge []uint64) error {
				return contextCauseError(wctx, dummyErrorFunc(), func(context.Context) error {
					if err := workerPhaseDone(worker, unmerge); err != nil {
						args.Log().Error().
							Err(err).
							Str("worker", worker).
							Msg("worker phase done")

						return err
					}

					return nil
				})
			},
			func(f workerMergerUnmergedOpsFunc) error {
				return contextCauseError(wctx, wctxCancel, func(context.Context) error {
					defer exhaustPool(nil)

					return phaseWait(f)
				})
			},
			nil
	}
}

func newWorkerMergerPhasePoolFuncs(
	mapSize uint64,
) (
	phaseDone func(worker string, unmerge []uint64) bool,
	exhaust func(func(worker string, unmerge []uint64) error) error,
	_ error,
) {
	var m *util.ShardedMap[string, []uint64]

	switch i, err := util.NewShardedMap[string, []uint64](mapSize, nil); {
	case err != nil:
		return nil, nil, err
	default:
		m = i
	}

	return func(worker string, unmerge []uint64) bool {
			var created bool

			_, _, _ = m.Set(worker, func(_ []uint64, found bool) ([]uint64, error) {
				if found {
					return nil, util.ErrLockedSetIgnore
				}

				created = true

				return unmerge, nil
			})

			return created
		},
		func(f func(worker string, unmerge []uint64) error) (gerr error) {
			defer m.Close()

			if f == nil {
				return nil
			}

			switch unmerged, found := m.Value(localWorkerName); {
			case !found:
				return util.ErrNotFound.Errorf("local not found in unmerged pool")
			default:
				if err := f(localWorkerName, unmerged); err != nil {
					return err
				}
			}

			_ = m.Traverse(func(worker string, unmerged []uint64) bool {
				if worker == localWorkerName {
					return true
				}

				gerr = f(worker, unmerged)

				return gerr == nil
			})

			return gerr
		},
		nil
}

func newWorkerPhaseMergerFuncsForWorkerMerger(
	ctx context.Context,
	newf workerPhaseMergerFunc,
	minEnough uint64,
	getStateMergeValues phaseMergerGetStateMergeValuesFunc,
	getMergers phaseMergerGetMergers,
	extractStateMergeValueKeys extractStateMergeValueKeysFunc,
	isLocalOp workerMergerIsLocalOpFunc,
) (
	workerPhaseMergerInsertFunc,
	workerMergerWaitFunc,
	error,
) {
	collectOps, filterMergeOps, collectOpsDone := newCollectUnmergedOps()

	var insert workerPhaseMergerInsertFunc
	var wait workerPhaseMergerWaitFunc

	switch i, w, err := newf(
		ctx,
		minEnough,
		getStateMergeValues,
		getMergers,
		extractStateMergeValueKeys,
		func(m, u []uint64) ([]uint64, []uint64) {
			return m, filterMergeOps(
				util.FilterSlice(u, isLocalOp),
			)
		},
	); {
	case err != nil:
		return nil, nil, err
	default:
		insert = i
		wait = w
	}

	return func(worker string, _, unmerge []uint64) error {
			collectOps(worker, unmerge)

			return insert(worker, nil, unmerge)
		},
		func(f workerMergerUnmergedOpsFunc) error {
			defer collectOpsDone(nil)

			if err := wait(nil); err != nil {
				return err
			}

			if f == nil {
				return nil
			}

			return collectOpsDone(func(unmerged []uint64) error {
				return f(unmerged)
			})
		},
		nil
}

func newCollectUnmergedOps() (
	insert func(worker string, unmerged []uint64),
	filter func(unmerge []uint64) []uint64,
	done func(func(unmerged []uint64) error) error,
) {
	var l sync.RWMutex

	um := map[uint64]struct{}{}
	localUnmerged := map[uint64]struct{}{}

	return func(worker string, unmerged []uint64) {
			l.Lock()
			defer l.Unlock()

			um = collectUnmergedOps(um, unmerged)

			if worker == localWorkerName {
				localUnmerged = collectUnmergedOps(localUnmerged, unmerged)
			}
		},
		func(unmerge []uint64) (filtered []uint64) {
			l.RLock()
			defer l.RUnlock()

			if len(localUnmerged) < 1 {
				return unmerge
			}

			if len(unmerge) > 0 {
				add, done, _ := util.CompactAppendSliceV1[uint64](uint64(len(unmerge)))

				_ = util.TraverseSliceV1(unmerge, func(_ int, opindex uint64) (bool, error) {
					if _, found := localUnmerged[opindex]; !found {
						add(opindex)
					}

					return true, nil
				})

				_ = done(func(s []uint64) error {
					filtered = slices.Clone(s)

					return nil
				})
			}

			return filtered
		},
		func(f func([]uint64) error) error {
			l.Lock()
			defer l.Unlock()

			defer func() {
				clear(um)
				clear(localUnmerged)
			}()

			if f == nil {
				return nil
			}

			if len(um) < 1 {
				return f(nil)
			}

			unmerged := make([]uint64, len(um))
			defer clear(unmerged)

			var i int

			_ = TraverseMap(um, func(opindex uint64, _ struct{}) (bool, error) {
				unmerged[i] = opindex
				i++

				return true, nil
			})

			return f(unmerged)
		}
}

func collectUnmergedOps(um map[uint64]struct{}, unmerged []uint64) map[uint64]struct{} {
	_ = util.TraverseSliceV1(unmerged, func(_ int, i uint64) (bool, error) {
		if _, found := um[i]; !found {
			um[i] = struct{}{}
		}

		return true, nil
	})

	return um
}

func newLocalWorkerIsReady() func(worker string, _ func(isReady bool) error) error {
	var ready bool
	var l sync.Mutex

	return func(worker string, f func(isReady bool) error) error {
		l.Lock()
		defer l.Unlock()

		if !ready && worker == localWorkerName {
			ready = true
		}

		return f(ready)
	}
}

func newWorkerMergerIsLocalOp(f func(uint64) bool) workerMergerIsLocalOpFunc {
	return func(opindex uint64) bool {
		return f(opindex)
	}
}
