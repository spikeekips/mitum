package main

import (
	"context"
	"slices"
	"sync"

	"github.com/rs/zerolog"
	"github.com/spikeekips/mitum/util"
	"github.com/spikeekips/mitum/util/logging"
)

type (
	newWorkerPhaseMergerFunc func(ctx context.Context, args *workerPhaseMergerArgs) (
		workerPhaseMergerInsertFunc,
		workerPhaseMergerWaitFunc,
		error,
	)
	newWorkerPhaseMergerFuncs func(phase int) (
		*workerPhaseMergerArgs,
		newWorkerPhaseMergerFunc,
		error,
	)
	workerMergerPhaseDoneFunc   func(phase int, worker string, unmerge []uint64) error
	setReasonsFunc              func(phase int, reasons map[uint64]string) error
	BroadcastPhaseDoneFunc      func(_ context.Context, phase int, newlyUnmerged []uint64) error // FIXME remove phase
	workerMergerUnmergedOpsFunc func(unmerged []uint64) error
	workerMergerWaitFunc        func(workerMergerUnmergedOpsFunc) error
	saveReasonsDoneFunc         func(unmerged []uint64, _ saveReasonFunc) error
	saveReasonsFuncs            func() (saveReasonFunc, func(error) error, error)
)

type workerMergerArgs struct {
	*logging.Logging
	NewWorkerPhaseMergerFuncs newWorkerPhaseMergerFuncs
	BroadcastPhaseDone        BroadcastPhaseDoneFunc
	SetReasons                setReasonsFunc
	PoolMapSize               uint64
	NewJobWorker              func(context.Context) (JobWorker, error)
	IsLocalOp                 func(opindex uint64) bool
	local                     string
}

func newWorkerMergerArgs() *workerMergerArgs {
	return &workerMergerArgs{
		Logging: logging.NewLogging(func(zctx zerolog.Context) zerolog.Context {
			return zctx.Str("module", "worker-merger")
		}),
		NewWorkerPhaseMergerFuncs: func(phase int) (
			*workerPhaseMergerArgs,
			newWorkerPhaseMergerFunc,
			error,
		) {
			return nil, nil, util.ErrNotImplemented.Errorf("NewWorkerPhaseMergerFuncs")
		},
		BroadcastPhaseDone: func(context.Context, int, []uint64) error {
			return util.ErrNotImplemented.Errorf("BroadcastPhaseDone")
		},
		SetReasons: func(int, map[uint64]string) error { // FIXME remove
			return util.ErrNotImplemented.Errorf("SetReasons")
		},
		PoolMapSize: 1 << 3,
		NewJobWorker: func(ctx context.Context) (JobWorker, error) {
			return NewBaseJobWorker(ctx, 1<<6)
		},
		IsLocalOp: func(opindex uint64) bool { return false },
	}
}

func newWorkerMerger(ctx context.Context, args *workerMergerArgs) (
	insert workerMergerPhaseDoneFunc,
	_ workerMergerWaitFunc,
	_ error,
) {
	wctx, wctxCancel := context.WithCancelCause(ctx)

	var insertFunc func(_ int, worker string, unmerge []uint64) error
	var insertPool func(worker string, unmerge []uint64) bool
	var exhaustPool func(func(worker string, unmerge []uint64) error) error

	switch i, e, err := newWorkerMergerPhasePoolFuncs(args.local, args.PoolMapSize); {
	case err != nil:
		wctxCancel(nil)

		return nil, nil, err
	default:
		insertPool = i
		exhaustPool = e
	}

	var phaseInsertFunc func(worker string, unmerge []uint64) error
	var phaseWait workerMergerWaitFunc

	switch i, w, err := newWorkerPhaseMergerFuncsForWorkerMerger(ctx, 0, args); {
	case err != nil:
		return nil, nil, err
	default:
		phaseInsertFunc = func(worker string, unmerge []uint64) error {
			return i(worker, nil, unmerge)
		}
		phaseWait = w
	}

	localIsReady := func() func(worker string, _ func(isReady bool) error) error {
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
	}()

	insertFunc = func(_ int, worker string, unmerge []uint64) error { // FIXME remove phase
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

		if worker == localWorkerName {
			_ = args.BroadcastPhaseDone(wctx, 0, unmerge)
		}

		if ok {
			return phaseInsertFunc(worker, unmerge)
		}

		return nil
	}

	waitDone := func() func(f workerMergerUnmergedOpsFunc) error {
		var waitDoneErr error
		var waitOnce sync.Once

		return func(f workerMergerUnmergedOpsFunc) error {
			waitOnce.Do(func() {
				waitDoneErr = func() error {
					defer exhaustPool(nil)

					return phaseWait(f)
				}()

				wctxCancel(waitDoneErr)
			})

			return waitDoneErr
		}
	}()

	return insertFunc, waitDone, nil
}

func newWorkerMergerPhasePoolFuncs(
	local string,
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
	phase int,
	args *workerMergerArgs,
) (
	workerPhaseMergerInsertFunc,
	workerMergerWaitFunc,
	error,
) {
	var wargs *workerPhaseMergerArgs
	var newf newWorkerPhaseMergerFunc

	switch a, n, err := args.NewWorkerPhaseMergerFuncs(phase); {
	case err != nil:
		return nil, nil, err
	default:
		wargs = a
		newf = n
	}

	wargs.local = args.local
	wargs.SetLogging(args.Logging)

	collectOps, filterMergeOps, collectOpsDone := newCollectUnmergedOps(args.local)

	wargs.FilterMergeOps = func(m, u []uint64) ([]uint64, []uint64) {
		return m, filterMergeOps(
			util.FilterSlice(u, args.IsLocalOp),
		)
	}

	var insert workerPhaseMergerInsertFunc
	var wait workerPhaseMergerWaitFunc

	switch i, w, err := newf(ctx, wargs); {
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

func newCollectUnmergedOps(local string) (
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

func newSaveReasonsFuncs(ctx context.Context) (
	setReasonsFunc,
	saveReasonsDoneFunc,
) {
	m := util.NewSingleLockedMap[uint64, [2]interface{}]()

	return func(phase int, reasons map[uint64]string) error {
			switch {
			case m.Len() < 1:
				_ = TraverseMap(reasons, func(opindex uint64, reason string) (bool, error) {
					_ = m.SetValue(opindex, [2]interface{}{phase, reason})

					return true, nil
				})
			default:
				_ = TraverseMap(reasons, func(opindex uint64, reason string) (bool, error) {
					_, _, _ = m.Set(opindex, func(prev [2]interface{}, found bool) (v [2]interface{}, _ error) {
						if found && phase <= prev[0].(int) && reason == prev[1].(string) {
							return v, util.ErrLockedSetIgnore
						}

						return [2]interface{}{phase, reason}, nil
					})

					return true, nil
				})
			}

			return nil
		},
		func(unmerged []uint64, f saveReasonFunc) error {
			defer m.Close()

			if m.Len() < 1 || len(unmerged) < 1 {
				return nil
			}

			var jw JobWorker

			switch i, err := NewBaseJobWorker(ctx, int64(m.Len())); {
			case err != nil:
				return err
			default:
				jw = i
			}

			var gerr error

			_ = m.Traverse(func(opindex uint64, v [2]interface{}) bool {
				gerr = jw.NewJob(func(context.Context) error {
					if slices.Index(unmerged, opindex) < 0 {
						return nil
					}

					return f(opindex, v[1].(string))
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
