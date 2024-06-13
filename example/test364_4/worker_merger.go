package main

import (
	"context"
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
	workerMergerPhaseDoneFunc   func(phase int, worker string, mergeOps, unmergeOps []uint64) error
	UpdateReasonsFunc           func(_ context.Context, phase int, reasons map[uint64]string) error
	BroadcastPhaseDoneFunc      func(_ context.Context, phase int, newlyMergedOps, newlyUnmergedOps []uint64) error
	workerMergerUnmergedOpsFunc func(unmergedOps []uint64) error
	workerMergerWaitFunc        func(workerMergerUnmergedOpsFunc) error
)

type workerMergerArgs struct {
	*logging.Logging
	NewWorkerPhaseMergerFuncs newWorkerPhaseMergerFuncs
	BroadcastPhaseDone        BroadcastPhaseDoneFunc
	UpdateReasons             UpdateReasonsFunc
	PoolMapSize               uint64
	NewJobWorker              func(context.Context) (JobWorker, error)
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
		BroadcastPhaseDone: func(context.Context, int, []uint64, []uint64) error {
			return util.ErrNotImplemented.Errorf("BroadcastPhaseDone")
		},
		UpdateReasons: func(context.Context, int, map[uint64]string) error {
			return util.ErrNotImplemented.Errorf("UpdateReasons")
		},
		PoolMapSize: 1 << 3,
		NewJobWorker: func(ctx context.Context) (JobWorker, error) {
			return NewBaseJobWorker(ctx, 1<<6)
		},
	}
}

func newWorkerMerger(ctx context.Context, args *workerMergerArgs) (
	insert workerMergerPhaseDoneFunc,
	_ workerMergerWaitFunc,
	_ error,
) {
	wctx, wctxCancel := context.WithCancelCause(ctx)

	newPhaseCh := make(chan /* phase */ int)

	var insertPool func(phase int, worker string, mergeOps, unmergeOps []uint64) bool
	var insertFunc func(phase int, worker string, mergeOps, unmergeOps []uint64) workerPhaseMergerInsertFunc
	var exhaustPool func(phase int, _ func(worker string, mergeOps, unmergeOps []uint64) error) error
	var closePool func()

	switch i, e, c, err := newWorkerMergerPhasePoolFuncs(args.PoolMapSize); {
	case err != nil:
		wctxCancel(nil)

		return nil, nil, err
	default:
		insertPool = i
		exhaustPool = e
		closePool = c
	}

	var delayedWorker JobWorker

	switch i, err := args.NewJobWorker(wctx); {
	case err != nil:
		defer closePool()
		wctxCancel(nil)

		return nil, nil, err
	default:
		delayedWorker = i
	}

	collectOps, collectOpsDone := newCollectXMergedOps()
	updateReasons, updateReasonsDone := newUpdateReasonsFuncs(args.UpdateReasons)

	var newPhase func(_ context.Context, phase int) error
	var lastPhase int = -1
	var phaseInsertFunc workerPhaseMergerInsertFunc

	var lock sync.RWMutex

	newPhase = func(ctx context.Context, phase int) error {
		lock.Lock()
		defer lock.Unlock()

		l := args.Log().With().Int("phase", phase).Logger()

		if phase <= lastPhase {
			l.Debug().Msg("new phase, but known")

			return nil
		}

		l.Debug().Msg("> new phase")

		var phaseWait workerPhaseMergerWaitFunc

		switch i, w, err := newWorkerPhaseMergerFuncsForWorkerMerger(ctx, phase, args); {
		case err != nil:
			l.Error().Err(err).Msg("< new phase")

			return err
		default:
			phaseInsertFunc = func(worker string, mergeOps, unmergeOps []uint64) error {
				collectOps(mergeOps, unmergeOps)

				if phase == 0 && worker == localWorkerName {
					return i(worker, nil, nil)
				}

				return i(worker, mergeOps, unmergeOps)
			}
			phaseWait = w
			lastPhase = phase
		}

		if err := exhaustPool(phase, func(worker string, mergeOps, unmergeOps []uint64) error {
			l.Debug().
				Str("worker", worker).
				Uints64("merge_ops", mergeOps).
				Uints64("unmerge_ops", unmergeOps).
				Msg("exhaust")

			return phaseInsertFunc(worker, mergeOps, unmergeOps)
		}); err != nil {
			l.Error().Err(err).Msg("< new phase")

			return err
		}

		go func() {
			nextPhase := phase + 1

			if err := phaseWait(func(newlyMergedOps, newlyUnmergedOps []uint64, reasons map[uint64]string, isDone bool) error {
				l.Debug().
					Uints64("merge_ops", newlyMergedOps).
					Uints64("unmerge_ops", newlyUnmergedOps).
					Interface("reasons", reasons).
					Bool("is_done", isDone).
					Msg("phase wait done")

				if !isDone {
					if err := args.BroadcastPhaseDone(wctx, nextPhase, newlyMergedOps, newlyUnmergedOps); err != nil {
						return err
					}

					if f := insertFunc(nextPhase, localWorkerName, newlyMergedOps, newlyUnmergedOps); f != nil {
						if err := f(localWorkerName, newlyMergedOps, newlyUnmergedOps); err != nil {
							return err
						}
					}

					if len(reasons) > 0 {
						if err := delayedWorker.NewJob(func(ctx context.Context) error {
							return updateReasons(ctx, phase, reasons)
						}); err != nil {
							return err
						}
					}
				}

				if isDone {
					nextPhase = -1
				}

				return nil
			}); err != nil {
				wctxCancel(err)

				return
			}

			newPhaseCh <- nextPhase
		}()

		return nil
	}

	if err := newPhase(wctx, 0); err != nil {
		wctxCancel(err)
		closePool()
		delayedWorker.Close()
		updateReasonsDone()

		return nil, nil, err
	}

	insertFunc = func(phase int, worker string, mergeOps, unmergeOps []uint64) workerPhaseMergerInsertFunc {
		lock.RLock()
		defer lock.RUnlock()

		switch {
		case phase == lastPhase:
			return phaseInsertFunc
		case phase > lastPhase:
			_ = insertPool(phase, worker, mergeOps, unmergeOps)
		}

		return nil
	}

	phasesDone := NewContextDoneFunc(
		wctx,
		func(ctx context.Context) error {
			for {
				select {
				case <-ctx.Done():
					return context.Cause(ctx)
				case phase := <-newPhaseCh:
					if phase < 0 {
						return nil
					}

					if err := newPhase(ctx, phase); err != nil {
						return err
					}
				}
			}
		},
	)

	go func() {
		_ = phasesDone()
	}()

	var waitOnce sync.Once
	var werr error

	return func(phase int, worker string, mergeOps, unmergeOps []uint64) error {
			if f := insertFunc(phase, worker, mergeOps, unmergeOps); f != nil {
				return f(worker, mergeOps, unmergeOps)
			}

			return nil
		},
		func(f workerMergerUnmergedOpsFunc) error {
			waitOnce.Do(func() {
				defer updateReasonsDone()
				defer collectOpsDone(nil)
				defer closePool()
				defer delayedWorker.Close()
				defer wctxCancel(nil)

				if err := phasesDone(); err != nil {
					werr = err

					return
				}

				if err := delayedWorker.NewJob(func(ctx context.Context) error {
					return collectOpsDone(func(unmergedOps []uint64) error {
						args.Log().Debug().Uints64("unmerged_ops", unmergedOps).Msg("unmerged ops collected")

						if f == nil {
							return nil
						}

						return f(unmergedOps)
					})
				}); err != nil {
					werr = err

					return
				}

				delayedWorker.JobDone()

				werr = delayedWorker.Wait()
			})

			return werr
		},
		nil
}

func newWorkerMergerPhasePoolFuncs(
	mapSize uint64,
) (
	phaseDone func(phase int, worker string, mergeOps, unmergeOps []uint64) bool,
	exhaust func(phase int, _ func(worker string, mergeOps, unmergeOps []uint64) error) error,
	_ func(),
	_ error,
) {
	var m *util.ShardedMap[int, map[string][2][]uint64]

	switch i, err := util.NewShardedMap[int, map[string][2][]uint64](mapSize, nil); {
	case err != nil:
		return nil, nil, nil, err
	default:
		m = i
	}

	return func(phase int, worker string, mergeOps, unmergeOps []uint64) bool {
			var created bool

			_, _, _ = m.Set(phase, func(prev map[string][2][]uint64, found bool) (map[string][2][]uint64, error) {
				switch {
				case found:
					if _, found := prev[worker]; found {
						return nil, util.ErrLockedSetIgnore
					}
				default:
					prev = map[string][2][]uint64{}
				}

				prev[worker] = [2][]uint64{mergeOps, unmergeOps}
				created = true

				return prev, nil
			})

			return created
		},
		func(phase int, f func(worker string, mergeOps, unmergeOps []uint64) error) error {
			defer m.RemoveValue(phase)

			return m.Get(phase, func(prev map[string][2][]uint64, found bool) error {
				if !found {
					return nil
				}

				return TraverseMap(prev, func(worker string, ops [2][]uint64) (bool, error) {
					return true, f(worker, ops[0], ops[1])
				})
			})
		},
		func() {
			m.Close()
		},
		nil
}

func newWorkerPhaseMergerFuncsForWorkerMerger(
	ctx context.Context,
	phase int,
	args *workerMergerArgs,
) (
	workerPhaseMergerInsertFunc,
	workerPhaseMergerWaitFunc,
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

	wargs.SetLogging(args.Logging)

	return newf(ctx, wargs)
}

func newCollectXMergedOps() (
	insert func(mergedOps, unmergedOps []uint64),
	done func(func(unmergedOps []uint64) error) error,
) {
	var l sync.RWMutex

	um := map[uint64]struct{}{}

	return func(mergedOps, unmergedOps []uint64) {
			l.Lock()
			defer l.Unlock()

			_ = util.TraverseSliceV1(mergedOps, func(_ int, i uint64) (bool, error) {
				if _, found := um[i]; found {
					delete(um, i)
				}

				return true, nil
			})

			_ = util.TraverseSliceV1(unmergedOps, func(_ int, i uint64) (bool, error) {
				if _, found := um[i]; !found {
					um[i] = struct{}{}
				}

				return true, nil
			})
		},
		func(f func([]uint64) error) error {
			l.RLock()
			defer l.RUnlock()

			defer clear(um)

			if len(um) < 1 {
				if f == nil {
					return nil
				}

				return f(nil)
			}

			var add util.CompactAppendSliceAddFunc[uint64]
			var done util.CompactAppendSliceDoneFunc[uint64]

			switch a, d, err := util.CompactAppendSliceV1[uint64](uint64(len(um))); {
			case err != nil:
				return err
			default:
				add = a
				done = d
			}

			_ = TraverseMap(um, func(i uint64, _ struct{}) (bool, error) {
				add(i)

				return true, nil
			})

			return done(f)
		}
}

func newUpdateReasonsFuncs(
	f UpdateReasonsFunc,
) (
	func(_ context.Context, phase int, reasons map[uint64]string) error,
	func(),
) {
	m := util.NewSingleLockedMap[uint64, [2]interface{}]()

	return func(ctx context.Context, phase int, reasons map[uint64]string) error {
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
							delete(reasons, opindex)

							return v, util.ErrLockedSetIgnore
						}

						return [2]interface{}{phase, reason}, nil
					})

					return true, nil
				})
			}

			return f(ctx, phase, reasons)
		},
		m.Close
}
