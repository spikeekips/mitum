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
	workerMergerPhaseDoneFunc   func(phase int, worker string, mergeOps, unmergeOps []uint64) error
	setReasonsFunc              func(phase int, reasons map[uint64]string) error
	BroadcastPhaseDoneFunc      func(_ context.Context, phase int, newlyMergedOps, newlyUnmergedOps []uint64) error
	workerMergerUnmergedOpsFunc func(unmergedOps []uint64) error
	workerMergerWaitFunc        func(workerMergerUnmergedOpsFunc) error
	saveReasonsDoneFunc         func(unmergedOps []uint64, _ saveReasonFunc) error
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
		BroadcastPhaseDone: func(context.Context, int, []uint64, []uint64) error {
			return util.ErrNotImplemented.Errorf("BroadcastPhaseDone")
		},
		SetReasons: func(int, map[uint64]string) error {
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

	newPhaseCh := make(chan /* phase */ int, 1<<9)

	var insertPool func(phase int, worker string, mergeOps, unmergeOps []uint64) bool
	var insertFunc func(phase int, worker string, mergeOps, unmergeOps []uint64) error
	var exhaustPool func(phase int, _ func(worker string, mergeOps, unmergeOps []uint64) error) error
	var closePool func()

	switch i, e, c, err := newWorkerMergerPhasePoolFuncs(args.local, args.PoolMapSize); {
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

	collectOps, filterMergeOps, collectOpsDone := newCollectXMergedOps(args.local)

	var newPhase func(_ context.Context, phase int) error
	var lastPhase int = -1
	var phaseInsertFunc func(phase int, worker string, mergeOps, unmergeOps []uint64) error

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

		switch i, w, err := newWorkerPhaseMergerFuncsForWorkerMerger(ctx, phase, args, filterMergeOps); {
		case err != nil:
			l.Error().Err(err).Msg("< new phase")

			return err
		default:
			phaseInsertFunc = func(phase int, worker string, mergeOps, unmergeOps []uint64) error {
				collectOps(worker, mergeOps, unmergeOps)

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

			return phaseInsertFunc(phase, worker, mergeOps, unmergeOps)
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
					_ = args.BroadcastPhaseDone(wctx, nextPhase, newlyMergedOps, newlyUnmergedOps)

					if err := insertFunc(nextPhase, localWorkerName, newlyMergedOps, newlyUnmergedOps); err != nil {
						return err
					}

					if len(reasons) > 0 {
						if err := delayedWorker.NewJob(func(context.Context) error {
							return args.SetReasons(phase, reasons)
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

	insertFunc = func(phase int, worker string, mergeOps, unmergeOps []uint64) error {
		lock.RLock()
		defer lock.RUnlock()

		if phase > 1 { // FIXME remove
			panic("showme")
		}

		if phase == 0 && worker == localWorkerName {
			_ = args.BroadcastPhaseDone(wctx, 0, mergeOps, unmergeOps)
		}

		switch {
		case phase == lastPhase:
			return phaseInsertFunc(phase, worker, mergeOps, unmergeOps)
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
	var waitDonef func() error

	waitDone := func(f workerMergerUnmergedOpsFunc) error {
		waitOnce.Do(func() {
			waitDonef = NewContextDoneFunc(
				wctx,
				func(context.Context) error {
					defer collectOpsDone(nil)
					defer closePool()
					defer delayedWorker.Close()

					if err := phasesDone(); err != nil {
						return err
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
						return err
					}

					delayedWorker.JobDone()

					return delayedWorker.Wait()
				},
			)
		})

		err := waitDonef()
		wctxCancel(err)

		return err
	}

	return func(phase int, worker string, mergeOps, unmergeOps []uint64) error {
		if err := insertFunc(phase, worker, mergeOps, unmergeOps); err != nil {
			return err
		}

		if worker == localWorkerName {
			return newPhase(wctx, 0)
		}

		return nil
	}, waitDone, nil
}

func newWorkerMergerPhasePoolFuncs(
	local string,
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

				switch ops, found := prev[localWorkerName]; {
				case !found:
					return util.ErrNotFound.Errorf("local not found in phase pool")
				default:
					if err := f(localWorkerName, ops[0], ops[1]); err != nil {
						return err
					}
				}

				return TraverseMap(prev, func(worker string, ops [2][]uint64) (bool, error) {
					if worker == localWorkerName {
						return true, nil
					}

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
	filterOps func([]uint64, []uint64) ([]uint64, []uint64),
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

	wargs.local = args.local
	wargs.SetLogging(args.Logging)

	wargs.FilterMergeOps = func(m, u []uint64) ([]uint64, []uint64) {
		return filterOps(
			util.FilterSlice(m, args.IsLocalOp),
			util.FilterSlice(u, args.IsLocalOp),
		)
	}

	return newf(ctx, wargs)
}

func newCollectXMergedOps(local string) (
	insert func(worker string, mergedOps, unmergedOps []uint64),
	filter func(mergeOps, unmergeOps []uint64) ([]uint64, []uint64),
	done func(func(unmergedOps []uint64) error) error,
) {
	var l sync.RWMutex

	um := map[uint64]struct{}{}
	localUnmerged := map[uint64]struct{}{}

	return func(worker string, mergedOps, unmergedOps []uint64) {
			l.Lock()
			defer l.Unlock()

			um = collectXMergedOps(um, mergedOps, unmergedOps)

			if worker == localWorkerName {
				localUnmerged = collectXMergedOps(localUnmerged, mergedOps, unmergedOps)
			}
		},
		func(mergeOps, unmergeOps []uint64) (fm, fu []uint64) {
			l.RLock()
			defer l.RUnlock()

			if len(localUnmerged) < 1 {
				return mergeOps, unmergeOps
			}

			if len(mergeOps) > 0 {
				add, done, _ := util.CompactAppendSliceV1[uint64](uint64(len(mergeOps)))

				_ = util.TraverseSliceV1(mergeOps, func(_ int, opindex uint64) (bool, error) {
					if _, found := localUnmerged[opindex]; found {
						add(opindex)
					}

					return true, nil
				})

				_ = done(func(s []uint64) error {
					fm = slices.Clone(s)

					return nil
				})
			}

			if len(unmergeOps) > 0 {
				add, done, _ := util.CompactAppendSliceV1[uint64](uint64(len(unmergeOps)))

				_ = util.TraverseSliceV1(unmergeOps, func(_ int, opindex uint64) (bool, error) {
					if _, found := localUnmerged[opindex]; !found {
						add(opindex)
					}

					return true, nil
				})

				_ = done(func(s []uint64) error {
					fu = slices.Clone(s)

					return nil
				})
			}

			return fm, fu
		},
		func(f func([]uint64) error) error {
			l.Lock()
			defer l.Unlock()

			defer func() {
				clear(um)
				clear(localUnmerged)
			}()

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

func collectXMergedOps(um map[uint64]struct{}, mergedOps, unmergedOps []uint64) map[uint64]struct{} {
	if len(um) > 0 {
		_ = util.TraverseSliceV1(mergedOps, func(_ int, i uint64) (bool, error) {
			if _, found := um[i]; found {
				delete(um, i)
			}

			return true, nil
		})
	}

	_ = util.TraverseSliceV1(unmergedOps, func(_ int, i uint64) (bool, error) {
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
		func(unmergedOps []uint64, f saveReasonFunc) error {
			defer m.Close()

			if m.Len() < 1 || len(unmergedOps) < 1 {
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
					if slices.Index(unmergedOps, opindex) < 0 {
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
