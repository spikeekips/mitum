package main

import (
	"context"
	"sync"

	"github.com/pkg/errors"
	"github.com/rs/zerolog"
	"github.com/spikeekips/mitum/util"
	"github.com/spikeekips/mitum/util/logging"
)

type (
	PhaseJobWorkerNewJobFunc func(jobID uint64, _ PhaseJobWorkerCallback) error
	PhaseJobWorkerCallback   func(_ context.Context, phase int, _ PhaseJobWorkerNewJobFunc) error
	NewPhaseJobWorkerFunc    func(context.Context) (
		newJob PhaseJobWorkerNewJobFunc,
		jobDone func(),
		wait func() error,
		_ error,
	)
)

func newPhaseJobWorker(
	ctx context.Context,
	newJobWorker func(context.Context) (JobWorker, error),
	phase int,
	jobIDLocked func(jobID uint64, phase int) bool,
) (
	newJob PhaseJobWorkerNewJobFunc,
	jobDone func(),
	wait func() error,
	_ error,
) {
	var wk JobWorker

	switch i, err := newJobWorker(ctx); {
	case err != nil:
		return nil, nil, nil, err
	default:
		wk = i
	}

	var addNext util.CompactAppendSliceAddFunc[[2]interface{}]
	var doneNext util.CompactAppendSliceDoneFunc[[2]interface{}]

	switch a, d, err := util.CompactAppendSliceV1[[2]interface{}](1<<9, 1<<9); { // FIXME size from argument
	case err != nil:
		return nil, nil, nil, err
	default:
		addNext = a
		doneNext = d
	}

	nextPhaseNewJob := func(jobID uint64, c PhaseJobWorkerCallback) error {
		addNext([2]interface{}{jobID, c})

		return nil
	}

	doNext := func() error {
		return doneNext(func(s [][2]interface{}) error {
			if len(s) < 1 {
				return nil
			}

			switch n, d, w, err := newPhaseJobWorker(ctx, newJobWorker, phase+1, jobIDLocked); {
			case err != nil:
				return err
			default:
				if err := util.TraverseSliceV1(s, func(_ int, i [2]interface{}) (bool, error) {
					return true, n(
						i[0].(uint64),
						i[1].(PhaseJobWorkerCallback),
					)
				}); err != nil {
					return err
				}

				d()

				return w()
			}
		})
	}

	return func(jobID uint64, c PhaseJobWorkerCallback) error {
			return wk.NewJob(func(ctx context.Context) error {
				if !jobIDLocked(jobID, phase) {
					return nil
				}

				return c(ctx, phase, nextPhaseNewJob)
			})
		},
		wk.JobDone,
		func() error {
			if err := wk.Wait(); err != nil {
				return err
			}

			return doNext()
		},
		nil
}

func NewPhaseJobWorker(
	ctx context.Context,
	newJobWorker func(context.Context) (JobWorker, error),
	mapSize uint64,
) (
	newJob PhaseJobWorkerNewJobFunc,
	jobDone func(),
	wait func() error,
	_ error,
) {
	if newJobWorker == nil {
		newJobWorker = func(ctx context.Context) (JobWorker, error) {
			return NewBaseJobWorker(ctx, 1<<9)
		}
	}

	var jobIDLocked *util.ShardedMap[uint64, int] // FIXME remove

	switch i, err := util.NewShardedMap[uint64, int](mapSize, nil); {
	case err != nil:
		return nil, nil, nil, err
	default:
		jobIDLocked = i
	}

	pctx, pctxCancel := context.WithCancelCause(ctx)

	var newJobFunc func(uint64, PhaseJobWorkerCallback) error
	var doneFunc func()
	var waitFunc func() error

	switch n, d, w, err := newPhaseJobWorker(
		pctx,
		newJobWorker,
		0,
		func(jobID uint64, phase int) (ok bool) {
			_, _, _ = jobIDLocked.Set(jobID, func(prev int, found bool) (int, error) {
				if found && phase <= prev {
					return 0, util.ErrLockedSetIgnore
				}

				ok = true

				return phase, nil
			})

			return ok
		},
	); {
	case err != nil:
		pctxCancel(err)

		return nil, nil, nil, err
	default:
		newJobFunc = n
		doneFunc = d
		waitFunc = w
	}

	go func() {
		<-pctx.Done()

		jobIDLocked.Close()
	}()

	return func(jobID uint64, c PhaseJobWorkerCallback) error {
			if err := newJobFunc(jobID, c); err != nil {
				pctxCancel(err)

				return err
			}

			return nil
		},
		doneFunc,
		func() error {
			err := waitFunc()
			pctxCancel(err)

			return err
		}, nil
}

type PhaseJobWorkerArgs struct {
	NewJobWorker       func(context.Context) (JobWorker, error)
	NewJobPoolSize     uint64
	NewJobPoolGrowSize uint64
	JobIDMapSize       uint64
	*logging.Logging
}

func NewPhaseJobWorkerArgs() *PhaseJobWorkerArgs {
	return &PhaseJobWorkerArgs{
		Logging: logging.NewLogging(func(zctx zerolog.Context) zerolog.Context {
			return zctx.Str("module", "phase-job-worker")
		}),
		NewJobWorker: func(ctx context.Context) (JobWorker, error) {
			return NewBaseJobWorker(ctx, 1<<9)
		},
		NewJobPoolSize:     1 << 9,
		NewJobPoolGrowSize: 1 << 9,
		JobIDMapSize:       1 << 9,
	}
}

func newPhaseJobWorker2(
	ctx context.Context,
	args *PhaseJobWorkerArgs,
	phase int,
	prevWait func() error,
) (
	newJob PhaseJobWorkerNewJobFunc,
	jobDone func(),
	wait func() error,
	_ error,
) {
	pctx, pctxCancel := context.WithCancelCause(ctx)

	var jobIDLocked *util.ShardedMap[uint64, int]

	switch i, err := util.NewShardedMap[uint64, int](args.JobIDMapSize, nil); {
	case err != nil:
		return nil, nil, nil, err
	default:
		jobIDLocked = i
	}

	filterJobID := func(jobID uint64, phase int) (ok bool) {
		_, _, _ = jobIDLocked.Set(jobID, func(prev int, found bool) (int, error) {
			if found && phase == prev {
				return 0, util.ErrLockedSetIgnore
			}

			ok = true

			return phase, nil
		})

		return ok
	}

	l := args.Log().With().Int("phase", phase).Logger()

	var wk JobWorker

	switch i, err := args.NewJobWorker(pctx); {
	case err != nil:
		return nil, nil, nil, err
	default:
		wk = i
	}

	_, waitDone, _ := NewContextDoneFunc(pctx, func(context.Context) error {
		return wk.Wait()
	})

	var nextPhaseLock sync.RWMutex
	var nextPhaseNewJobFunc PhaseJobWorkerNewJobFunc
	var nextPhaseWaitFunc func() error

	newNextPhase := func() error {
		nextPhaseLock.Lock()
		defer nextPhaseLock.Unlock()

		if nextPhaseNewJobFunc != nil {
			return nil
		}

		l.Debug().Int("phase", phase).Msg("new phase job worker")

		switch n, d, w, err := newPhaseJobWorker2(pctx, args, phase+1, waitDone); {
		case err != nil:
			return err
		default:
			nextPhaseNewJobFunc = n
			nextPhaseWaitFunc = func() error {
				d()

				return w()
			}

			return nil
		}
	}

	nextPhaseNewJobCh := make(chan [2]interface{}) // FIXME with map size

	nextPhaseNewJob := func(jobID uint64, c PhaseJobWorkerCallback) error {
		if c == nil {
			return nil
		}

		nextPhaseNewJobCh <- [2]interface{}{jobID, c}

		l.Debug().Uint64("job_id", jobID).Msg("next phase new job to ch")

		return nil
	}

	nextPhaseNewJobDoneCh := make(chan struct{})

	go func() {
		if err := newPhaseJobWorkerNewJobsPool(
			pctx,
			args,
			phase,
			newNextPhase,
			func() PhaseJobWorkerNewJobFunc {
				nextPhaseLock.RLock()
				defer nextPhaseLock.RUnlock()

				return nextPhaseNewJobFunc
			},
			func(f func(jobID uint64, c PhaseJobWorkerCallback) error) error {
				for {
					select {
					case <-pctx.Done():
						return pctx.Err()
					case i, notClosed := <-nextPhaseNewJobCh:
						if !notClosed {
							return nil
						}

						if err := f(
							i[0].(uint64),
							i[1].(PhaseJobWorkerCallback),
						); err != nil {
							return err
						}
					}
				}
			},
			prevWait,
		); err != nil {
			pctxCancel(err)
		}

		close(nextPhaseNewJobDoneCh)
	}()

	var closeOnce sync.Once

	closeCh := func() {
		closeOnce.Do(func() {
			close(nextPhaseNewJobCh)
		})
	}

	nextPhaseWait := func() error {
		nextPhaseLock.Lock()
		defer nextPhaseLock.Unlock()

		if nextPhaseWaitFunc == nil {
			return nil
		}

		return nextPhaseWaitFunc()
	}

	return func(jobID uint64, c PhaseJobWorkerCallback) error {
			if c == nil {
				return nil
			}

			return wk.NewJob(func(ctx context.Context) error {
				if !filterJobID(jobID, phase) {
					return nil
				}

				return c(ctx, phase, nextPhaseNewJob)
			})
		},
		wk.JobDone,
		func() error {
			defer pctxCancel(nil)
			defer closeCh()
			defer jobIDLocked.Close()

			if err := waitDone(); err != nil {
				return err
			}

			closeCh()

			select {
			case <-pctx.Done():
				return errors.WithStack(context.Cause(pctx))
			case <-nextPhaseNewJobDoneCh:
			}

			return nextPhaseWait()
		},
		nil
}

func newPhaseJobWorkerNewJobsPool(
	ctx context.Context,
	args *PhaseJobWorkerArgs,
	phase int,
	newNextPhase func() error,
	nextPhaseNewJobFunc func() PhaseJobWorkerNewJobFunc,
	newJobs func(func(jobID uint64, c PhaseJobWorkerCallback) error) error,
	prevDonef func() error,
) error {
	l := args.Log().With().Int("phase", phase).Logger()

	var addNext util.CompactAppendSliceAddFunc[[2]interface{}]
	var doneNext util.CompactAppendSliceDoneFunc[[2]interface{}]

	switch a, d, err := util.CompactAppendSliceV1[[2]interface{}](args.NewJobPoolSize, args.NewJobPoolGrowSize); {
	case err != nil:
		return err
	default:
		addNext = a
		doneNext = d
	}

	isAdded := util.EmptyLocked[bool]()

	var exhaustOnce sync.Once

	exhaust := func() error {
		var gerr error

		exhaustOnce.Do(func() {
			if err := newNextPhase(); err != nil {
				gerr = err

				return
			}

			l.Debug().Msg("exhaust")

			f := nextPhaseNewJobFunc()

			switch added, _ := isAdded.Value(); {
			case !added:
				_ = doneNext(func([][2]interface{}) error { return nil })
			default:
				if err := doneNext(func(s [][2]interface{}) error {
					if len(s) < 1 {
						return nil
					}

					return util.TraverseSliceV1(s, func(_ int, i [2]interface{}) (bool, error) {
						return true, f(
							i[0].(uint64),
							i[1].(PhaseJobWorkerCallback),
						)
					})
				}); err != nil {
					gerr = err
				}
			}
		})

		return gerr
	}

	_, prevDone, _ := NewContextDoneFunc(
		ctx,
		func(context.Context) error {
			if err := prevDonef(); err != nil {
				return err
			}

			if added, _ := isAdded.Value(); added {
				l.Debug().Msg("new sub phase worker before ch closed")

				return exhaust()
			}

			return nil
		},
	)

	_, newJobsDone, _ := NewContextDoneFunc(
		ctx,
		func(context.Context) error {
			return newJobs(func(jobID uint64, c PhaseJobWorkerCallback) error {
				if f := nextPhaseNewJobFunc(); f != nil {
					l.Debug().Uint64("job_id", jobID).Msg("next phase new job to func")

					return f(jobID, c)
				}

				addNext([2]interface{}{jobID, c})

				_ = isAdded.SetValue(true)

				return nil
			})
		},
	)

	go func() {
		_ = prevDone()
	}()

	if err := newJobsDone(); err != nil {
		return err
	}

	l.Debug().Msg("next phase new job ch closed")
	l.Debug().Msg("next phase new jobs done")

	if err := prevDone(); err != nil {
		return err
	}

	if added, _ := isAdded.Value(); added {
		l.Debug().Msg("next phase exhaust after done")

		if err := exhaust(); err != nil {
			return err
		}
	}

	return nil
}

func NewPhaseJobWorker2(
	ctx context.Context,
	args *PhaseJobWorkerArgs,
	phase int,
	prevWait func() error,
) (
	newJob PhaseJobWorkerNewJobFunc,
	jobDone func(),
	wait func() error,
	_ error,
) {
	return newPhaseJobWorker2(
		ctx,
		args,
		0,
		func() error { return nil },
	)
}
