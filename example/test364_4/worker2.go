package main

import (
	"context"
	"sync"

	"github.com/spikeekips/mitum/util"
)

type (
	PhaseJobWorkerNewJobFunc func(jobID uint64, _ PhaseJobWorkerCallback2) error
	PhaseJobWorkerCallback2  func(_ context.Context, phase int, _ PhaseJobWorkerNewJobFunc) error
	NewPhaseJobWorkerFunc2   func(context.Context) (
		newJob PhaseJobWorkerNewJobFunc,
		jobDone func(),
		wait func() error,
		_ error,
	)
)

func newPhaseJobWorker2(
	ctx context.Context,
	newJobWorker func(context.Context) (JobWorker, error),
	phase int,
	jobIDLocked func(jobID uint64, phase int, f func() error) error,
) (
	newJob func(jobID uint64, _ PhaseJobWorkerCallback2) error,
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

	var nextLock sync.RWMutex
	var nextOnce sync.Once
	var nextNewJobFunc PhaseJobWorkerNewJobFunc
	var nextDoneFunc func()
	var nextWaitFunc func() error

	nextPhaseNewJob := func(jobID uint64, c PhaseJobWorkerCallback2) error {
		var gerr error

		nextOnce.Do(func() {
			nextLock.Lock()
			defer nextLock.Unlock()

			switch n, d, w, err := newPhaseJobWorker2(ctx, newJobWorker, phase+1, jobIDLocked); {
			case err != nil:
				gerr = err
			default:
				nextNewJobFunc = n
				nextDoneFunc = d
				nextWaitFunc = w
			}
		})

		if gerr != nil {
			return gerr
		}

		return nextNewJobFunc(jobID, c)
	}

	return func(jobID uint64, c PhaseJobWorkerCallback2) error {
			return wk.NewJob(func(ctx context.Context) error {
				return jobIDLocked(jobID, phase, func() error {
					return c(ctx, phase, nextPhaseNewJob)
				})
			})
		},
		wk.JobDone,
		func() error {
			if err := wk.Wait(); err != nil {
				return err
			}

			if err := func() error {
				nextLock.RLock()
				defer nextLock.RLock()

				if nextDoneFunc == nil {
					return nil
				}

				nextDoneFunc()

				return nextWaitFunc()
			}(); err != nil {
				return err
			}

			return nil
		},
		nil
}

func NewPhaseJobWorker2(
	ctx context.Context,
	newJobWorker func(context.Context) (JobWorker, error),
	mapSize uint64,
) (
	newJob func(jobID uint64, _ PhaseJobWorkerCallback2) error,
	jobDone func(),
	wait func() error,
	_ error,
) {
	if newJobWorker == nil {
		newJobWorker = func(ctx context.Context) (JobWorker, error) {
			return NewBaseJobWorker(ctx, 1<<9)
		}
	}

	var jobIDLocked *util.ShardedMap[uint64, int]

	switch i, err := util.NewShardedMap[uint64, int](mapSize, nil); {
	case err != nil:
		return nil, nil, nil, err
	default:
		jobIDLocked = i
	}

	pctx, pctxCancel := context.WithCancelCause(ctx)

	var newJobFunc func(uint64, PhaseJobWorkerCallback2) error
	var doneFunc func()
	var waitFunc func() error

	switch n, d, w, err := newPhaseJobWorker2(
		pctx,
		newJobWorker,
		0,
		func(jobID uint64, phase int, f func() error) error {
			var ok bool

			if _, _, err := jobIDLocked.Set(jobID, func(prev int, found bool) (int, error) {
				if found && phase <= prev {
					return 0, util.ErrLockedSetIgnore
				}

				ok = true

				return phase, nil
			}); err != nil {
				return err
			}

			if !ok {
				return nil
			}

			return f()
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

	return func(jobID uint64, c PhaseJobWorkerCallback2) error {
			if err := newJobFunc(jobID, c); err != nil {
				pctxCancel(err)

				return err
			}

			return nil
		},
		doneFunc,
		func() error {
			defer pctxCancel(nil)

			if err := waitFunc(); err != nil {
				pctxCancel(err)

				return err
			}

			return nil
		}, nil
}
