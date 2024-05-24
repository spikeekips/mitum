package main

import (
	"context"
	"fmt"
	"sync"

	"github.com/pkg/errors"
	"github.com/spikeekips/mitum/util"
	"golang.org/x/sync/semaphore"
)

type (
	PhaseJobWorkerCallback0 func(_ context.Context, phase int) (uint64, PhaseJobWorkerCallback0, error)
	NewPhaseJobWorkerFunc0  func(context.Context) (
		newJob func(jobID uint64, _ PhaseJobWorkerCallback0) error,
		jobDone func(),
		wait func() error,
		_ error,
	)
)

func NewPhaseJobWorker0(
	ctx context.Context,
	newJobWorker func(context.Context) (JobWorker, error),
	mapSize uint64,
) (
	newJob func(jobID uint64, _ PhaseJobWorkerCallback0) error,
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

	var newJobFunc func(uint64, PhaseJobWorkerCallback0) error
	var doneFunc func()
	var waitFunc func() error

	switch n, d, w, err := newPhaseJobWorker0(
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

	return func(jobID uint64, c PhaseJobWorkerCallback0) error {
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

func newPhaseJobWorker0(
	ctx context.Context,
	newJobWorker func(context.Context) (JobWorker, error),
	phase int,
	jobIDLocked func(jobID uint64, phase int, f func() error) error,
) (
	newJob func(jobID uint64, _ PhaseJobWorkerCallback0) error,
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
	var nextNewJobFunc func(uint64, PhaseJobWorkerCallback0) error
	var nextDoneFunc func()
	var nextWaitFunc func() error

	nextPhaseNewJob := func(jobID uint64, c PhaseJobWorkerCallback0) error {
		var gerr error

		nextOnce.Do(func() {
			nextLock.Lock()
			defer nextLock.Unlock()

			switch n, d, w, err := newPhaseJobWorker0(ctx, newJobWorker, phase+1, jobIDLocked); {
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

	return func(jobID uint64, c PhaseJobWorkerCallback0) error {
			return wk.NewJob(func(ctx context.Context) error {
				return jobIDLocked(jobID, phase, func() error {
					switch nextJobID, i, err := c(ctx, phase); {
					case err != nil:
						return err
					case i == nil:
						return nil
					default:
						return nextPhaseNewJob(nextJobID, i)
					}
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
				defer nextLock.RUnlock()

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

type ContextWorkerCallback func(context.Context) error

type JobWorker interface {
	NewJob(ContextWorkerCallback) error
	JobDone()
	Wait() error
	Close()
}

type BaseJobWorker struct {
	ctx        func() context.Context
	ctxCancel  func(error)
	newJobCtx  func() context.Context
	NewJobFunc func(ContextWorkerCallback) error
	newJob     func(ContextWorkerCallback) error
	done       func()
	wait       func() error
}

func NewBaseJobWorker(ctx context.Context, semSize int64) (*BaseJobWorker, error) {
	if semSize < 1 {
		return nil, errors.Errorf("semSize under 1")
	}

	errNewJobDone := errors.New(fmt.Sprintf("no more new job; %s", util.UUID().String()))

	ectx, eCtxCancel := context.WithCancelCause(ctx)
	newJobCtx, newJobCtxCancel := context.WithCancelCause(ectx)

	wk := &BaseJobWorker{
		ctx:       func() context.Context { return ectx },
		ctxCancel: eCtxCancel,
		newJobCtx: func() context.Context { return newJobCtx },
		done: func() {
			newJobCtxCancel(errNewJobDone)
		},
		NewJobFunc: func(c ContextWorkerCallback) error {
			return c(ectx)
		},
	}

	sem := semaphore.NewWeighted(semSize)

	newJobf := func(c ContextWorkerCallback) error {
		if err := context.Cause(wk.newJobCtx()); err != nil {
			return errors.WithStack(err)
		}

		if err := sem.Acquire(wk.newJobCtx(), 1); err != nil {
			return errors.WithStack(err)
		}

		go func() {
			if err := wk.NewJobFunc(c); err != nil {
				wk.ctxCancel(err)
			}

			sem.Release(1)
		}()

		return nil
	}

	waitf := func() error {
		<-wk.newJobCtx().Done()

		if err := sem.Acquire(wk.ctx(), semSize); err != nil {
			if cerr := context.Cause(wk.ctx()); cerr != nil {
				return errors.WithStack(cerr)
			}

			return errors.WithStack(err)
		}

		if err := context.Cause(wk.ctx()); err != nil {
			return errors.WithStack(err)
		}

		return nil
	}

	wk.newJob = func(c ContextWorkerCallback) error {
		switch err := newJobf(c); {
		case errors.Is(err, errNewJobDone):
			return util.ErrJobWorkerDone.Wrap(err)
		default:
			return err
		}
	}

	wk.wait = func() error {
		switch err := waitf(); {
		case errors.Is(err, errNewJobDone):
			return util.ErrJobWorkerDone.Wrap(err)
		default:
			return err
		}
	}

	return wk, nil
}

func (wk *BaseJobWorker) Close() {
	wk.ctxCancel(nil)
}

func (wk *BaseJobWorker) JobDone() {
	wk.done()
}

func (wk *BaseJobWorker) NewJob(c ContextWorkerCallback) error {
	return wk.newJob(c)
}

// Wait waits until all job finished.
func (wk *BaseJobWorker) Wait() error {
	defer wk.ctxCancel(nil)

	return wk.wait()
}

// LazyWait don't wait until all job finished.
func (wk *BaseJobWorker) LazyWait() error {
	defer wk.ctxCancel(nil)

	waitch := make(chan error, 1)

	go func() {
		waitch <- wk.wait()
	}()

	select {
	case <-wk.ctx().Done():
		return errors.WithStack(context.Cause(wk.ctx()))
	case err := <-waitch:
		return err
	}
}

// NewErrCallbackJobWorker ignores job error.
func NewErrCallbackJobWorker(ctx context.Context, semSize int64, errf func(error)) (wk *BaseJobWorker, _ error) {
	if errf == nil {
		errf = func(error) {} //revive:disable-line:modifies-parameter
	}

	switch i, err := NewBaseJobWorker(ctx, semSize); {
	case err != nil:
		return nil, err
	default:
		wk = i
	}

	wk.NewJobFunc = func(c ContextWorkerCallback) error {
		if err := c(wk.ctx()); err != nil {
			errf(err)
		}

		return nil
	}

	return wk, nil
}
