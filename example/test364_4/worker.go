package main

import (
	"context"
	"fmt"

	"github.com/pkg/errors"
	"github.com/spikeekips/mitum/util"
	"golang.org/x/sync/semaphore"
)

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
