package util

import (
	"context"
	"sync"

	"github.com/pkg/errors"
	"golang.org/x/sync/semaphore"
)

type ContextWorkerCallback func(ctx context.Context, jobid uint64) error

type JobWorker interface {
	NewJob(ContextWorkerCallback) error
	Done()
	Wait() error
	Close()
}

var ErrJobWorkerDone = NewIDError("job worker: no more new job")

type BaseJobWorker struct {
	ctx        func() context.Context
	ctxCancel  func(error)
	newJobCtx  func() context.Context
	NewJobFunc func(jobCount uint64, _ ContextWorkerCallback) error
	newJob     func(ContextWorkerCallback) error
	done       func()
	waitFunc   func() error
}

func NewBaseJobWorker(ctx context.Context, semSize int64) (*BaseJobWorker, error) {
	if semSize < 1 {
		return nil, errors.Errorf("semSize under 1")
	}

	ectx, eCtxCancel := context.WithCancelCause(ctx)
	newJobCtx, newJobCtxCancel := context.WithCancelCause(ectx)

	wk := &BaseJobWorker{
		ctx:       func() context.Context { return ectx },
		ctxCancel: eCtxCancel,
		newJobCtx: func() context.Context { return newJobCtx },
		done:      func() { newJobCtxCancel(ErrJobWorkerDone) },
		NewJobFunc: func(jobCount uint64, c ContextWorkerCallback) error {
			return c(ectx, jobCount)
		},
	}

	var jobCount uint64
	var countJobLock sync.Mutex

	countJob := func() uint64 {
		countJobLock.Lock()
		defer countJobLock.Unlock()

		i := jobCount
		jobCount++

		return i
	}

	sem := semaphore.NewWeighted(semSize)

	wk.newJob = func(c ContextWorkerCallback) error {
		if err := context.Cause(wk.newJobCtx()); err != nil {
			return errors.WithStack(err)
		}

		if err := sem.Acquire(wk.newJobCtx(), 1); err != nil {
			return errors.WithStack(err)
		}

		jobCount := countJob()

		go func() {
			defer sem.Release(1)

			if err := wk.NewJobFunc(jobCount, c); err != nil {
				wk.ctxCancel(err)
			}
		}()

		return nil
	}

	wk.waitFunc = func() error {
		if err := sem.Acquire(wk.ctx(), semSize); err != nil {
			if cerr := context.Cause(wk.ctx()); cerr != nil {
				return errors.WithStack(cerr)
			}

			return errors.WithStack(err)
		}

		return nil
	}

	return wk, nil
}

func (wk *BaseJobWorker) Cancel() {
	wk.ctxCancel(nil)
}

func (wk *BaseJobWorker) Close() {
	wk.Cancel()
}

func (wk *BaseJobWorker) Done() {
	wk.done()
}

func (wk *BaseJobWorker) NewJob(c ContextWorkerCallback) error {
	return wk.newJob(c)
}

// Wait waits until all job finished.
func (wk *BaseJobWorker) Wait() error {
	defer wk.Cancel()

	<-wk.newJobCtx().Done()

	if err := wk.waitFunc(); err != nil {
		return err
	}

	return errors.WithStack(context.Cause(wk.ctx()))
}

// LazyWait don't wait until all job finished.
func (wk *BaseJobWorker) LazyWait() error {
	defer wk.Cancel()

	<-wk.newJobCtx().Done()

	waitch := make(chan error, 1)

	go func() {
		waitch <- wk.waitFunc()
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

	wk.NewJobFunc = func(jobCount uint64, c ContextWorkerCallback) error {
		if err := c(wk.ctx(), jobCount); err != nil {
			errf(err)
		}

		return nil
	}

	return wk, nil
}

// BatchWork runs f by limit size in worker. For example,
// size=5 limit=2 are given,
// 1. Run worker(0,1)
// 2. Run worker(2,3)
// 3. Run worker(4), done.
func BatchWork(
	ctx context.Context,
	size, limit int64,
	pref func(_ context.Context, last uint64) error,
	f func(_ context.Context, i, last uint64) error,
) error {
	if size < 1 {
		return errors.Errorf("do nothing; wrong size")
	}

	if size <= limit {
		if err := pref(ctx, uint64(size-1)); err != nil {
			return err
		}

		return RunJobWorker(ctx, size, size, func(ctx context.Context, i, _ uint64) error {
			return f(ctx, i, uint64(size-1))
		})
	}

	var i uint64

	for {
		end := i + uint64(limit)
		if end > uint64(size) {
			end = uint64(size)
		}

		if err := pref(ctx, end-1); err != nil {
			return err
		}

		if err := RunJobWorker(ctx, limit, int64(end-i), func(ctx context.Context, n, _ uint64) error {
			return f(ctx, i+n, end-1)
		}); err != nil {
			return err
		}

		if end == uint64(size) {
			break
		}

		i += uint64(limit)
	}

	return nil
}

func RunErrCallbackJobWorker(
	ctx context.Context,
	workersize, size int64,
	errf func(error),
	f func(ctx context.Context, i, jobid uint64) error,
) error {
	return runWorker(ctx, size, f,
		func(ctx context.Context) (JobWorker, error) {
			return NewErrCallbackJobWorker(ctx, workersize, errf)
		},
	)
}

func RunJobWorker(
	ctx context.Context,
	workersize, size int64,
	f func(ctx context.Context, i, jobid uint64) error,
) error {
	return runWorker(ctx, size, f,
		func(ctx context.Context) (JobWorker, error) {
			return NewBaseJobWorker(ctx, workersize)
		},
	)
}

func runWorker(
	ctx context.Context,
	size int64,
	f func(ctx context.Context, i, jobid uint64) error,
	workerf func(context.Context) (JobWorker, error),
) error {
	worker, err := workerf(ctx)
	if err != nil {
		return err
	}

	defer worker.Close()

	for i := int64(0); i < size; i++ {
		i := i

		if err := worker.NewJob(func(ctx context.Context, jobid uint64) error {
			return f(ctx, uint64(i), jobid)
		}); err != nil {
			return err
		}
	}

	worker.Done()

	return worker.Wait()
}

func RunJobWorkerByJobs(ctx context.Context, jobs ...ContextWorkerCallback) error {
	worker, err := NewBaseJobWorker(ctx, int64(len(jobs)))
	if err != nil {
		return err
	}

	defer worker.Close()

	for i := range jobs {
		if err := worker.NewJob(jobs[i]); err != nil {
			return err
		}
	}

	worker.Done()

	return worker.Wait()
}

type FuncChain struct {
	fs []func() (bool, error)
}

func NewFuncChain() *FuncChain {
	return &FuncChain{}
}

func (c *FuncChain) Add(f func() (bool, error)) *FuncChain {
	c.fs = append(c.fs, f)

	return c
}

func (c *FuncChain) Run() error {
	defer func() {
		c.fs = nil
	}()

	for i := range c.fs {
		switch keep, err := c.fs[i](); {
		case err != nil:
			return err
		case !keep:
			return nil
		}
	}

	return nil
}
