package util

import (
	"context"
	"sync"
	"time"

	"github.com/pkg/errors"
	"golang.org/x/sync/errgroup"
	"golang.org/x/sync/semaphore"
)

var baseSemWorkerPool = sync.Pool{
	New: func() interface{} {
		return new(BaseSemWorker)
	},
}

var baseSemWorkerPoolPut = func(wk *BaseSemWorker) {
	wk.N = 0
	wk.Sem = nil
	wk.Ctx = nil
	// wk.Cancel = nil
	wk.JobCount = 0
	wk.NewJobFunc = nil
	wk.donech = nil

	baseSemWorkerPool.Put(wk)
}

var distributeWorkerPool = sync.Pool{
	New: func() interface{} {
		return new(DistributeWorker)
	},
}

var distributeWorkerPoolPut = func(wk *DistributeWorker) {
	wk.BaseSemWorker = nil
	wk.errch = nil

	distributeWorkerPool.Put(wk)
}

var errgroupWorkerPool = sync.Pool{
	New: func() interface{} {
		return new(ErrgroupWorker)
	},
}

var errgroupWorkerPoolPut = func(wk *ErrgroupWorker) {
	wk.BaseSemWorker = nil
	wk.eg = nil

	errgroupWorkerPool.Put(wk)
}

type (
	WorkerCallback        func(jobid uint, arg interface{}) error
	ContextWorkerCallback func(ctx context.Context, jobid uint64) error
)

var ErrWorkerContextCanceled = NewError("context canceled in worker")

type ParallelWorker struct {
	jobChan     chan interface{}
	errChan     chan error
	callbacks   []WorkerCallback
	jobFinished int
	bufsize     uint
	jobCalled   uint
	lastCalled  int
	sync.RWMutex
}

func NewParallelWorker(bufsize uint) *ParallelWorker {
	wk := &ParallelWorker{
		bufsize:    bufsize,
		jobChan:    make(chan interface{}, int(bufsize)),
		errChan:    make(chan error),
		lastCalled: -1,
	}

	go wk.roundrobin()

	return wk
}

func (wk *ParallelWorker) roundrobin() {
	var jobID uint

	for job := range wk.jobChan {
		callback := wk.nextCallback()

		go func(jobID uint, job interface{}) {
			err := callback(jobID, job)

			wk.Lock()
			wk.jobFinished++
			wk.Unlock()

			wk.errChan <- err
		}(jobID, job)
		jobID++
	}
}

func (wk *ParallelWorker) Run(callback WorkerCallback) *ParallelWorker {
	wk.Lock()
	defer wk.Unlock()

	wk.callbacks = append(wk.callbacks, callback)

	return wk
}

func (wk *ParallelWorker) nextCallback() WorkerCallback {
	wk.Lock()
	defer wk.Unlock()

	index := wk.lastCalled + 1

	if index >= len(wk.callbacks) {
		index = 0
	}

	wk.lastCalled = index

	return wk.callbacks[index]
}

func (wk *ParallelWorker) NewJob(j interface{}) {
	wk.Lock()
	wk.jobCalled++
	wk.Unlock()

	wk.jobChan <- j
}

func (wk *ParallelWorker) Errors() <-chan error {
	return wk.errChan
}

func (wk *ParallelWorker) Jobs() uint {
	wk.RLock()
	defer wk.RUnlock()

	return wk.jobCalled
}

func (wk *ParallelWorker) FinishedJobs() int {
	wk.RLock()
	defer wk.RUnlock()

	return wk.jobFinished
}

func (wk *ParallelWorker) Done() {
	if wk.jobChan != nil {
		close(wk.jobChan)
	}
	// NOTE don't close errChan :)
}

func (wk *ParallelWorker) IsFinished() bool {
	wk.RLock()
	defer wk.RUnlock()

	return uint(wk.jobFinished) == wk.jobCalled
}

type BaseSemWorker struct {
	Ctx        context.Context //nolint:containedctx //...
	Sem        *semaphore.Weighted
	Cancel     func()
	NewJobFunc func(context.Context, uint64, ContextWorkerCallback)
	donech     chan time.Duration
	N          int64
	JobCount   uint64
	runonce    sync.Once
	closeonece sync.Once
}

func NewBaseSemWorker(ctx context.Context, semsize int64) *BaseSemWorker {
	wk := baseSemWorkerPool.Get().(*BaseSemWorker) //nolint:forcetypeassert //...
	closectx, cancel := context.WithCancel(ctx)

	wk.N = semsize
	wk.Sem = semaphore.NewWeighted(semsize)
	wk.Ctx = closectx
	wk.Cancel = cancel
	wk.JobCount = 0
	wk.runonce = sync.Once{}
	wk.donech = make(chan time.Duration, 2)

	return wk
}

func (wk *BaseSemWorker) NewJob(callback ContextWorkerCallback) error {
	if err := wk.Ctx.Err(); err != nil {
		return err
	}

	sem := wk.Sem
	newjob := wk.NewJobFunc
	jobs := wk.JobCount

	if err := wk.Sem.Acquire(wk.Ctx, 1); err != nil {
		wk.Cancel()

		return ErrWorkerContextCanceled.Wrap(err)
	}

	ctx, cancel := context.WithCancel(wk.Ctx)

	go func() {
		defer sem.Release(1)
		defer cancel()

		newjob(ctx, jobs, callback)
	}()
	wk.JobCount++

	return nil
}

func (wk *BaseSemWorker) Jobs() uint64 {
	return wk.JobCount
}

func (wk *BaseSemWorker) Wait() error {
	return wk.wait()
}

func (wk *BaseSemWorker) wait() error {
	n := wk.N
	sem := wk.Sem
	ctx := wk.Ctx
	cancel := wk.Cancel

	var werr error

	wk.runonce.Do(func() {
		timeout := <-wk.donech

		donech := make(chan error, 1)
		go func() {
			switch err := sem.Acquire(context.Background(), n); { //nolint:contextcheck //...
			case err != nil:
				donech <- err
			default:
				donech <- ctx.Err()
			}
		}()

		if timeout < 1 {
			werr = <-donech

			return
		}

		select {
		case <-time.After(timeout):
			cancel()

			werr = ErrWorkerContextCanceled.Call()
		case werr = <-donech:
		}
	})

	return werr
}

func (wk *BaseSemWorker) WaitChan() chan error {
	ch := make(chan error)

	go func() {
		ch <- wk.Wait()
	}()

	return ch
}

func (wk *BaseSemWorker) Done() {
	wk.donech <- 0
}

func (wk *BaseSemWorker) Close() {
	wk.closeonece.Do(func() {
		defer baseSemWorkerPoolPut(wk)

		wk.donech <- 0

		wk.Cancel()
	})
}

func (wk *BaseSemWorker) LazyCancel(timeout time.Duration) {
	wk.donech <- timeout
}

type DistributeWorker struct {
	*BaseSemWorker
	errch chan error
}

func NewDistributeWorker(ctx context.Context, semsize int64, errch chan error) *DistributeWorker {
	wk := distributeWorkerPool.Get().(*DistributeWorker) //nolint:forcetypeassert //...

	base := NewBaseSemWorker(ctx, semsize)

	var errf func(error)
	if errch == nil {
		errf = func(error) {}
	} else {
		errf = func(err error) {
			if cerr := base.Ctx.Err(); cerr == nil {
				errch <- err
			}
		}
	}

	base.NewJobFunc = func(ctx context.Context, jobs uint64, callback ContextWorkerCallback) {
		errf(callback(ctx, jobs))
	}

	wk.BaseSemWorker = base
	wk.errch = errch

	return wk
}

func (wk *DistributeWorker) Close() {
	defer distributeWorkerPoolPut(wk)

	wk.BaseSemWorker.Close()
}

type ErrgroupWorker struct {
	*BaseSemWorker
	eg         *errgroup.Group
	doneonce   sync.Once
	closeonece sync.Once
}

func NewErrgroupWorker(ctx context.Context, semsize int64) *ErrgroupWorker {
	wk := errgroupWorkerPool.Get().(*ErrgroupWorker) //nolint:forcetypeassert //...

	base := NewBaseSemWorker(ctx, semsize)

	eg, egctx := errgroup.WithContext(base.Ctx)
	base.Ctx = egctx

	var cancelonece sync.Once

	base.NewJobFunc = func(ctx context.Context, jobs uint64, callback ContextWorkerCallback) {
		donech := make(chan struct{}, 1)

		eg.Go(func() error {
			defer func() {
				donech <- struct{}{}
			}()

			errch := make(chan error, 1)
			go func() {
				errch <- callback(ctx, jobs)
			}()

			var err error
			select {
			case <-ctx.Done():
			case err = <-errch:
				if err != nil {
					defer cancelonece.Do(func() {
						wk.Cancel()
					})
				}
			}

			return err
		})

		<-donech
	}

	wk.BaseSemWorker = base
	wk.eg = eg
	wk.doneonce = sync.Once{}

	return wk
}

func (wk *ErrgroupWorker) Wait() error {
	var berr error

	if err := wk.BaseSemWorker.wait(); err != nil {
		switch {
		case errors.Is(err, context.Canceled):
			berr = err
		case errors.Is(err, ErrWorkerContextCanceled):
			return context.Canceled
		default:
			return err
		}
	}

	var werr error

	wk.doneonce.Do(func() {
		werr = wk.eg.Wait()
	})

	switch {
	case werr != nil:
		return errors.Wrap(werr, "")
	default:
		return berr
	}
}

func (wk *ErrgroupWorker) Close() {
	wk.closeonece.Do(func() {
		defer errgroupWorkerPoolPut(wk)

		wk.BaseSemWorker.Close()
	})
}

func (wk *ErrgroupWorker) RunChan() chan error {
	ch := make(chan error)

	go func() {
		ch <- wk.Wait()
	}()

	return ch
}

// BatchWork runs f by limit size in worker. For example,
// size=5 limit=2 are given,
// 1. Run worker(0,1)
// 2. Run worker(2,3)
// 3. Run worker(4), done.
func BatchWork(
	ctx context.Context,
	size, limit uint64,
	pref func(_ context.Context, last uint64) error,
	f func(_ context.Context, i, last uint64) error,
) error {
	if size < 1 {
		return errors.Errorf("do nothing; wrong size")
	}

	if size <= limit {
		if err := pref(ctx, size-1); err != nil {
			return errors.WithMessage(err, "")
		}

		return RunErrgroupWorker(ctx, size, func(ctx context.Context, i, _ uint64) error {
			return f(ctx, i, size-1)
		})
	}

	var i uint64

	for {
		end := i + limit
		if end > size {
			end = size
		}

		if err := pref(ctx, end-1); err != nil {
			return err
		}

		if err := RunErrgroupWorker(ctx, end-i, func(ctx context.Context, n, _ uint64) error {
			return f(ctx, i+n, end-1)
		}); err != nil {
			return err
		}

		if end == size {
			break
		}

		i += limit
	}

	return nil
}

func RunDistributeWorker(
	ctx context.Context, size uint64, errch chan error, f func(ctx context.Context, i, jobid uint64) error,
) error {
	worker := NewDistributeWorker(ctx, int64(size), errch)
	defer worker.Close()

	for i := uint64(0); i < size; i++ {
		i := i

		if err := worker.NewJob(func(ctx context.Context, jobid uint64) error {
			return f(ctx, i, jobid)
		}); err != nil {
			return err
		}
	}

	worker.Done()

	return worker.Wait()
}

func RunErrgroupWorker(ctx context.Context, size uint64, f func(ctx context.Context, i, jobid uint64) error) error {
	worker := NewErrgroupWorker(ctx, int64(size))
	defer worker.Close()

	for i := uint64(0); i < size; i++ {
		i := i

		if err := worker.NewJob(func(ctx context.Context, jobid uint64) error {
			return f(ctx, i, jobid)
		}); err != nil {
			return err
		}
	}

	worker.Done()

	return worker.Wait()
}

func RunErrgroupWorkerByJobs(ctx context.Context, jobs ...ContextWorkerCallback) error {
	worker := NewErrgroupWorker(ctx, int64(len(jobs)))
	defer worker.Close()

	for i := range jobs {
		if err := worker.NewJob(jobs[i]); err != nil {
			return err
		}
	}

	worker.Done()

	return worker.Wait()
}
