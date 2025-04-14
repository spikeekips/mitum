package util

import (
	"context"
	"slices"
	"sync/atomic"

	"github.com/pkg/errors"
	"golang.org/x/sync/semaphore"
)

var ErrJobWorkerDone = NewSError("job worker: no more new job")

type (
	JobWorkerNewJobFunc = InErrorFunc[InErrorFunc[context.Context]]
	NewJobWorkerFunc    = func(_ context.Context, semSize int64) (
		JobWorkerNewJobFunc,
		func(),
		InErrorFunc[error],
		error,
	)
)

func NewFuncBaseJobWorker(
	ctx context.Context,
	semSize int64,
	errf InErrorFunc[error],
) (
	JobWorkerNewJobFunc,
	func(),
	InErrorFunc[error],
	error,
) {
	if semSize < 1 {
		return nil, nil, nil, errors.New("semSize under 1")
	}

	wctx, wctxCancel := ContextWithCancelCause(ctx)
	newJobCtx, newJobDone := ContextWithCancelCause(wctx)

	sem := semaphore.NewWeighted(semSize)

	newJobf := newFuncJobWorkerNewJobFunc(wctx, newJobCtx, sem, func(err error) error {
		if nerr := errf(err); nerr != nil {
			return wctxCancel(err)
		}

		return nil
	})

	var done int64

	wait := NewOnceFunc(func(err error) error {
		if err := context.Cause(wctx); err != nil {
			return err
		}

		if err != nil {
			return wctxCancel(err)
		}

		waitch := make(chan error, 1)

		go func() {
			<-newJobCtx.Done()

			waitch <- sem.Acquire(ctx, semSize)
		}()

		select {
		case err := <-waitch:
			defer func() {
				_ = wctxCancel(nil)
			}()

			if err != nil {
				return err
			}

			return context.Cause(wctx)
		case <-wctx.Done():
			return context.Cause(wctx)
		}
	})

	return func(c InErrorFunc[context.Context]) error {
			switch err := newJobf(c); {
			case err != nil && atomic.LoadInt64(&done) == 1:
				return ErrJobWorkerDone
			case err != nil:
				return wctxCancel(err)
			default:
				return nil
			}
		},
		func() {
			if atomic.CompareAndSwapInt64(&done, 0, 1) {
				_ = newJobDone(nil)
			}
		},
		func(err error) error {
			switch called, werr := wait(err); {
			case werr != nil:
				return werr
			case !called:
				return context.Canceled
			default:
				return nil
			}
		},
		nil
}

func NewFuncJobWorker(ctx context.Context, semSize int64) (
	JobWorkerNewJobFunc,
	func(),
	InErrorFunc[error],
	error,
) {
	return NewFuncBaseJobWorker(
		ctx,
		semSize,
		func(err error) error { return err },
	)
}

func NewFuncErrJobWorker(ctx context.Context, semSize int64, errf InErrorFunc[error]) (
	JobWorkerNewJobFunc,
	func(),
	InErrorFunc[error],
	error,
) {
	return NewFuncBaseJobWorker(
		ctx,
		semSize,
		errf,
	)
}

func newFuncJobWorkerNewJobFunc(
	ctx, newJobCtx context.Context,
	sem *semaphore.Weighted,
	errf InErrorFunc[error],
) JobWorkerNewJobFunc {
	return func(c InErrorFunc[context.Context]) error {
		if err := context.Cause(newJobCtx); err != nil {
			return err
		}

		if sem.Acquire(newJobCtx, 1) != nil {
			return context.Cause(newJobCtx)
		}

		go func() {
			defer sem.Release(1)

			if err := c(ctx); err != nil {
				_ = errf(err)
			}
		}()

		return nil
	}
}

func NewFuncNewJobWorkerFunc(maxSemSize int64) NewJobWorkerFunc {
	return func(ctx context.Context, semSize int64) (
		JobWorkerNewJobFunc,
		func(),
		InErrorFunc[error],
		error,
	) {
		return NewFuncJobWorker(ctx, min(semSize, maxSemSize))
	}
}

func NewFuncNewErrJobWorkerFunc(maxSemSize int64, errf InErrorFunc[error]) NewJobWorkerFunc {
	return func(ctx context.Context, semSize int64) (
		JobWorkerNewJobFunc,
		func(),
		InErrorFunc[error],
		error,
	) {
		return NewFuncErrJobWorker(ctx, min(semSize, maxSemSize), errf)
	}
}

func FuncsJobWorker(ctx context.Context, fs ...InErrorFunc[context.Context]) error {
	var newJob JobWorkerNewJobFunc
	var jobDone func()
	var jobWait InErrorFunc[error]

	switch n, d, w, err := NewFuncJobWorker(ctx, int64(len(fs))); {
	case err != nil:
		return err
	default:
		newJob, jobDone, jobWait = n, d, w
	}

	go func() {
		defer jobDone()

		for f := range slices.Values(fs) {
			_ = newJob(f)
		}
	}()

	return jobWait(nil)
}
