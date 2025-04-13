package util

import (
	"context"
	"slices"
	"sync"

	"github.com/pkg/errors"
	"github.com/spikeekips/mitum/util"
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
	jctx, jctxCancel := ContextWithCancelCause(wctx)

	sem := semaphore.NewWeighted(semSize)

	newJobf := NewFuncJobWorkerNewJobFunc(wctx, jctx, sem, errf)
	waitf, errNewJobDone := NewFuncJobWorkerWaitFunc(wctx, jctx, sem, semSize)

	iferr := NewIfErrErrorInFunc(errf)

	return func(c InErrorFunc[context.Context]) error {
			err := newJobf(c)
			switch {
			case errors.Is(err, errNewJobDone):
				return ErrJobWorkerDone
			default:
				return iferr(err)
			}
		},
		sync.OnceFunc(func() {
			_ = jctxCancel(errNewJobDone)
		}),
		func(err error) error {
			if err := context.Cause(wctx); err != nil {
				return err
			}

			if err != nil {
				return wctxCancel(err)
			}

			defer func() {
				_ = wctxCancel(nil)
			}()

			waitch := make(chan error, 1)

			go func() {
				waitch <- waitf()
			}()

			select {
			case err := <-waitch:
				return err
			case <-wctx.Done():
				return context.Cause(wctx)
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
	wctx, wctxCancel := ContextWithCancelCause(ctx)

	newJob, jobDone, wait, err := NewFuncBaseJobWorker(
		wctx,
		semSize,
		wctxCancel,
	)
	if err != nil {
		return nil, nil, nil, err
	}

	return newJob, jobDone,
		func(err error) error {
			return wctxCancel(wait(err))
		},
		nil
}

func NewFuncErrJobWorker(ctx context.Context, semSize int64, errf InErrorFunc[error]) (
	JobWorkerNewJobFunc,
	func(),
	InErrorFunc[error],
	error,
) {
	wctx, wctxCancel := ContextWithCancelCause(ctx)

	ierrf := NewIfErrErrorInFunc(wctxCancel)

	newJob, jobDone, wait, err := NewFuncBaseJobWorker(
		wctx,
		semSize,
		func(err error) error {
			return ierrf(errf(err))
		},
	)
	if err != nil {
		return nil, nil, nil, err
	}

	return newJob, jobDone,
		func(err error) error {
			return wctxCancel(wait(err))
		},
		nil
}

func NewFuncJobWorkerNewJobFunc(
	ctx, newJobCtx context.Context,
	sem *semaphore.Weighted,
	errf InErrorFunc[error],
) JobWorkerNewJobFunc {
	iferr := NewIfErrErrorInFunc(errf)

	return func(c InErrorFunc[context.Context]) error {
		if err := context.Cause(newJobCtx); err != nil {
			return err
		}

		if err := sem.Acquire(newJobCtx, 1); err != nil {
			return context.Cause(newJobCtx)
		}

		go func() {
			defer sem.Release(1)

			_ = iferr(c(ctx))
		}()

		return nil
	}
}

func NewFuncJobWorkerWaitFunc(
	ctx, newJobCtx context.Context,
	sem *semaphore.Weighted,
	semSize int64,
) (
	ErrorFunc,
	error, /* doneErr */
) {
	errNewJobDone := NewSError(util.UUID().String()) // FIXME remove

	return func() error {
			<-newJobCtx.Done()

			return ContextCauseError(ctx, nil, func(ctx context.Context) error {
				return sem.Acquire(ctx, semSize)
			})
		},
		errNewJobDone
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
