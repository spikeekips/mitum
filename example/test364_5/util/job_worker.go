package util

import (
	"context"

	"github.com/pkg/errors"
	"github.com/spikeekips/mitum/util"
	"golang.org/x/sync/semaphore"
)

var ErrJobWorkerDone = util.NewIDError("job worker: no more new job")

type (
	JobWorkerNewJobFunc = InErrorFunc[InErrorFunc[context.Context]]
	NewJobWorkerFunc    = func(_ context.Context, semSize uint64) (
		JobWorkerNewJobFunc,
		func(),
		InErrorFunc[error],
		error,
	)
)

func NewFuncBaseJobWorker(
	ctx context.Context,
	semSize uint64,
	errf InErrorFunc[error],
) (
	JobWorkerNewJobFunc,
	func(),
	InErrorFunc[error],
	error,
) {
	if semSize < 1 {
		return nil, nil, nil, errors.Errorf("semSize under 1")
	}

	wctx, wctxCancel := ContextWithCancelCause(ctx)
	jctx, jctxCancel := ContextWithCancelCause(wctx)

	sem := semaphore.NewWeighted(int64(semSize))

	newJobf := NewFuncJobWorkerNewJobFunc(wctx, jctx, sem, errf)
	waitf, errNewJobDone := NewFuncJobWorkerWaitFunc(wctx, jctx, sem, semSize)

	return func(c InErrorFunc[context.Context]) error {
			switch err := newJobf(c); {
			case errors.Is(err, errNewJobDone):
				return ErrJobWorkerDone.WithStack()
			default:
				return err
			}
		},
		func() {
			_ = jctxCancel(errNewJobDone)
		},
		func(err error) error {
			defer func() {
				_ = wctxCancel(nil)
			}()

			if err != nil {
				if err := context.Cause(wctx); err != nil {
					return ErrorWithStack(err)
				}

				return wctxCancel(err)
			}

			if err := context.Cause(wctx); err != nil {
				return ErrorWithStack(err)
			}

			waitch := make(chan error, 1)

			go func() {
				waitch <- waitf()
			}()

			select {
			case err := <-waitch:
				return err
			case <-wctx.Done():
				return ErrorWithStack(context.Cause(wctx))
			}
		},
		nil
}

func NewFuncJobWorker(ctx context.Context, semSize uint64) (
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

func NewFuncErrJobWorker(ctx context.Context, semSize uint64, errf InErrorFunc[error]) (
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
	return func(c InErrorFunc[context.Context]) error {
		iferr := NewIfErrErrorInFunc(errf)

		return ContextCauseError(newJobCtx, iferr, func(context.Context) error {
			if err := sem.Acquire(newJobCtx, 1); err != nil {
				return ErrorWithStack(err)
			}

			go func() {
				defer sem.Release(1)

				_ = iferr(c(ctx))
			}()

			return nil
		})
	}
}

func NewFuncJobWorkerWaitFunc(
	ctx, newJobCtx context.Context,
	sem *semaphore.Weighted,
	semSize uint64,
) (
	_ ErrorFunc,
	doneErr error,
) {
	errNewJobDone := errors.New(util.UUID().String())

	return func() error {
			<-newJobCtx.Done()

			return ContextCauseError(ctx, nil, func(ctx context.Context) error {
				return sem.Acquire(ctx, int64(semSize))
			})
		},
		errNewJobDone
}

func NewFuncNewJobWorkerFunc(maxSemSize uint64) NewJobWorkerFunc {
	return func(ctx context.Context, semSize uint64) (
		JobWorkerNewJobFunc,
		func(),
		InErrorFunc[error],
		error,
	) {
		if semSize > maxSemSize {
			semSize = maxSemSize
		}

		return NewFuncJobWorker(ctx, semSize)
	}
}

func NewFuncNewErrJobWorkerFunc(maxSemSize uint64, errf InErrorFunc[error]) NewJobWorkerFunc {
	return func(ctx context.Context, semSize uint64) (
		JobWorkerNewJobFunc,
		func(),
		InErrorFunc[error],
		error,
	) {
		if semSize > maxSemSize {
			semSize = maxSemSize
		}

		return NewFuncErrJobWorker(ctx, semSize, errf)
	}
}

func FuncsJobWorker(ctx context.Context, fs ...InErrorFunc[context.Context]) error {
	var newJob JobWorkerNewJobFunc
	var jobDone func()
	var jobWait InErrorFunc[error]

	switch n, d, w, err := NewFuncJobWorker(ctx, uint64(len(fs))); {
	case err != nil:
		return err
	default:
		newJob, jobDone, jobWait = n, d, w
	}

	go func() {
		defer jobDone()

		_ = TraverseSlice(fs, func(_ int, f InErrorFunc[context.Context]) (bool, error) {
			return true, newJob(f)
		})
	}()

	return jobWait(nil)
}
