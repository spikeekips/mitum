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
	NewJobWorkerFunc    = func(context.Context) (
		JobWorkerNewJobFunc,
		func(),
		ErrorFunc,
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
	ErrorFunc,
	error,
) {
	if semSize < 1 {
		return nil, nil, nil, errors.Errorf("semSize under 1")
	}

	jctx, jctxCancel := ContextWithCancelCause(ctx)

	sem := semaphore.NewWeighted(int64(semSize))

	newJobf := NewFuncJobWorkerNewJobFunc(ctx, jctx, sem, errf)
	waitf, errNewJobDone := NewFuncJobWorkerWaitFunc(ctx, jctx, sem, semSize)

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
		func() error {
			defer func() {
				_ = jctxCancel(nil)
			}()

			if err := context.Cause(ctx); err != nil {
				return ErrorWithStack(err)
			}

			waitch := make(chan error, 1)

			go func() {
				waitch <- waitf()
			}()

			select {
			case err := <-waitch:
				return err
			case <-ctx.Done():
				return ErrorWithStack(context.Cause(ctx))
			}
		},
		nil
}

func NewFuncJobWorker(ctx context.Context, semSize uint64) (
	JobWorkerNewJobFunc,
	func(),
	ErrorFunc,
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
		func() error {
			return wctxCancel(wait())
		},
		nil
}

func NewFuncErrJobWorker(ctx context.Context, semSize uint64, errf InErrorFunc[error]) (
	JobWorkerNewJobFunc,
	func(),
	ErrorFunc,
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
		func() error {
			return wctxCancel(wait())
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

func NewFuncNewJobWorkerFunc(semSize uint64) NewJobWorkerFunc {
	return func(ctx context.Context) (
		JobWorkerNewJobFunc,
		func(),
		ErrorFunc,
		error,
	) {
		return NewFuncJobWorker(ctx, semSize)
	}
}

func NewFuncNewErrJobWorkerFunc(semSize uint64, errf InErrorFunc[error]) NewJobWorkerFunc {
	return func(ctx context.Context) (
		JobWorkerNewJobFunc,
		func(),
		ErrorFunc,
		error,
	) {
		return NewFuncErrJobWorker(ctx, semSize, errf)
	}
}

func FuncsJobWorker(ctx context.Context, fs ...InErrorFunc[context.Context]) error {
	var newJob JobWorkerNewJobFunc
	var jobDone func()
	var jobWait ErrorFunc

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

	return jobWait()
}
