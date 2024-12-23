package main

import (
	"context"

	"github.com/pkg/errors"
	"github.com/spikeekips/mitum/util"
	"golang.org/x/sync/semaphore"
)

var errJobWorkerDone = util.NewIDError("job worker: no more new job")

type (
	jobWorkerNewJobFunc = inErrorFunc[inErrorFunc[context.Context]]
	newJobWorkerFunc    = func(context.Context) (
		jobWorkerNewJobFunc,
		func(),
		errorFunc,
		error,
	)
)

func newFuncBaseJobWorker(
	ctx context.Context,
	semSize uint64,
	errf inErrorFunc[error],
) (
	jobWorkerNewJobFunc,
	func(),
	errorFunc,
	error,
) {
	if semSize < 1 {
		return nil, nil, nil, errors.Errorf("semSize under 1")
	}

	jctx, jctxCancel := contextWithCancelCause(ctx)

	sem := semaphore.NewWeighted(int64(semSize))

	newJobf := newFuncJobWorkerNewJobFunc(ctx, jctx, sem, errf)
	waitf, errNewJobDone := newFuncJobWorkerWaitFunc(ctx, jctx, sem, semSize)

	return func(c inErrorFunc[context.Context]) error {
			switch err := newJobf(c); {
			case errors.Is(err, errNewJobDone):
				return errJobWorkerDone.WithStack()
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
				return errorWithStack(err)
			}

			waitch := make(chan error, 1)

			go func() {
				waitch <- waitf()
			}()

			select {
			case err := <-waitch:
				return err
			case <-ctx.Done():
				return errorWithStack(context.Cause(ctx))
			}
		},
		nil
}

func newFuncJobWorker(ctx context.Context, semSize uint64) (
	jobWorkerNewJobFunc,
	func(),
	errorFunc,
	error,
) {
	wctx, wctxCancel := contextWithCancelCause(ctx)

	newJob, jobDone, wait, err := newFuncBaseJobWorker(
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

func newFuncErrJobWorker(ctx context.Context, semSize uint64, errf inErrorFunc[error]) (
	jobWorkerNewJobFunc,
	func(),
	errorFunc,
	error,
) {
	wctx, wctxCancel := contextWithCancelCause(ctx)

	ierrf := newIfErrErrorInFunc(wctxCancel)

	newJob, jobDone, wait, err := newFuncBaseJobWorker(
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

func newFuncJobWorkerNewJobFunc(
	ctx, newJobCtx context.Context,
	sem *semaphore.Weighted,
	errf inErrorFunc[error],
) jobWorkerNewJobFunc {
	return func(c inErrorFunc[context.Context]) error {
		iferr := newIfErrErrorInFunc(errf)

		return contextCauseError(newJobCtx, iferr, func(context.Context) error {
			if err := sem.Acquire(newJobCtx, 1); err != nil {
				return errorWithStack(err)
			}

			go func() {
				defer sem.Release(1)

				_ = iferr(c(ctx))
			}()

			return nil
		})
	}
}

func newFuncJobWorkerWaitFunc(
	ctx, newJobCtx context.Context,
	sem *semaphore.Weighted,
	semSize uint64,
) (
	_ errorFunc,
	doneErr error,
) {
	errNewJobDone := errors.New(util.UUID().String())

	return func() error {
			<-newJobCtx.Done()

			return contextCauseError(ctx, dummyErrorInFunc, func(ctx context.Context) error {
				return sem.Acquire(ctx, int64(semSize))
			})
		},
		errNewJobDone
}

func newFuncNewJobWorkerFunc(semSize uint64) newJobWorkerFunc {
	return func(ctx context.Context) (
		jobWorkerNewJobFunc,
		func(),
		errorFunc,
		error,
	) {
		return newFuncJobWorker(ctx, semSize)
	}
}

func newFuncNewErrJobWorkerFunc(semSize uint64, errf inErrorFunc[error]) newJobWorkerFunc {
	return func(ctx context.Context) (
		jobWorkerNewJobFunc,
		func(),
		errorFunc,
		error,
	) {
		return newFuncErrJobWorker(ctx, semSize, errf)
	}
}

func funcsJobWorker(ctx context.Context, fs ...inErrorFunc[context.Context]) error {
	var newJob jobWorkerNewJobFunc
	var jobDone func()
	var jobWait errorFunc

	switch n, d, w, err := newFuncJobWorker(ctx, uint64(len(fs))); {
	case err != nil:
		return err
	default:
		newJob, jobDone, jobWait = n, d, w
	}

	go func() {
		defer jobDone()

		_ = traverseSlice(fs, func(_ int, f inErrorFunc[context.Context]) (bool, error) {
			return true, newJob(f)
		})
	}()

	return jobWait()
}
