package main

import (
	"context"

	"github.com/pkg/errors"
	"github.com/spikeekips/mitum/util"
	"golang.org/x/sync/semaphore"
)

var errJobWorkerDone = util.NewIDError("job worker: no more new job")

type (
	contextWorkerCallback func(context.Context) error
	jobWorkerNewJobFunc   func(contextWorkerCallback) error
	jobWorkerJobDoneFunc  func()
	jobWorkerWaitFunc     func() error
	newJobWorkerFunc      func(context.Context) (
		jobWorkerNewJobFunc,
		jobWorkerJobDoneFunc,
		jobWorkerWaitFunc,
		error,
	)
)

func newBaseJobWorker(
	ctx context.Context,
	semSize int64,
	errf errorFunc,
) (
	jobWorkerNewJobFunc,
	jobWorkerJobDoneFunc,
	jobWorkerWaitFunc,
	error,
) {
	if semSize < 1 {
		return nil, nil, nil, errors.Errorf("semSize under 1")
	}

	jctx, jctxCancel := contextWithCancelCause(ctx)

	sem := semaphore.NewWeighted(semSize)

	newJobf := newJobWorkerNewJobFunc(ctx, jctx, sem, errf)
	waitf, errNewJobDone := newJobWorkerWaitFunc(ctx, jctx, sem, semSize, errf)

	return func(c contextWorkerCallback) error {
			switch err := newJobf(c); {
			case errors.Is(err, errNewJobDone):
				return errJobWorkerDone.WithStack()
			default:
				return err
			}
		},
		func() {
			jctxCancel(errNewJobDone)
		},
		func() error {
			defer jctxCancel(nil)

			if err := context.Cause(ctx); err != nil {
				return errors.WithStack(err)
			}

			waitch := make(chan error, 1)

			go func() {
				waitch <- waitf()
			}()

			select {
			case err := <-waitch:
				return err
			case <-ctx.Done():
				return errors.WithStack(context.Cause(ctx))
			}
		},
		nil
}

func newJobWorker(ctx context.Context, semSize int64) (
	jobWorkerNewJobFunc,
	jobWorkerJobDoneFunc,
	jobWorkerWaitFunc,
	error,
) {
	wctx, wctxCancel := contextWithCancelCause(ctx)

	newJob, jobDone, wait, err := newBaseJobWorker(
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

func newErrJobWorker(ctx context.Context, semSize int64, errf errorFunc) (
	jobWorkerNewJobFunc,
	jobWorkerJobDoneFunc,
	jobWorkerWaitFunc,
	error,
) {
	wctx, wctxCancel := contextWithCancelCause(ctx)

	ierrf := ifErrErrorFunc(wctxCancel)

	newJob, jobDone, wait, err := newBaseJobWorker(
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

func newJobWorkerNewJobFunc(
	ctx, newJobCtx context.Context,
	sem *semaphore.Weighted,
	errf errorFunc,
) jobWorkerNewJobFunc {
	return func(c contextWorkerCallback) error {
		if err := context.Cause(newJobCtx); err != nil {
			return errors.WithStack(err)
		}

		if err := sem.Acquire(newJobCtx, 1); err != nil {
			return errors.WithStack(err)
		}

		go func() {
			if err := c(ctx); err != nil {
				errf(err)
			}

			sem.Release(1)
		}()

		return nil
	}
}

func newJobWorkerWaitFunc(
	ctx, newJobCtx context.Context,
	sem *semaphore.Weighted,
	semSize int64,
	ctxCancel errorFunc,
) (
	_ jobWorkerWaitFunc,
	doneErr error,
) {
	errNewJobDone := errors.New(util.UUID().String())

	return func() error {
			<-newJobCtx.Done()

			switch err := context.Cause(ctx); {
			case err == nil, errors.Is(err, errNewJobDone):
			default:
				return errors.WithStack(err)
			}

			if err := sem.Acquire(ctx, semSize); err != nil {
				if cerr := context.Cause(ctx); cerr != nil {
					return errors.WithStack(cerr)
				}

				return errors.WithStack(err)
			}

			return errors.WithStack(context.Cause(ctx))
		},
		errNewJobDone
}

func newFuncJobWorker(semSize int64) newJobWorkerFunc {
	return func(ctx context.Context) (
		jobWorkerNewJobFunc,
		jobWorkerJobDoneFunc,
		jobWorkerWaitFunc,
		error,
	) {
		return newJobWorker(ctx, semSize)
	}
}

func newFuncErrJobWorker(semSize int64, errf errorFunc) newJobWorkerFunc {
	return func(ctx context.Context) (
		jobWorkerNewJobFunc,
		jobWorkerJobDoneFunc,
		jobWorkerWaitFunc,
		error,
	) {
		return newErrJobWorker(ctx, semSize, errf)
	}
}
