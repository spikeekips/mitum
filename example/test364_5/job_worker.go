package main

import (
	"context"

	"github.com/pkg/errors"
	"github.com/spikeekips/mitum/util"
	"golang.org/x/sync/semaphore"
)

var errJobWorkerDone = util.NewIDError("job worker: no more new job")

type (
	jobWorkerNewJobFunc func(inErrorFunc[context.Context]) error
	newJobWorkerFunc    func(context.Context) (
		jobWorkerNewJobFunc,
		func(),
		errorFunc,
		error,
	)
)

func newFuncBaseJobWorker(
	ctx context.Context,
	semSize int64,
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

	sem := semaphore.NewWeighted(semSize)

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

func newFuncJobWorker(ctx context.Context, semSize int64) (
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

func newFuncErrJobWorker(ctx context.Context, semSize int64, errf inErrorFunc[error]) (
	jobWorkerNewJobFunc,
	func(),
	errorFunc,
	error,
) {
	wctx, wctxCancel := contextWithCancelCause(ctx)

	ierrf := ifErrErrorInFunc(wctxCancel)

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
		if err := context.Cause(newJobCtx); err != nil {
			return errors.WithStack(err)
		}

		if err := sem.Acquire(newJobCtx, 1); err != nil {
			return errors.WithStack(err)
		}

		go func() {
			if err := c(ctx); err != nil {
				_ = errf(err)
			}

			sem.Release(1)
		}()

		return nil
	}
}

func newFuncJobWorkerWaitFunc(
	ctx, newJobCtx context.Context,
	sem *semaphore.Weighted,
	semSize int64,
) (
	_ errorFunc,
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

func newFuncNewJobWorkerFunc(semSize int64) newJobWorkerFunc {
	return func(ctx context.Context) (
		jobWorkerNewJobFunc,
		func(),
		errorFunc,
		error,
	) {
		return newFuncJobWorker(ctx, semSize)
	}
}

func newFuncNewErrJobWorkerFunc(semSize int64, errf inErrorFunc[error]) newJobWorkerFunc {
	return func(ctx context.Context) (
		jobWorkerNewJobFunc,
		func(),
		errorFunc,
		error,
	) {
		return newFuncErrJobWorker(ctx, semSize, errf)
	}
}
