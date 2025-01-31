package util

import (
	"context"
	stderrors "errors"
	"sync"

	"github.com/pkg/errors"
	"github.com/spikeekips/mitum/util"
)

func ContextWithCancelCause(ctx context.Context) (context.Context, InErrorFunc[error]) {
	cctx, cancel := context.WithCancelCause(ctx)

	var gerr error
	var once sync.Once

	return cctx, func(err error) error {
		var called bool

		once.Do(func() {
			switch {
			case err == nil:
				gerr = ErrorWithStack(context.Canceled)

				cancel(nil)
			default:
				gerr = stderrors.Join(err, context.Canceled)

				cancel(gerr)
			}

			called = true
		})

		if err == nil && called {
			return nil
		}

		return gerr
	}
}

func ContextCauseError(ctx context.Context, ef InErrorFunc[error], f InErrorFunc[context.Context]) error {
	if err := context.Cause(ctx); err != nil {
		return ErrorWithStack(err)
	}

	if ef == nil {
		return f(ctx)
	}

	return ef(f(ctx))
}

func NewContextWait(ctx context.Context) (
	context.Context,
	InErrorFunc[error],
	func() (bool, error),
	InErrorFunc[error],
) {
	cctx, cctxCancel, check := CheckContextWait(ctx)

	return cctx, cctxCancel, check, func(err error) error {
		if err != nil {
			return cctxCancel(err)
		}

		<-cctx.Done()

		_, err = check()

		return err
	}
}

func CheckContextWait(ctx context.Context) (
	context.Context,
	InErrorFunc[error],
	func() (bool, error),
) {
	errDone := errors.New(util.UUID().String())
	cctx, cctxCancel := ContextWithCancelCause(ctx)

	var once sync.Once

	return cctx, func(err error) error {
			once.Do(func() {
				if err == nil {
					cctxCancel(errDone)

					return
				}

				cctxCancel(err)
			})

			return err
		},
		func() (bool, error) {
			if err := context.Cause(ctx); err != nil {
				return false, err
			}

			switch err := context.Cause(cctx); {
			case errors.Is(err, errDone):
				return false, nil
			case err != nil:
				return false, err
			default:
				return true, nil
			}
		}
}

func NewFuncCheckParentContextError(f func() (bool, error), cancel InErrorFunc[error]) func() (bool, error) {
	return func() (bool, error) {
		ok, err := f()
		if err != nil || !ok {
			cancel(err)
		}

		return ok, err
	}
}

func NewContextDone(ctx context.Context) (
	InErrorFunc[error],
	ErrorFunc,
) {
	done := make(chan struct{})

	var gerr error
	var once sync.Once

	return func(err error) error {
			once.Do(func() {
				close(done)
				gerr = err
			})

			return err
		},
		func() error {
			select {
			case <-ctx.Done():
				return context.Cause(ctx)
			case <-done:
				return gerr
			}
		}
}
