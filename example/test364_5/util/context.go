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

	var called bool
	var gerr error
	var once sync.Once

	return cctx, func(err error) error {
		if cerr := context.Cause(cctx); cerr != nil {
			return ErrorWithStack(cerr)
		}

		once.Do(func() {
			switch {
			case err == nil:
				gerr = context.Canceled

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

func NewContextDone(ctx context.Context) (
	context.Context,
	InErrorFunc[error],
	func() error,
) {
	errDone := errors.New(util.UUID().String())
	cctx, cctxCancel := context.WithCancelCause(ctx)

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
		func() error {
			<-cctx.Done()

			switch err := context.Cause(cctx); {
			case errors.Is(err, errDone):
				return nil
			default:
				return err
			}
		}
}
