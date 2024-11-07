package main

import (
	"context"
	"sync"

	"github.com/pkg/errors"
)

type errorFunc func(error) error

func dummyErrorFunc(err error) error {
	return err
}

func ifErrErrorFunc(f errorFunc) errorFunc {
	return func(err error) error {
		if err != nil {
			return f(err)
		}

		return nil
	}
}

func contextWithCancelCause(ctx context.Context) (context.Context, errorFunc) {
	cctx, cancel := context.WithCancelCause(ctx)

	var called bool
	var gerr error
	var once sync.Once

	return cctx, func(err error) error {
		if cerr := context.Cause(cctx); cerr != nil {
			return cerr
		}

		once.Do(func() {
			cancel(err)

			switch {
			case err == nil:
				gerr = context.Canceled
			default:
				gerr = err
			}

			called = true
		})

		if err == nil && called {
			return nil
		}

		return gerr
	}
}

func contextCauseError(ctx context.Context, ef errorFunc, f func(context.Context) error) error {
	if err := context.Cause(ctx); err != nil {
		return errors.WithStack(err)
	}

	return ef(f(ctx))
}
