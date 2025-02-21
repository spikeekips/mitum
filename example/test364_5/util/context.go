package util

import (
	"context"
	stderrors "errors"
	"sync"
	"time"

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
	ErrorFunc,
) {
	cctx, cctxCancel, check := CheckContextWait(ctx)

	return cctx, cctxCancel, check, func() error {
		<-cctx.Done()

		_, err := check()

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
			switch err := context.Cause(cctx); {
			case errors.Is(err, errDone):
				return false, nil
			case err != nil:
				return false, err
			}

			if err := context.Cause(ctx); err != nil {
				return false, err
			}

			return true, nil
		}
}

func NewFuncCheckContextError(f func() (bool, error), cancel InErrorFunc[error]) func() (bool, error) {
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

	gerr := util.EmptyLocked[error]()
	var once sync.Once

	return func(err error) error {
			once.Do(func() {
				close(done)

				gerr.SetValue(err)
			})

			if i, isempty := gerr.Value(); !isempty {
				return i
			}

			return err
		},
		func() error {
			select {
			case <-ctx.Done():
				return context.Cause(ctx)
			case <-done:
				if i, isempty := gerr.Value(); !isempty {
					return i
				}

				return nil
			}
		}
}

type JoinedContext struct {
	c0 context.Context
	c1 context.Context
}

var _ context.Context = &JoinedContext{}

func JoinContext(c0, c1 context.Context) *JoinedContext {
	return &JoinedContext{c0: c0, c1: c1}
}

func (c *JoinedContext) Deadline() (time.Time, bool) {
	if i, j := c.c1.Deadline(); j {
		return i, j
	}

	return c.c0.Deadline()
}

func (c *JoinedContext) Done() <-chan struct{} {
	ch := make(chan struct{})

	go func() {
		select {
		case <-c.c1.Done():
			close(ch)
		case <-c.c0.Done():
			close(ch)
		}
	}()

	return ch
}

func (c *JoinedContext) Err() error {
	if e := c.c1.Err(); e != nil {
		return e
	}

	return c.c0.Err()
}

func (c *JoinedContext) Value(key any) any {
	if v := c.c1.Value(key); v != nil {
		return v
	}

	return c.c0.Value(key)
}
