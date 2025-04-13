package util

import (
	"context"
	stderrors "errors"
	"sync"
	"sync/atomic"
	"time"
)

type ContextKey string

func ContextWithCancelCause(ctx context.Context) (context.Context, InErrorFunc[error]) {
	cctx, cancel := context.WithCancelCause(ctx)

	var gerr error
	var once sync.Once

	return cctx, func(err error) error {
		var called bool

		once.Do(func() {
			switch err {
			case nil:
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
		return err
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
	var done int64

	cctx, cctxCancel := ContextWithCancelCause(ctx)

	var once sync.Once

	return cctx, func(err error) error {
			once.Do(func() {
				if err == nil && atomic.CompareAndSwapInt64(&done, 0, 1) {
					_ = cctxCancel(nil)
				}

				_ = cctxCancel(err)
			})

			return err
		},
		func() (bool, error) {
			gerr := context.Cause(ctx)

			switch err := context.Cause(cctx); {
			case err != nil:
				if atomic.LoadInt64(&done) == 1 {
					return false, gerr
				}

				return false, err
			default:
				return true, gerr
			}
		}
}

func NewFuncCheckContextError(f func() (bool, error), cancel InErrorFunc[error]) func() (bool, error) {
	return func() (bool, error) {
		ok, err := f()
		if err != nil || !ok {
			_ = cancel(err)
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
				gerr = err

				close(done)
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

type JoinedContext struct {
	c0 context.Context //nolint:containedctx
	c1 context.Context //nolint:containedctx
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
