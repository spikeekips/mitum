package quicstream

import (
	"context"
	"sync/atomic"
	"time"

	"github.com/pkg/errors"
	dutil "github.com/spikeekips/mitum/example/test364_5/util"
)

func contextWithCancelCause(ctx context.Context) (context.Context, dutil.InErrorFunc[error]) {
	cctx, cancel := context.WithCancelCause(ctx)

	var p atomic.Pointer[error]

	return cctx, func(err error) error {
		if p.CompareAndSwap(nil, &err) {

			cancel(err)

			return err
		}

		if i := p.Load(); i != nil {
			return *i
		}

		return nil
	}
}

func contextEnsureWait(ctx context.Context, f dutil.InErrorFunc[context.Context]) error {
	if ctx.Err() != nil {
		return contextCause(ctx)
	}

	done, wait := newContextDone(ctx)
	defer done(nil)

	go func() {
		_ = done(f(ctx))
	}()

	return wait()
}

func contextEnsureWaitT[T any](ctx context.Context, f func(context.Context) (T, error)) (T, error) {
	var t T

	if ctx.Err() != nil {
		return t, contextCause(ctx)
	}

	done, wait := newContextDone(ctx)
	defer done(nil)

	var p atomic.Pointer[T]

	go func() {
		i, err := f(ctx)
		if err == nil {
			p.Store(&i)
		}

		_ = done(err)
	}()

	if err := wait(); err != nil {
		return t, err
	}

	if i := p.Load(); i != nil {
		t = *i
	}

	return t, nil
}

func newContextDone(ctx context.Context) (
	dutil.InErrorFunc[error],
	dutil.ErrorFunc,
) {
	done := make(chan struct{})

	var gerr atomic.Pointer[error]

	return func(err error) error {
			if gerr.CompareAndSwap(nil, &err) {
				close(done)
			}

			return err
		},
		func() error {
			select {
			case <-ctx.Done():
				if i := gerr.Load(); i != nil {
					return *i
				}

				return contextCause(ctx)
			case <-done:
				if i := gerr.Load(); i != nil {
					return *i
				}

				return nil
			}
		}
}

type joinedContext struct {
	c0 context.Context //nolint:containedctx
	c1 context.Context //nolint:containedctx
}

var _ context.Context = &joinedContext{}

func joinContext(c0, c1 context.Context) *joinedContext {
	return &joinedContext{c0: c0, c1: c1}
}

func (c *joinedContext) Deadline() (time.Time, bool) {
	if i, j := c.c1.Deadline(); j {
		return i, j
	}

	return c.c0.Deadline()
}

func (c *joinedContext) Done() <-chan struct{} {
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

func (c *joinedContext) Err() error {
	if e := c.c1.Err(); e != nil {
		return e
	}

	return c.c0.Err()
}

func (c *joinedContext) Value(key any) any {
	if v := c.c1.Value(key); v != nil {
		return v
	}

	return c.c0.Value(key)
}

func (c *joinedContext) cause() error {
	var c1err error

	if err := context.Cause(c.c1); err != nil {
		if !errors.Is(err, context.Canceled) {
			return err
		}

		c1err = err
	}

	if err := context.Cause(c.c0); err != nil {
		switch {
		case !errors.Is(err, context.Canceled):
			return err
		case c1err == nil:
			return err
		}
	}

	return c1err
}

func contextCause(ctx context.Context) error {
	if c, ok := ctx.(*joinedContext); ok {
		return c.cause()
	}

	return context.Cause(ctx)
}
