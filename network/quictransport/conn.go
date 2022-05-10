package quictransport

import (
	"context"
	"io"
	"net"
	"os"
	"sync"
	"time"

	"github.com/pkg/errors"
	"github.com/spikeekips/mitum/util"
)

type qconn struct {
	laddr  net.Addr
	raddr  net.Addr
	writef func(context.Context, []byte) (int, error)
	closef func()
	r      *io.PipeReader
	w      *io.PipeWriter
	dr     *util.Locked
	dw     *util.Locked
	sync.RWMutex
	closeonce sync.Once
	closed    bool
}

func newQConn(
	laddr net.Addr,
	raddr net.Addr,
	writef func(context.Context, []byte) (int, error),
	closef func(),
) *qconn {
	c := &qconn{
		laddr:     laddr,
		raddr:     raddr,
		writef:    writef,
		closeonce: sync.Once{},
		closef:    closef,
		dr:        util.EmptyLocked(),
		dw:        util.EmptyLocked(),
	}

	c.r, c.w = io.Pipe()

	return c
}

func (c *qconn) Read(b []byte) (int, error) {
	if c.isclosed() {
		return c.r.Read(b)
	}

	var t time.Time
	switch i, _ := c.dr.Value(); {
	case i == nil:
		return c.r.Read(b)
	default:
		t = i.(time.Time)
		if t.IsZero() {
			return c.r.Read(b)
		}
	}

	dur := time.Until(t)
	if dur < 1 {
		return 0, errors.Wrap(context.DeadlineExceeded, "")
	}

	ctx, cancel := context.WithTimeout(context.Background(), dur)
	defer cancel()

	var n int
	var e error
	err := util.AwareContext(ctx, func() error {
		n, e = c.r.Read(b)

		return nil
	})

	switch {
	case err == nil:
		return n, nil
	case errors.Is(err, context.DeadlineExceeded):
		return n, errors.Wrap(os.ErrDeadlineExceeded, "")
	default:
		return n, e
	}
}

func (c *qconn) Write(b []byte) (int, error) {
	if c.isclosed() {
		return 0, errors.Wrap(net.ErrClosed, "")
	}

	ctx := context.Background()
	var t time.Time
	switch i, _ := c.dw.Value(); {
	case i == nil:
		return c.writef(ctx, b)
	default:
		t = i.(time.Time)
		if t.IsZero() {
			return c.writef(ctx, b)
		}
	}

	dur := time.Until(t)
	if dur < 1 {
		return 0, errors.Wrap(context.DeadlineExceeded, "")
	}

	var cancel func()
	ctx, cancel = context.WithTimeout(ctx, dur)
	defer cancel()

	n, err := c.writef(ctx, b)
	switch {
	case err == nil:
		return n, nil
	case errors.Is(err, context.DeadlineExceeded):
		return n, errors.Wrap(os.ErrDeadlineExceeded, "")
	default:
		return n, errors.Wrap(err, "")
	}
}

func (c *qconn) Close() error {
	if c.isclosed() {
		return errors.Wrap(net.ErrClosed, "")
	}

	c.closeonce.Do(func() {
		c.Lock()
		c.closed = true
		c.Unlock()

		if c.closef != nil {
			c.closef()
		}

		_ = c.w.Close()
	})

	return nil
}

func (c *qconn) LocalAddr() net.Addr {
	return c.laddr
}

func (c *qconn) RemoteAddr() net.Addr {
	return c.raddr
}

func (c *qconn) SetDeadline(t time.Time) error {
	if t.IsZero() {
		_ = c.dr.Empty()
		_ = c.dw.Empty()

		return nil
	}

	_ = c.dr.SetValue(t)
	_ = c.dw.SetValue(t)

	return nil
}

func (c *qconn) SetReadDeadline(t time.Time) error {
	if t.IsZero() {
		_ = c.dr.Empty()

		return nil
	}

	_ = c.dr.SetValue(t)

	return nil
}

func (c *qconn) SetWriteDeadline(t time.Time) error {
	if t.IsZero() {
		_ = c.dw.Empty()

		return nil
	}

	_ = c.dw.SetValue(t)

	return nil
}

func (c *qconn) isclosed() bool {
	c.RLock()
	defer c.RUnlock()

	return c.closed
}

func (c *qconn) append(b []byte) bool {
	if c.isclosed() {
		return false
	}

	go func() {
		_, _ = c.w.Write(b)
		_ = c.w.Close()
	}()

	return true
}
