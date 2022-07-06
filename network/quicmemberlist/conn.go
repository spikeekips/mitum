package quicmemberlist

import (
	"context"
	"io"
	"net"
	"os"
	"sync"
	"time"

	"github.com/pkg/errors"
	"github.com/spikeekips/mitum/network"
	"github.com/spikeekips/mitum/network/quicstream"
	"github.com/spikeekips/mitum/util"
)

type NamedAddr struct {
	addr string
}

func (NamedAddr) Network() string {
	return "udp"
}

func (a NamedAddr) String() string {
	return a.addr
}

type NamedConnInfo struct {
	addr        NamedAddr
	tlsinsecure bool
}

func NewNamedConnInfo(addr string, tlsinsecure bool) NamedConnInfo {
	return NamedConnInfo{
		addr: NamedAddr{addr: addr}, tlsinsecure: tlsinsecure,
	}
}

func (c NamedConnInfo) UDPConnInfo() (ci quicstream.UDPConnInfo, _ error) {
	udp, err := net.ResolveUDPAddr("udp", c.addr.String())
	if err != nil {
		return ci, errors.Wrap(err, "failed to resolve NamedConnInfo")
	}

	return quicstream.NewUDPConnInfo(udp, c.tlsinsecure), nil
}

func (c NamedConnInfo) Addr() net.Addr {
	return c.addr
}

func (c NamedConnInfo) TLSInsecure() bool {
	return c.tlsinsecure
}

func (c NamedConnInfo) IsValid([]byte) error {
	e := util.ErrInvalid.Errorf("invalid NamedConnInfo")

	if err := network.IsValidAddr(c.addr.String()); err != nil {
		return e.Wrap(err)
	}

	return nil
}

func (c NamedConnInfo) String() string {
	return network.ConnInfoToString(c.addr.String(), c.tlsinsecure)
}

func (c NamedConnInfo) MarshalText() ([]byte, error) {
	return []byte(c.String()), nil
}

func (c *NamedConnInfo) UnmarshalText(b []byte) error {
	addr, tlsinsecure := network.ParseTLSInsecure(string(b))

	c.addr = NamedAddr{addr: addr}
	c.tlsinsecure = tlsinsecure

	return nil
}

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
		n, err := c.r.Read(b)

		return n, errors.WithStack(err)
	}

	var t time.Time

	switch i, _ := c.dr.Value(); {
	case i == nil:
		n, err := c.r.Read(b)

		return n, errors.WithStack(err)
	default:
		t = i.(time.Time) //nolint:forcetypeassert // ...
		if t.IsZero() {
			n, err := c.r.Read(b)

			return n, errors.WithStack(err)
		}
	}

	dur := time.Until(t)
	if dur < 1 {
		return 0, errors.WithStack(context.DeadlineExceeded)
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
		return n, errors.WithStack(os.ErrDeadlineExceeded)
	default:
		return n, errors.WithStack(e)
	}
}

func (c *qconn) Write(b []byte) (int, error) {
	if c.isclosed() {
		return 0, errors.WithStack(net.ErrClosed)
	}

	ctx := context.Background()
	var t time.Time

	switch i, _ := c.dw.Value(); {
	case i == nil:
		return c.writef(ctx, b)
	default:
		t = i.(time.Time) //nolint:forcetypeassert // ...
		if t.IsZero() {
			return c.writef(ctx, b)
		}
	}

	dur := time.Until(t)
	if dur < 1 {
		return 0, errors.WithStack(context.DeadlineExceeded)
	}

	var cancel func()
	ctx, cancel = context.WithTimeout(ctx, dur)

	defer cancel()

	n, err := c.writef(ctx, b)

	switch {
	case err == nil:
		return n, nil
	case errors.Is(err, context.DeadlineExceeded):
		return n, errors.WithStack(os.ErrDeadlineExceeded)
	default:
		return n, err
	}
}

func (c *qconn) Close() error {
	if c.isclosed() {
		return errors.WithStack(net.ErrClosed)
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

func (c *qconn) writeClose(b []byte) bool {
	if c.isclosed() {
		return false
	}

	go func() {
		_, _ = c.w.Write(b)
		_ = c.w.Close()
	}()

	return true
}
