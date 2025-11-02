package quicstream

import (
	"context"
	"net"
	"slices"
	"sync"
	"sync/atomic"
	"time"

	"github.com/pkg/errors"
	"github.com/quic-go/quic-go"
	dutil "github.com/spikeekips/mitum/example/test364_5/util"
)

type (
	OpenStreamFunc = func(context.Context) (*quic.Stream, error)
)

type Conn interface {
	AcceptStream(context.Context) (*quic.Stream, error)
	AcceptUniStream(context.Context) (*quic.ReceiveStream, error)
	AddPath(*quic.Transport) (*quic.Path, error)
	CloseWithError(quic.ApplicationErrorCode, string) error
	ConnectionState() quic.ConnectionState
	ConnectionStats() quic.ConnectionStats
	Context() context.Context
	HandshakeComplete() <-chan struct{}
	LocalAddr() net.Addr
	NextConnection(context.Context) (*quic.Conn, error)
	OpenStream() (*quic.Stream, error)
	OpenStreamSync(context.Context) (*quic.Stream, error)
	OpenUniStream() (*quic.SendStream, error)
	OpenUniStreamSync(context.Context) (*quic.SendStream, error)
	ReceiveDatagram(context.Context) ([]byte, error)
	RemoteAddr() net.Addr
	SendDatagram([]byte) error
}

type Stream interface {
	CancelRead(quic.StreamErrorCode)
	CancelWrite(quic.StreamErrorCode)
	Close() error
	Context() context.Context
	Read([]byte) (int, error)
	SetDeadline(time.Time) error
	SetReadDeadline(time.Time) error
	SetWriteDeadline(time.Time) error
	StreamID() quic.StreamID
	Write([]byte) (int, error)
}

type noCloseConn struct {
	Conn
	ic atomic.Value
}

func (c *noCloseConn) AcceptStream(ctx context.Context) (*quic.Stream, error) {
	if err := c.isClosed(); err != nil {
		return nil, err
	}

	return c.Conn.AcceptStream(ctx)
}

func (c *noCloseConn) AcceptUniStream(ctx context.Context) (*quic.ReceiveStream, error) {
	if err := c.isClosed(); err != nil {
		return nil, err
	}

	return c.Conn.AcceptUniStream(ctx)
}

func (c *noCloseConn) AddPath(t *quic.Transport) (*quic.Path, error) {
	if err := c.isClosed(); err != nil {
		return nil, err
	}

	return c.Conn.AddPath(t)
}

func (c *noCloseConn) HandshakeComplete() <-chan struct{} {
	if err := c.isClosed(); err != nil {
		return nil
	}

	return c.Conn.HandshakeComplete()
}

func (c *noCloseConn) NextConnection(ctx context.Context) (*quic.Conn, error) {
	if err := c.isClosed(); err != nil {
		return nil, err
	}

	return c.Conn.NextConnection(ctx)
}

func (c *noCloseConn) OpenStream() (*quic.Stream, error) {
	if err := c.isClosed(); err != nil {
		return nil, err
	}

	return c.Conn.OpenStream()
}

func (c *noCloseConn) OpenStreamSync(ctx context.Context) (*quic.Stream, error) {
	if err := c.isClosed(); err != nil {
		return nil, err
	}

	return c.Conn.OpenStreamSync(ctx)
}

func (c *noCloseConn) OpenUniStream() (*quic.SendStream, error) {
	if err := c.isClosed(); err != nil {
		return nil, err
	}

	return c.Conn.OpenUniStream()
}

func (c *noCloseConn) OpenUniStreamSync(ctx context.Context) (*quic.SendStream, error) {
	if err := c.isClosed(); err != nil {
		return nil, err
	}

	return c.Conn.OpenUniStreamSync(ctx)
}

func (c *noCloseConn) ReceiveDatagram(ctx context.Context) ([]byte, error) {
	if err := c.isClosed(); err != nil {
		return nil, err
	}

	return c.Conn.ReceiveDatagram(ctx)
}

func (c *noCloseConn) SendDatagram(b []byte) error {
	if err := c.isClosed(); err != nil {
		return err
	}

	return c.Conn.SendDatagram(b)
}

func (c *noCloseConn) CloseWithError(errcode quic.ApplicationErrorCode, desc string) error {
	if !c.ic.CompareAndSwap(nil, true) {
		return quic.ErrTransportClosed
	}

	if errcode == connErrorCodeOK {
		return nil
	}

	return c.Conn.CloseWithError(errcode, desc)
}

func (c *noCloseConn) isClosed() error {
	if i := c.ic.Load(); i != nil && i.(bool) {
		return quic.ErrTransportClosed
	}

	return nil
}

func newNoCloseConn(conn Conn) Conn {
	if _, ok := conn.(*noCloseConn); ok {
		return conn
	}

	return &noCloseConn{Conn: conn}
}

func newFuncCloseConnection(err error) dutil.InErrorFunc[Conn] {
	var errcode quic.ApplicationErrorCode
	var errmsg string

	switch {
	case err != nil:
		errcode = connErrorCodeUnknown
		errmsg = err.Error()
	default:
		errcode = connErrorCodeOK
	}

	return func(conn Conn) error {
		return errors.WithStack(conn.CloseWithError(errcode, errmsg))
	}
}

func closeConnection(conn Conn, err error) error {
	return newFuncCloseConnection(err)(conn)
}

func newFuncCloseStream(err error) func(Stream) error {
	switch {
	case err != nil:
		return func(stream Stream) error {
			stream.CancelRead(streamErrorCodeUnknown)
			stream.CancelWrite(streamErrorCodeUnknown)

			return nil
		}
	default:
		return func(stream Stream) error {
			return stream.Close()
		}
	}
}

func closeStream(stream Stream, err error) error {
	return newFuncCloseStream(err)(stream)
}

func closeStreams(streams []Stream, err error) (gerr error) {
	f := newFuncCloseStream(err)

	switch len(streams) {
	case 0:
		return nil
	case 1:
		if streams[0] == nil {
			return nil
		}

		return f(streams[0])
	}

	var wg sync.WaitGroup
	var e int64

	for stream := range slices.Values(streams) {
		if stream != nil {
			wg.Go(func() {
				if err := f(stream); err != nil {
					if atomic.CompareAndSwapInt64(&e, 0, 1) {
						gerr = err
					}
				}
			})
		}
	}

	wg.Wait()

	return gerr
}
