package quicstream

import (
	"context"
	"net"
	"slices"
	"sync"
	"sync/atomic"

	"github.com/pkg/errors"
	"github.com/quic-go/quic-go"
	dutil "github.com/spikeekips/mitum/example/test364_5/util"
)

type QuicConn interface {
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

type noCloseQuicConn struct {
	QuicConn
	ic atomic.Value
}

func (c *noCloseQuicConn) AcceptStream(ctx context.Context) (*quic.Stream, error) {
	if err := c.isClosed(); err != nil {
		return nil, err
	}

	return c.QuicConn.AcceptStream(ctx)
}

func (c *noCloseQuicConn) AcceptUniStream(ctx context.Context) (*quic.ReceiveStream, error) {
	if err := c.isClosed(); err != nil {
		return nil, err
	}

	return c.QuicConn.AcceptUniStream(ctx)
}

func (c *noCloseQuicConn) AddPath(t *quic.Transport) (*quic.Path, error) {
	if err := c.isClosed(); err != nil {
		return nil, err
	}

	return c.QuicConn.AddPath(t)
}

func (c *noCloseQuicConn) HandshakeComplete() <-chan struct{} {
	if err := c.isClosed(); err != nil {
		return nil
	}

	return c.QuicConn.HandshakeComplete()
}

func (c *noCloseQuicConn) NextConnection(ctx context.Context) (*quic.Conn, error) {
	if err := c.isClosed(); err != nil {
		return nil, err
	}

	return c.QuicConn.NextConnection(ctx)
}

func (c *noCloseQuicConn) OpenStream() (*quic.Stream, error) {
	if err := c.isClosed(); err != nil {
		return nil, err
	}

	return c.QuicConn.OpenStream()
}

func (c *noCloseQuicConn) OpenStreamSync(ctx context.Context) (*quic.Stream, error) {
	if err := c.isClosed(); err != nil {
		return nil, err
	}

	return c.QuicConn.OpenStreamSync(ctx)
}

func (c *noCloseQuicConn) OpenUniStream() (*quic.SendStream, error) {
	if err := c.isClosed(); err != nil {
		return nil, err
	}

	return c.QuicConn.OpenUniStream()
}

func (c *noCloseQuicConn) OpenUniStreamSync(ctx context.Context) (*quic.SendStream, error) {
	if err := c.isClosed(); err != nil {
		return nil, err
	}

	return c.QuicConn.OpenUniStreamSync(ctx)
}

func (c *noCloseQuicConn) ReceiveDatagram(ctx context.Context) ([]byte, error) {
	if err := c.isClosed(); err != nil {
		return nil, err
	}

	return c.QuicConn.ReceiveDatagram(ctx)
}

func (c *noCloseQuicConn) SendDatagram(b []byte) error {
	if err := c.isClosed(); err != nil {
		return err
	}

	return c.QuicConn.SendDatagram(b)
}

func (c *noCloseQuicConn) CloseWithError(errcode quic.ApplicationErrorCode, desc string) error {
	if !c.ic.CompareAndSwap(nil, true) {
		return quic.ErrTransportClosed
	}

	if errcode == connErrorCodeOK {
		return nil
	}

	return c.QuicConn.CloseWithError(errcode, desc)
}

func (c *noCloseQuicConn) isClosed() error {
	if i := c.ic.Load(); i != nil && i.(bool) {
		return quic.ErrTransportClosed
	}

	return nil
}

func newNoCloseQuicConn(conn QuicConn) QuicConn {
	if _, ok := conn.(*noCloseQuicConn); ok {
		return conn
	}

	return &noCloseQuicConn{QuicConn: conn}
}

func newFuncCloseConnection(err error) dutil.InErrorFunc[QuicConn] {
	var errcode quic.ApplicationErrorCode
	var errmsg string

	switch {
	case err != nil:
		errcode = connErrorCodeUnknown
		errmsg = err.Error()
	default:
		errcode = connErrorCodeOK
	}

	return func(conn QuicConn) error {
		return errors.WithStack(conn.CloseWithError(errcode, errmsg))
	}
}

func closeConnection(conn QuicConn, err error) error {
	return newFuncCloseConnection(err)(conn)
}

func newFuncCloseStream(err error) func(*quic.Stream) error {
	switch {
	case err != nil:
		return func(stream *quic.Stream) error {
			stream.CancelRead(streamErrorCodeUnknown)
			stream.CancelWrite(streamErrorCodeUnknown)

			return nil
		}
	default:
		return func(stream *quic.Stream) error {
			return stream.Close()
		}
	}
}

func closeStream(stream *quic.Stream, err error) error {
	return newFuncCloseStream(err)(stream)
}

func closeStreams(streams []*quic.Stream, err error) (gerr error) {
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
