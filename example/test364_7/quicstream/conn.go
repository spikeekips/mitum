package quicstream

import (
	"context"
	"net"
	"sync/atomic"

	"github.com/quic-go/quic-go"
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
