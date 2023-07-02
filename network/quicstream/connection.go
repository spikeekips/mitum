package quicstream

import (
	"context"
	"crypto/tls"
	"fmt"
	"io"
	"net"

	"github.com/pkg/errors"
	"github.com/quic-go/quic-go"
	"github.com/spikeekips/mitum/util"
)

var ErrOpenStream = util.NewIDError("open stream")

type Connection struct {
	conn quic.EarlyConnection
	id   string
}

func Dial(
	ctx context.Context,
	addr *net.UDPAddr,
	tlsconfig *tls.Config,
	quicconfig *quic.Config,
) (*Connection, error) {
	conn, err := quic.DialAddrEarly(ctx, addr.String(), tlsconfig, quicconfig)
	if err != nil {
		return nil, &net.OpError{Net: "udp", Op: "dial", Err: err}
	}

	select {
	case <-ctx.Done():
		if conn != nil {
			_ = conn.CloseWithError(0, ctx.Err().Error())
		}

		return nil, &net.OpError{Net: "udp", Op: "dial", Err: ctx.Err()}
	case <-conn.HandshakeComplete():
		return &Connection{
			conn: conn,
			id:   fmt.Sprintf("%s-%s", conn.RemoteAddr(), util.UUID().String()),
		}, nil
	}
}

func (c *Connection) Stream(ctx context.Context, f StreamFunc) error {
	switch stream, err := c.openStream(ctx); {
	case err != nil:
		return ErrOpenStream.WithMessage(err, "stream")
	default:
		defer func() {
			stream.CancelRead(0)
			_ = stream.Close()
		}()

		return util.AwareContext(ctx, func(ctx context.Context) error {
			return f(ctx, stream, stream)
		})
	}
}

func (c *Connection) OpenStream(ctx context.Context) (io.Reader, io.WriteCloser, func() error, error) {
	switch stream, err := c.openStream(ctx); {
	case err != nil:
		return nil, nil, nil, ErrOpenStream.WithMessage(err, "open stream")
	default:
		return stream, stream, func() error {
			stream.CancelRead(0)

			return errors.Wrap(stream.Close(), "close stream")
		}, nil
	}
}

func (c *Connection) Close() error {
	if c.conn.Context().Err() != nil {
		return nil
	}

	if err := c.conn.CloseWithError(0, ""); err != nil { // no error
		return errors.Wrap(err, "close client")
	}

	return nil
}

func (c *Connection) Context() context.Context {
	return c.conn.Context()
}

func (c *Connection) ID() string {
	return c.id
}

func (c *Connection) openStream(ctx context.Context) (stream quic.Stream, _ error) {
	if c.conn.Context().Err() != nil {
		return nil, errors.Wrap(c.conn.Context().Err(), "closed")
	}

	stream, err := c.conn.OpenStreamSync(ctx)
	if err != nil {
		return nil, errors.WithStack(err)
	}

	return stream, nil
}
