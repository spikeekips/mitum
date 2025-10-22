package quicstream

import (
	"context"
	"crypto/sha3"
	"crypto/tls"
	"net"

	"github.com/pkg/errors"
	"github.com/quic-go/quic-go"
	dutil "github.com/spikeekips/mitum/example/test364_5/util"
	"github.com/spikeekips/mitum/util"
)

const (
	connErrorCodeUnknown quic.ApplicationErrorCode = iota
	connErrorCodeOK
)

var (
	LocalAddrContextKey  dutil.ContextKey = "local_addr"
	RemoteAddrContextKey dutil.ContextKey = "remote_addr"
)

const (
	streamErrorCodeOK quic.StreamErrorCode = iota
	streamErrorCodeUnknown
)

func Serve(
	ctx context.Context,
	bind *net.UDPAddr,
	tlsConfig *tls.Config,
	quicConfig *quic.Config,
	statelessResetKey *quic.StatelessResetKey,
) (
	dutil.InErrorFunc[StreamHandler],
	dutil.ErrorFunc,
	error,
) {
	var pconn net.PacketConn
	switch c, err := NewPacketConn(bind); {
	case err != nil:
		return nil, nil, errors.WithStack(err)
	default:
		pconn = c
	}

	switch h, c, err := serveWithPacketConn(ctx, pconn, tlsConfig, quicConfig, statelessResetKey); {
	case err != nil:
		return nil, nil, err
	default:
		return h,
			func() error {
				defer func() {
					_ = pconn.Close()
				}()

				if err := c(); err != nil {
					return err
				}

				return pconn.Close()
			},
			nil
	}
}

func serveWithPacketConn(
	ctx context.Context,
	pconn net.PacketConn,
	tlsConfig *tls.Config,
	quicConfig *quic.Config,
	statelessResetKey *quic.StatelessResetKey,
) (
	dutil.InErrorFunc[StreamHandler],
	dutil.ErrorFunc,
	error,
) {
	if !quicConfig.Allow0RTT {
		return nil, nil, errors.New("Allow0RTT is disabled")
	}

	if !quicConfig.EnableDatagrams {
		return nil, nil, errors.New("EnableDatagrams is disabled")
	}

	tp := quic.Transport{
		Conn:                             pconn,
		StatelessResetKey:                statelessResetKey,
		DisableVersionNegotiationPackets: true,
	}

	var listener *quic.EarlyListener

	switch l, err := tp.ListenEarly(tlsConfig, quicConfig); {
	case err != nil:
		return nil, nil, errors.WithStack(err)
	default:
		listener = l
	}

	return func(f StreamHandler) error {
			switch err := serveListener(ctx, listener, f); {
			case err == nil,
				errors.Is(err, quic.ErrServerClosed):
				return nil
			default:
				return err
			}
		},
		func() error {
			return errors.WithStack(listener.Close())
		},
		nil
}

func serveListener(
	ctx context.Context,
	listener *quic.EarlyListener,
	f StreamHandler,
) error {
	for {
		switch conn, err := listener.Accept(ctx); {
		case err != nil:
			return errors.WithStack(err)
		default:
			go func() {
				_ = serveConnection(ctx, conn, f)
			}()

			go func() {
				_ = exhaustDatagram(ctx, conn)
			}()
		}
	}
}

func serveConnection(
	ctx context.Context,
	conn *quic.Conn,
	f StreamHandler,
) error {
	bctx := context.WithValue(ctx, LocalAddrContextKey, conn.LocalAddr())
	bctx = context.WithValue(bctx, RemoteAddrContextKey, conn.RemoteAddr())

	for {
		switch stream, err := conn.AcceptStream(bctx); {
		case err != nil:
			return errors.WithStack(err)
		default:
			go func() {
				_, err := f(ctx, stream)
				_ = closeStream(stream, err)
			}()
		}
	}
}

func closeConnection(conn QuicConn, err error) error {
	var errcode quic.ApplicationErrorCode
	var errmsg string

	switch {
	case err != nil:
		errcode = connErrorCodeUnknown
		errmsg = err.Error()
	default:
		errcode = connErrorCodeOK
	}

	return errors.WithStack(conn.CloseWithError(errcode, errmsg))
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
			stream.CancelRead(streamErrorCodeOK)

			return stream.Close()
		}
	}
}

func closeStream(stream *quic.Stream, err error) error {
	return newFuncCloseStream(err)(stream)
}

func exhaustDatagram(ctx context.Context, conn QuicConn) error {
	var nerr *net.OpError
	var aerr *quic.ApplicationError

	var err error

	for {
		switch _, err = conn.ReceiveDatagram(ctx); {
		case errors.As(err, &aerr),
			errors.Is(err, quic.ErrTransportClosed),
			errors.As(err, &nerr),
			errors.Is(err, context.Canceled):
			return err
		case err != nil:
			panic(errors.Errorf("exhaustDatagram: unknown error, %T %+v\n", err, err))
		}
	}
}

func NewStatelessResetKey(b []byte) *quic.StatelessResetKey {
	i := quic.StatelessResetKey(sha3.Sum256(b))

	return &i
}

func DefaultStatelessResetKey(tlsConfig *tls.Config) *quic.StatelessResetKey {
	if c := tlsConfig.Certificates; len(c) > 0 {
		if i, ok := c[0].PrivateKey.(interface{ Bytes() ([]byte, error) }); ok {
			if b, err := i.Bytes(); err == nil {
				return NewStatelessResetKey(b)
			}
		}
	}

	return NewStatelessResetKey(util.UUID().Bytes())
}
