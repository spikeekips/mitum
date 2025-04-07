package quicstream

import (
	"context"
	"crypto/tls"
	"net"

	"github.com/pkg/errors"
	"github.com/quic-go/quic-go"
	dutil "github.com/spikeekips/mitum/example/test364_5/util"
)

var (
	QuicConnectionContextKey    dutil.ContextKey = "quic_connection"
	QuicConnectionUIDContextKey dutil.ContextKey = "quic_connection_uid"
	RemoteAddrContextKey        dutil.ContextKey = "remote_addr"
	StreamIDContextKey          dutil.ContextKey = "stream_id"
)

func Serve(
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
	tp := quic.Transport{
		Conn:                             pconn,
		StatelessResetKey:                statelessResetKey,
		DisableVersionNegotiationPackets: true,
	}

	var listener *quic.EarlyListener

	switch l, err := tp.ListenEarly(tlsConfig, quicConfig); {
	case err != nil:
		return nil, nil, errors.Wrap(err, "transport.EarlyListener")
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
			return errors.Wrap(listener.Close(), "listener.Close()")
		},
		nil
}

func serveListener(
	ctx context.Context,
	listener *quic.EarlyListener,
	f StreamHandler,
) error {
	var connUID uint64

	for {
		switch conn, err := listener.Accept(ctx); {
		case err != nil:
			return errors.Wrap(err, "listener.Accept")
		default:
			cuid := connUID
			connUID++

			go func() {
				cctx := context.WithValue(ctx, QuicConnectionUIDContextKey, cuid)

				if err := serveConnection(cctx, conn, f); err != nil {
					_ = closeConnection(conn, err)
				}
			}()
		}
	}
}

func serveConnection(
	ctx context.Context,
	conn quic.Connection,
	f StreamHandler,
) error {
	bctx := context.WithValue(ctx, QuicConnectionContextKey, conn)
	bctx = context.WithValue(bctx, RemoteAddrContextKey, conn.RemoteAddr())

	for {
		switch stream, err := conn.AcceptStream(ctx); {
		case err != nil:
			return errors.Wrap(err, "connection.AcceptStream")
		default:
			sctx := context.WithValue(bctx, StreamIDContextKey, stream.StreamID())

			go func() {
				_, err := f(sctx, stream)
				_ = closeStream(stream, err)
			}()
		}
	}
}
