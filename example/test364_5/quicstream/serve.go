package quicstream

import (
	"context"
	"crypto/tls"
	"net"
	"slices"

	"github.com/pkg/errors"
	"github.com/quic-go/quic-go"
	dutil "github.com/spikeekips/mitum/example/test364_5/util"
	"github.com/spikeekips/mitum/util"
)

const (
	connErrorCodeOK quic.ApplicationErrorCode = iota
	connErrorCodeUnknown
)

const (
	streamErrorCodeOK quic.StreamErrorCode = iota
	streamErrorCodeNoStreamHandler
	streamErrorCodeUnknown
)

var (
	QuicConnectionContextKey    util.ContextKey = "quic_connection"
	QuicConnectionUIDContextKey util.ContextKey = "quic_connection_uid"
	RemoteAddrContextKey        util.ContextKey = "remote_addr"
	StreamIDContextKey          util.ContextKey = "stream_id"
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

			return nil
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
	for {
		switch conn, err := listener.Accept(ctx); {
		case err != nil:
			return errors.Wrap(err, "listener.Accept")
		default:
			go func() {
				cctx := context.WithValue(ctx, QuicConnectionUIDContextKey, util.ULID().String())

				if err := serveConnection(cctx, conn, f); err != nil {
					_ = conn.CloseWithError(connErrorCodeUnknown, "")
				}
			}()
		}
	}

	return nil
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
				_ = closeStreamWithError(stream, err)
			}()
		}
	}
}

func newFuncCloseStreamWithError(err error) func(quic.Stream) error {
	switch {
	case err != nil:
		return func(stream quic.Stream) error {
			stream.CancelRead(streamErrorCodeUnknown)
			stream.CancelWrite(streamErrorCodeUnknown)

			return err
		}
	default:
		return func(stream quic.Stream) error {
			stream.CancelRead(streamErrorCodeOK)
			_ = stream.Close()

			return err
		}
	}
}

func closeStreamWithError(stream quic.Stream, err error) error {
	return newFuncCloseStreamWithError(err)(stream)
}

func closeStreamsWithError(streams []quic.Stream, err error) error {
	f := newFuncCloseStreamWithError(err)

	slices.Values(streams)(func(stream quic.Stream) bool {
		_ = f(stream)

		return true
	})

	return err
}
