package quicstream

import (
	"context"
	"crypto/tls"
	"net"

	"github.com/pkg/errors"
	"github.com/quic-go/quic-go"
	"github.com/rs/zerolog"
	"github.com/spikeekips/mitum/util"
	"github.com/spikeekips/mitum/util/logging"
)

type Server struct {
	*logging.Logging
	*util.ContextDaemon
	bind       *net.UDPAddr
	tlsconfig  *tls.Config
	quicconfig *quic.Config
	handler    Handler
}

func NewServer(
	bind *net.UDPAddr,
	tlsconfig *tls.Config,
	quicconfig *quic.Config,
	handler Handler,
) (*Server, error) {
	listener, err := quic.ListenAddrEarly(bind.String(), tlsconfig, quicconfig)
	if err != nil {
		return nil, errors.Wrap(err, "failed to listen")
	}

	srv := &Server{
		Logging: logging.NewLogging(func(zctx zerolog.Context) zerolog.Context {
			return zctx.Str("module", "quicstream-server")
		}),
		bind:       bind,
		tlsconfig:  tlsconfig,
		quicconfig: quicconfig,
		handler:    handler,
	}

	srv.ContextDaemon = util.NewContextDaemon(func(ctx context.Context) error {
		return srv.start(ctx, listener)
	})

	return srv, nil
}

func (srv *Server) start(ctx context.Context, listener quic.EarlyListener) error {
	go srv.accept(ctx, listener)

	<-ctx.Done()

	if err := listener.Close(); err != nil {
		return errors.Wrap(err, "failed to close listener")
	}

	return nil
}

func (srv *Server) accept(ctx context.Context, listener quic.EarlyListener) {
	for {
		session, err := listener.Accept(ctx)
		if err != nil {
			switch {
			case errors.Is(err, context.Canceled):
			case err.Error() == "server closed":
			default:
				srv.Log().Trace().Err(err).Msg("failed to accept")
			}

			return
		}

		go srv.handleSession(ctx, session)
	}
}

func (srv *Server) handleSession(ctx context.Context, session quic.EarlyConnection) {
	for {
		stream, err := session.AcceptStream(ctx)
		if err != nil {
			var nerr net.Error
			var errcode quic.ApplicationErrorCode

			switch {
			case errors.Is(err, context.Canceled):
				errcode = 0x401
			case errors.As(err, &nerr) && nerr.Timeout():
				errcode = 0x402

				srv.Log().Trace().Err(err).Msg("failed to accept stream; timeout")
			default:
				errcode = 0x403

				srv.Log().Trace().Err(err).Msg("failed to accept stream")
			}

			_ = session.CloseWithError(errcode, err.Error())

			return
		}

		go srv.handleStream(ctx, session.RemoteAddr(), stream)
	}
}

func (srv *Server) handleStream(ctx context.Context, remoteAddr net.Addr, stream quic.Stream) {
	defer func() {
		_ = stream.Close()
	}()

	if err := util.AwareContext(ctx, func() error {
		return srv.handler(remoteAddr, stream, stream)
	}); err != nil {
		var errcode quic.StreamErrorCode
		if errors.Is(err, context.Canceled) {
			errcode = 0x401
		}

		stream.CancelRead(errcode)
		stream.CancelWrite(errcode)

		srv.Log().Trace().Err(err).Msg("failed to handle stream")
	}
}
