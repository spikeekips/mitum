package quicstream

import (
	"context"
	"crypto/tls"
	"net"

	"github.com/lucas-clemente/quic-go"
	"github.com/pkg/errors"
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
	bind *net.UDPAddr, // FIXME use netip.AddrPort
	tlsconfig *tls.Config,
	quicconfig *quic.Config,
	handler Handler,
) *Server {
	srv := &Server{
		Logging: logging.NewLogging(func(zctx zerolog.Context) zerolog.Context {
			return zctx.Str("module", "quicstream-server")
		}),
		bind:       bind,
		tlsconfig:  tlsconfig,
		quicconfig: quicconfig,
		handler:    handler,
	}

	srv.ContextDaemon = util.NewContextDaemon("quicstream-server", srv.start)

	return srv
}

func (srv *Server) start(ctx context.Context) error {
	srv.Log().Debug().
		Str("bind", srv.bind.String()).
		Msg("trying to start")
	defer srv.Log().Debug().Msg("stopped")

	listener, err := quic.ListenAddrEarly(srv.bind.String(), srv.tlsconfig, srv.quicconfig)
	if err != nil {
		srv.Log().Error().Err(err).Msg("failed to listen")

		return errors.Wrap(err, "failed to listen")
	}

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
				srv.Log().Error().Err(err).Msg("failed to accept")
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

				srv.Log().Debug().Err(err).Msg("failed to accept stream; timeout")
			default:
				errcode = 0x403

				srv.Log().Error().Err(err).Msg("failed to accept stream")
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

		srv.Log().Error().Err(err).Msg("failed to handle stream")
	}
}

func (srv *Server) SetLogging(l *logging.Logging) *logging.Logging {
	_ = srv.ContextDaemon.SetLogging(l)

	return srv.Logging.SetLogging(l)
}
