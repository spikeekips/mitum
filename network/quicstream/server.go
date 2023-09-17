package quicstream

import (
	"context"
	"crypto/tls"
	"net"
	"sync/atomic"
	"time"

	"github.com/pkg/errors"
	"github.com/quic-go/quic-go"
	"github.com/rs/zerolog"
	"github.com/spikeekips/mitum/util"
	"github.com/spikeekips/mitum/util/logging"
)

type Server struct {
	*logging.Logging
	*util.ContextDaemon
	handler              Handler
	streamTimeoutContext func(context.Context) (context.Context, func())
}

func NewServer(
	bind *net.UDPAddr,
	tlsconfig *tls.Config,
	quicconfig *quic.Config,
	handler Handler,
	maxStreamTimeout func() time.Duration,
) (*Server, error) {
	if maxStreamTimeout() < 1 {
		return nil, errors.Errorf("maxStreamTimeout should be over zero")
	}

	listener, err := quic.ListenAddrEarly(bind.String(), tlsconfig, quicconfig)
	if err != nil {
		return nil, errors.Wrap(err, "listen")
	}

	srv := &Server{
		Logging: logging.NewLogging(func(zctx zerolog.Context) zerolog.Context {
			return zctx.Str("module", "quicstream-server")
		}),
		handler: handler,
	}

	srv.streamTimeoutContext = func(ctx context.Context) (context.Context, func()) {
		return context.WithTimeout(ctx, maxStreamTimeout())
	}

	srv.ContextDaemon = util.NewContextDaemon(func(ctx context.Context) error {
		return srv.start(ctx, listener)
	})

	return srv, nil
}

func (srv *Server) start(ctx context.Context, listener *quic.EarlyListener) error {
	go srv.accept(ctx, listener)

	<-ctx.Done()

	if err := listener.Close(); err != nil {
		return errors.Wrap(err, "close listener")
	}

	return nil
}

func (srv *Server) accept(ctx context.Context, listener *quic.EarlyListener) {
	var count int64

	go func() {
		var lastcount int64 = -1

		ticker := time.NewTicker(time.Second * 3)
		defer ticker.Stop()

		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				current := atomic.LoadInt64(&count)
				if current != lastcount {
					srv.Log().Trace().Int64("count", current).Msg("open connections")

					lastcount = current
				}
			}
		}
	}()

	for {
		conn, err := listener.Accept(ctx)
		if err != nil {
			switch {
			case errors.Is(err, context.Canceled):
			case err.Error() == "server closed":
			default:
				srv.Log().Trace().Err(err).Msg("failed to accept connection")
			}

			return
		}

		// FIXME set connection id(UUID) to context

		atomic.AddInt64(&count, 1)

		go func() {
			select {
			case <-ctx.Done():
			case <-conn.Context().Done():
			}

			atomic.AddInt64(&count, -1)
		}()

		go srv.handleConnection(ctx, conn)
	}
}

func (srv *Server) handleConnection(ctx context.Context, conn quic.EarlyConnection) {
	var count int64

	go func() {
		var lastcount int64 = -1

		ticker := time.NewTicker(time.Second * 3)
		defer ticker.Stop()

		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				current := atomic.LoadInt64(&count)
				if current != lastcount {
					srv.Log().Trace().
						Int64("count", atomic.LoadInt64(&current)).
						Stringer("remote", conn.RemoteAddr()).
						Msg("open streams")

					lastcount = current
				}
			}
		}
	}()

	for {
		// FIXME set StreamID() to context
		stream, err := conn.AcceptStream(ctx)
		if err != nil {
			var nerr net.Error
			var aerr *quic.ApplicationError
			var errcode quic.ApplicationErrorCode

			switch {
			case errors.Is(err, context.Canceled):
				errcode = 0x401

				srv.Log().Trace().Err(err).Msg("failed to accept stream; canceled")
			case errors.As(err, &aerr):
				errcode = aerr.ErrorCode
				if errcode != quic.ApplicationErrorCode(0) {
					srv.Log().Trace().Err(err).Msg("failed to accept stream; application error")
				}
			case errors.As(err, &nerr) && nerr.Timeout():
				errcode = 0x402

				srv.Log().Trace().Err(err).Msg("failed to accept stream; timeout")
			default:
				errcode = 0x403

				srv.Log().Trace().Err(err).Msg("failed to accept stream")
			}

			_ = conn.CloseWithError(errcode, err.Error())

			return
		}

		atomic.AddInt64(&count, 1)

		go func() {
			select {
			case <-ctx.Done():
			case <-stream.Context().Done():
			}

			atomic.AddInt64(&count, -1)
		}()

		go srv.handleStream(ctx, conn.RemoteAddr(), stream)
	}
}

func (srv *Server) handleStream(ctx context.Context, remoteAddr net.Addr, stream quic.Stream) {
	sctx, cancel := srv.streamTimeoutContext(ctx)
	defer cancel()

	var errcode quic.StreamErrorCode

	if err := util.AwareContext(sctx, func(context.Context) error {
		_, err := srv.handler(sctx, remoteAddr, stream, stream)

		return err
	}); err != nil {
		if errors.Is(err, context.Canceled) {
			errcode = 0x401
		}

		stream.CancelWrite(errcode)

		srv.Log().Trace().Err(err).Msg("failed to handle stream")
	}

	stream.CancelRead(errcode)
	_ = stream.Close()
}
