package quicstreamheader

import (
	"context"
	"io"
	"net"
	"time"

	"github.com/pkg/errors"
	"github.com/spikeekips/mitum/network/quicstream"
	"github.com/spikeekips/mitum/util"
	"github.com/spikeekips/mitum/util/encoder"
)

type Handler[T RequestHeader] func(
	context.Context,
	net.Addr,
	*HandlerBroker,
	T,
) (context.Context, error)

type ErrorHandler func(
	context.Context,
	net.Addr,
	*HandlerBroker,
	error,
) (context.Context, error)

func NewHandler[T RequestHeader](
	encs *encoder.Encoders,
	timeoutf func() time.Duration,
	handler Handler[T],
	errhandler ErrorHandler,
) quicstream.Handler {
	if timeoutf == nil {
		timeoutf = func() time.Duration { return 0 } //revive:disable-line:modifies-parameter
	}

	if errhandler == nil {
		errhandler = func( //revive:disable-line:modifies-parameter
			ctx context.Context, _ net.Addr, broker *HandlerBroker, err error,
		) (context.Context, error) {
			return ctx, broker.WriteResponseHeadOK(ctx, false, err)
		}
	}

	return func(ctx context.Context, addr net.Addr, r io.Reader, w io.WriteCloser) (context.Context, error) {
		if timeout := timeoutf(); timeout > 0 {
			var cancel func()

			ctx, cancel = context.WithTimeout(ctx, timeout)
			defer cancel()
		}

		broker := NewHandlerBroker(encs, nil, r, w)
		defer func() {
			_ = broker.Close()
		}()

		switch nctx, err := baseHandler[T](ctx, addr, broker, handler); {
		case err == nil:
			return nctx, nil
		default:
			timeout := timeoutf()

			switch {
			case errors.Is(err, context.Canceled), errors.Is(err, context.DeadlineExceeded):
				if timeout < 1 {
					return nctx, err
				}

				var cancel func()

				nctx, cancel = context.WithTimeout(context.Background(), timeout)
				defer cancel()
			}

			return util.AwareContextValue(nctx, func(ctx context.Context) (context.Context, error) {
				return errhandler(ctx, addr, broker, err)
			})
		}
	}
}

func baseHandler[T RequestHeader](
	ctx context.Context, addr net.Addr, broker *HandlerBroker, handler Handler[T],
) (context.Context, error) {
	return util.AwareContextValue[context.Context](ctx, func(ctx context.Context) (context.Context, error) {
		req, err := broker.ReadRequestHead(ctx)
		if err != nil {
			return ctx, err
		}

		header, ok := req.(T)
		if !ok {
			var t T

			return ctx, errors.Errorf("expected %T, but %T", t, req)
		}

		return handler(ctx, addr, broker, header)
	})
}
