package quicstreamheader

import (
	"context"
	"io"
	"net"

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

func (h Handler[T]) Handler(handler Handler[T]) Handler[T] {
	return func(ctx context.Context, addr net.Addr, broker *HandlerBroker, header T) (context.Context, error) {
		switch nctx, err := h(ctx, addr, broker, header); {
		case err != nil:
			return nctx, err
		default:
			return handler(nctx, addr, broker, header)
		}
	}
}

func NewHandler[T RequestHeader](
	encs *encoder.Encoders,
	handler Handler[T],
	errhandler ErrorHandler,
) quicstream.Handler {
	if errhandler == nil {
		errhandler = func( //revive:disable-line:modifies-parameter
			ctx context.Context, _ net.Addr, broker *HandlerBroker, err error,
		) (context.Context, error) {
			return ctx, broker.WriteResponseHeadOK(ctx, false, err)
		}
	}

	return func(ctx context.Context, addr net.Addr, r io.Reader, w io.WriteCloser) (context.Context, error) {
		broker := NewHandlerBroker(encs, nil, r, w)
		defer func() {
			_ = broker.Close()
		}()

		switch nctx, err := baseHandler[T](ctx, addr, broker, handler); {
		case err == nil:
			return nctx, nil
		case errors.Is(err, context.Canceled), errors.Is(err, context.DeadlineExceeded):
			return nctx, err
		default:
			return errhandler(nctx, addr, broker, err)
		}
	}
}

func baseHandler[T RequestHeader](
	ctx context.Context, addr net.Addr, broker *HandlerBroker, handler Handler[T],
) (context.Context, error) {
	i, err := util.AwareContextValue(ctx, func(ctx context.Context) (context.Context, error) {
		req, err := broker.ReadRequestHead(ctx)
		if err != nil {
			return ctx, err
		}

		header, ok := req.(T)
		if !ok {
			var t T

			return ctx, errors.Errorf("expected %T, but %T", t, req)
		}

		if i, ok := interface{}(header).(util.IsValider); ok {
			if err := i.IsValid(nil); err != nil {
				return ctx, err
			}
		}

		return handler(ctx, addr, broker, header)
	})

	if i == nil {
		i = ctx
	}

	return i, err
}
