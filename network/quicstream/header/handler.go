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
) error

type ErrorHandler func(
	context.Context,
	net.Addr,
	*HandlerBroker,
	error,
) error

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
		) error {
			return broker.WriteResponseHeadOK(ctx, false, err)
		}
	}

	return func(ctx context.Context, addr net.Addr, r io.Reader, w io.WriteCloser) error {
		if timeout := timeoutf(); timeout > 0 {
			var cancel func()

			ctx, cancel = context.WithTimeout(ctx, timeout)
			defer cancel()
		}

		broker := NewHandlerBroker(encs, nil, r, w)
		defer func() {
			_ = broker.Close()
		}()

		if err := baseHandler[T](ctx, addr, broker, handler); err != nil {
			nctx := ctx
			timeout := timeoutf()

			switch {
			case errors.Is(err, context.Canceled), errors.Is(err, context.DeadlineExceeded):
				if timeout < 1 {
					return err
				}

				var cancel func()

				nctx, cancel = context.WithTimeout(context.Background(), timeout)
				defer cancel()
			}

			return util.AwareContext(nctx, func(ctx context.Context) error {
				return errhandler(ctx, addr, broker, err)
			})
		}

		return nil
	}
}

func baseHandler[T RequestHeader](ctx context.Context, addr net.Addr, broker *HandlerBroker, handler Handler[T]) error {
	return util.AwareContext(ctx, func(ctx context.Context) error {
		req, err := broker.ReadRequestHead(ctx)
		if err != nil {
			return err
		}

		header, ok := req.(T)
		if !ok {
			var t T

			return errors.Errorf("expected %T, but %T", t, req)
		}

		return handler(ctx, addr, broker, header)
	})
}
