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
	timeout time.Duration,
	handler Handler[T],
	errhandler ErrorHandler,
) quicstream.Handler {
	return func(addr net.Addr, r io.Reader, w io.Writer) error {
		ctx := context.Background()

		if timeout > 0 {
			var cancel func()

			ctx, cancel = context.WithTimeout(ctx, timeout)
			defer cancel()
		}

		broker := NewHandlerBroker(encs, nil, r, w)
		defer func() {
			_ = broker.Close()
		}()

		if err := baseHandler[T](ctx, addr, broker, handler); err != nil {
			aerr := util.AwareContext(ctx, func(ctx context.Context) error {
				if errhandler != nil {
					if berr := errhandler(ctx, addr, broker, err); berr != nil {
						return berr
					}
				}

				return broker.WriteResponseHeadOK(ctx, false, err)
			})

			return util.JoinErrors(err, aerr)
		}

		return nil
	}
}

func baseHandler[T RequestHeader](ctx context.Context, addr net.Addr, broker *HandlerBroker, handler Handler[T]) error {
	req, err := broker.ReadRequestHead(ctx)
	if err != nil {
		return err
	}

	header, ok := req.(T)
	if !ok {
		var t T

		return errors.Errorf("expected %T, but %T", t, req)
	}

	return util.AwareContext(ctx, func(ctx context.Context) error {
		return handler(ctx, addr, broker, header)
	})
}
