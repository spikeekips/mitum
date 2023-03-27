package quicstream

import (
	"context"
	"io"
	"net"
	"time"

	"github.com/pkg/errors"
	"github.com/spikeekips/mitum/util/encoder"
)

type RequestHeadDetail struct {
	Encoders *encoder.Encoders
	Encoder  encoder.Encoder
	Header   RequestHeader
}

type HeaderHandler func(
	context.Context,
	net.Addr,
	io.Reader,
	io.Writer,
	RequestHeadDetail,
) error

func NewHeaderHandler(
	encs *encoder.Encoders,
	idleTimeout time.Duration,
	handler HeaderHandler,
) Handler {
	return func(addr net.Addr, r io.Reader, w io.Writer) error {
		ctx := context.Background()

		if idleTimeout > 0 {
			var cancel func()

			ctx, cancel = context.WithTimeout(ctx, idleTimeout)

			defer cancel()
		}

		var detail RequestHeadDetail

		donech := make(chan error, 1)

		go func() {
			var err error

			detail, err = readRequestHeadDetailInHandler(encs, r)

			donech <- err
		}()

		select {
		case <-ctx.Done():
			return ctx.Err()
		case err := <-donech:
			if err != nil {
				return err
			}

			return handler(ctx, addr, r, w, detail)
		}
	}
}

func readRequestHeadDetailInHandler(encs *encoder.Encoders, r io.Reader) (RequestHeadDetail, error) {
	detail := RequestHeadDetail{Encoders: encs}

	switch enc, h, err := HeaderReadHead(r, encs); {
	case err != nil:
		return detail, errors.WithMessage(err, "read request")
	default:
		if enc == nil {
			return detail, errors.Errorf("empty request encoder")
		}

		if err := h.IsValid(nil); err != nil {
			return detail, errors.WithMessage(err, "invalid request header")
		}

		switch i, ok := h.(RequestHeader); {
		case !ok:
			return detail, errors.Errorf("expected request header, but %T", h)
		default:
			detail.Header = i
		}

		detail.Encoder = enc

		return detail, nil
	}
}
