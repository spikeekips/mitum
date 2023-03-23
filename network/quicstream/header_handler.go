package quicstream

import (
	"context"
	"io"
	"net"
	"time"

	"github.com/pkg/errors"
	"github.com/spikeekips/mitum/util/encoder"
)

type HeaderHandlerDetail struct {
	Encoders *encoder.Encoders
	Encoder  encoder.Encoder
	Header   RequestHeader
	Body     io.Reader
}

type HeaderHandler func(
	net.Addr,
	io.Reader,
	io.Writer,
	HeaderHandlerDetail,
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

		var detail HeaderHandlerDetail

		donech := make(chan error, 1)

		go func() {
			var err error

			detail, err = readRequestInHandler(encs, r)

			donech <- err
		}()

		select {
		case <-ctx.Done():
			return ctx.Err()
		case err := <-donech:
			if err != nil {
				return err
			}

			return handler(addr, r, w, detail)
		}
	}
}

func readRequestInHandler(
	encs *encoder.Encoders,
	r io.Reader,
) (HeaderHandlerDetail, error) {
	detail := HeaderHandlerDetail{Encoders: encs}

	switch enc, h, dataFormat, bodyLength, err := ReadHead(r, encs); {
	case err != nil:
		return detail, errors.WithMessage(err, "read request")
	default:
		if enc == nil {
			return detail, errors.Errorf("empty request encoder")
		}

		if err := dataFormat.IsValid(nil); err != nil {
			return detail, errors.WithMessage(err, "invalid request DataFormat")
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

		switch dataFormat {
		case StreamDataFormat:
			detail.Body = r
		default:
			detail.Body = io.LimitReader(r, int64(bodyLength))
		}

		return detail, nil
	}
}
