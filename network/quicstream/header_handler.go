package quicstream

import (
	"context"
	"io"
	"net"
	"time"

	"github.com/pkg/errors"
	"github.com/spikeekips/mitum/util"
	"github.com/spikeekips/mitum/util/encoder"
)

type HeaderHandler func(net.Addr, io.Reader, io.Writer, Header, *encoder.Encoders, encoder.Encoder) error

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

		var header Header
		var enc encoder.Encoder

		donech := make(chan error, 1)

		go func() {
			var err error

			header, enc, err = readHeader(ctx, encs, r)

			donech <- err
		}()

		select {
		case <-ctx.Done():
			return ctx.Err()
		case err := <-donech:
			if err != nil {
				return err
			}

			if header != nil {
				if err := header.IsValid(nil); err != nil {
					return err
				}
			}

			return handler(addr, r, w, header, encs, enc)
		}
	}
}

func writeResponse(
	w io.Writer,
	enc encoder.Encoder,
	header ResponseHeader,
	writebodyf func(w io.Writer) error,
) error {
	e := util.StringErrorFunc("write response")

	var headerb []byte

	if header != nil {
		i, err := enc.Marshal(header)
		if err != nil {
			return e(err, "")
		}

		headerb = i
	}

	return writeTo(w, enc.Hint(), headerb, writebodyf)
}

func WriteResponseBytes(w io.Writer, header ResponseHeader, enc encoder.Encoder, body []byte) error {
	return writeResponse(w, enc, header, func(w io.Writer) error {
		if len(body) < 1 {
			return nil
		}

		_, err := w.Write(body)

		return errors.WithStack(err)
	})
}

func WriteResponseBody(w io.Writer, header ResponseHeader, enc encoder.Encoder, body io.Reader) error {
	return writeResponse(w, enc, header, func(w io.Writer) error {
		if body == nil {
			return nil
		}

		_, err := io.Copy(w, body)

		return errors.WithStack(err)
	})
}

func WriteResponseEncode(w io.Writer, header ResponseHeader, enc encoder.Encoder, body interface{}) error {
	return writeResponse(w, enc, header, func(w io.Writer) error {
		if body == nil {
			return nil
		}

		return enc.StreamEncoder(w).Encode(body)
	})
}
