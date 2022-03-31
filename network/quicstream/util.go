package quicstream

import (
	"bytes"
	"context"
	"io"

	"github.com/lucas-clemente/quic-go"
	"github.com/pkg/errors"
)

type StreamResponse struct {
	quic.Stream
}

func (r StreamResponse) Close() error {
	r.Stream.CancelRead(0)

	return nil
}

func ReadAll(ctx context.Context, r io.ReadCloser) ([]byte, error) {
	defer func() {
		_ = r.Close()
	}()

	var b bytes.Buffer

	readdonech := make(chan error, 1)
	go func() {
		var err error

	end:
		for {
			p := make([]byte, 1024)
			n, e := r.Read(p)

			var eof bool
			switch {
			case e == nil:
			default:
				if eof = errors.Is(e, io.EOF); !eof {
					err = e

					break end
				}
			}

			_, _ = b.Write(p[:n])

			if eof {
				break end
			}
		}

		readdonech <- err
	}()

	select {
	case <-ctx.Done():
		return nil, errors.Wrap(ctx.Err(), "failed to read")
	case err := <-readdonech:
		if err != nil {
			return nil, errors.Wrap(err, "failed to read")
		}

		return b.Bytes(), nil
	}
}
