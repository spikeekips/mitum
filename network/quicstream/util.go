package quicstream

import (
	"context"
	"io"

	"github.com/lucas-clemente/quic-go"
	"github.com/pkg/errors"
)

func readStream(ctx context.Context, stream quic.Stream) ([]byte, error) {
	var b []byte

	readdonech := make(chan error, 1)
	go func() {
		var err error

	end:
		for {
			p := make([]byte, 1024)
			n, e := stream.Read(p)

			var eof bool
			switch {
			case e == nil:
			default:
				if eof = errors.Is(e, io.EOF); !eof {
					err = e

					break end
				}
			}

			b = append(b, p[:n]...)

			if eof {
				break end
			}
		}

		readdonech <- err
	}()

	select {
	case <-ctx.Done():
		stream.CancelRead(0)

		return nil, errors.Wrap(ctx.Err(), "failed to read")
	case err := <-readdonech:
		if err != nil {
			return nil, errors.Wrap(err, "failed to read")
		}

		return b, nil
	}
}
