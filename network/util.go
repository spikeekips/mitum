package network

import (
	"context"
	"io"

	"github.com/pkg/errors"
)

func EnsureRead(ctx context.Context, r io.Reader, b []byte) (int, error) {
	if len(b) < 1 {
		return 0, nil
	}

	var n int

	for {
		select {
		case <-ctx.Done():
			return n, ctx.Err()
		default:
			l := make([]byte, len(b)-n)

			i, err := r.Read(l)

			switch {
			case err == nil:
			case !errors.Is(err, io.EOF):
				return n, errors.Wrap(err, "")
			}

			n += i

			copy(b[len(b)-len(l):], l)

			if n == len(b) || errors.Is(err, io.EOF) {
				return n, errors.Wrap(err, "")
			}
		}
	}
}
