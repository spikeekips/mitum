package network

import (
	"context"
	"io"
	"net"

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
			return n, errors.WithStack(ctx.Err())
		default:
			l := make([]byte, len(b)-n)

			i, err := r.Read(l)

			switch {
			case err == nil:
			case !errors.Is(err, io.EOF):
				return n, errors.WithStack(err)
			}

			n += i

			copy(b[len(b)-len(l):], l)

			if n == len(b) || errors.Is(err, io.EOF) {
				return n, errors.WithStack(err)
			}
		}
	}
}

func IsValidAddr(s string) error {
	if len(s) < 1 {
		return errors.Errorf("empty publish")
	}

	switch host, port, err := net.SplitHostPort(s); {
	case err != nil:
		return errors.WithStack(err)
	case len(host) < 1:
		return errors.Errorf("empty host")
	case len(port) < 1:
		return errors.Errorf("empty port")
	}

	return nil
}
