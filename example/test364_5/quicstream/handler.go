package quicstream

import (
	"context"
	"io"

	"github.com/pkg/errors"
	"github.com/quic-go/quic-go"
	"github.com/spikeekips/mitum/example/test364_5/frame"
	"github.com/spikeekips/mitum/util"
	"golang.org/x/crypto/sha3"
)

type (
	HandlerPrefix = [28]byte
	StreamHandler func(context.Context, quic.Stream) (context.Context, error)
)

func (h StreamHandler) Next(i StreamHandler) StreamHandler {
	return func(ctx context.Context, stream quic.Stream) (context.Context, error) {
		switch nctx, err := h(ctx, stream); {
		case err != nil:
			return nctx, err
		default:
			return i(nctx, stream)
		}
	}
}

func NilStreamHandler(ctx context.Context, _ quic.Stream) (context.Context, error) {
	return ctx, nil
}

func NewFuncPrefixStreamHandler() (
	func([]byte, StreamHandler) bool,
	StreamHandler,
	func(),
) {
	pool := map[HandlerPrefix]StreamHandler{}

	return func(b []byte, h StreamHandler) bool {
			k := EncodeStreamHandlerPrefix(b)
			if _, found := pool[k]; found {
				return false
			}

			pool[k] = h

			return true
		},
		func(ctx context.Context, stream quic.Stream) (context.Context, error) {
			switch p, err := readStreamHandlerPrefix(stream); {
			case err != nil:
				return ctx, err
			default:
				h, found := pool[p]
				if !found {
					return ctx, util.ErrNotFound.Errorf("unknown handler prefix, %v", p)
				}

				return h(ctx, stream)
			}
		},
		func() {
			clear(pool)
		}
}

func EncodeStreamHandlerPrefix(b []byte) HandlerPrefix {
	return sha3.Sum224(b)
}

func readStreamHandlerPrefix(r io.Reader) (v HandlerPrefix, _ error) {
	switch b, err := frame.ReadFixed(r); {
	case err != nil:
		return v, err
	case len(b) != 28:
		return v, errors.Errorf("invalid handler prefix length")
	default:
		copy(v[:], b)

		return v, nil
	}
}

func writeStreamHandlerPrefix(w io.Writer, v HandlerPrefix) error {
	return frame.WriteFixed(w, v[:])
}
