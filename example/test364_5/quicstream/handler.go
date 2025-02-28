package quicstream

import (
	"context"
	"io"

	"github.com/pkg/errors"
	"github.com/quic-go/quic-go"
	dutil "github.com/spikeekips/mitum/example/test364_5/util"
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
	func([]byte) bool,
	StreamHandler,
	func(),
) {
	pool := dutil.NewSingleLockedMap[HandlerPrefix, StreamHandler]()

	return func(b []byte, h StreamHandler) bool {
			created, _ := pool.Set(NewStreamHandlerPrefix(b), func(_ StreamHandler, found bool) (StreamHandler, error) {
				if found {
					return nil, dutil.ErrLockedSetIgnore
				}

				return h, nil
			})

			return created
		},
		func(b []byte) bool {
			n := pool.RemoveValues(NewStreamHandlerPrefix(b))

			return n > 0
		},
		func(ctx context.Context, stream quic.Stream) (context.Context, error) {
			switch p, err := readStreamHandlerPrefix(stream); {
			case err != nil:
				return ctx, err
			default:
				h, found := pool.Value(p)
				if !found {
					return ctx, util.ErrNotFound.Errorf("unknown handler prefix, %v", p)
				}

				return h(ctx, stream)
			}
		},
		pool.Close
}

func NewStreamHandlerPrefix(b []byte) HandlerPrefix {
	return sha3.Sum224(b)
}

func encodeStreamHandlerPrefix(b []byte) []byte {
	p := make([]byte, 28)
	h := sha3.New224()
	defer h.Reset()

	h.Write(b)
	h.Sum(p)

	return p
}

func readStreamHandlerPrefix(r io.Reader) (v HandlerPrefix, _ error) {
	var p HandlerPrefix

	switch n, err := r.Read(p[:]); {
	case err != nil && errors.Is(err, io.EOF):
		return v, err
	case n != len(p):
		return v, errors.Errorf("invalid handler prefix length")
	default:
		return p, err
	}
}

func WriteStreamHandlerPrefix(w io.Writer, b []byte) error {
	_, err := w.Write(encodeStreamHandlerPrefix(b))

	return err
}
