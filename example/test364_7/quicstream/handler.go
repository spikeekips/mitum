package quicstream

import (
	"context"
	"crypto/tls"
	"io"
	"net"
	"slices"

	"github.com/pkg/errors"
	"github.com/quic-go/quic-go"
	dutil "github.com/spikeekips/mitum/example/test364_5/util"
	"github.com/spikeekips/mitum/util"
	"github.com/zeebo/blake3"
)

type (
	HandlerPrefix = [32]byte
	StreamHandler func(context.Context, *quic.Stream) (context.Context, error)
)

func (h StreamHandler) Next(next StreamHandler) StreamHandler {
	return func(ctx context.Context, stream *quic.Stream) (context.Context, error) {
		switch nctx, err := h(ctx, stream); {
		case err != nil:
			return nctx, err
		default:
			return next(nctx, stream)
		}
	}
}

func NewPrefixStreamHandlers() (
	func([]byte, StreamHandler) bool,
	func([]byte) bool,
	StreamHandler,
	func(),
) {
	pool := dutil.NewSingleLockedMap[HandlerPrefix, StreamHandler]()

	return func(b []byte, h StreamHandler) bool {
			created, _ := pool.Set(newStreamHandlerPrefix(b), func(_ StreamHandler, found bool) (StreamHandler, error) {
				if found {
					return nil, dutil.ErrLockedSetIgnore
				}

				return h, nil
			})

			return created
		},
		func(b []byte) bool {
			n := pool.RemoveValues(newStreamHandlerPrefix(b))

			return n > 0
		},
		func(ctx context.Context, stream *quic.Stream) (context.Context, error) {
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

func NewPrefixRequest(request RequestFunc, p ...[]byte) RequestFunc {
	hl := len(HandlerPrefix{})
	prefix := make([]byte, hl*len(p))
	for i, b := range slices.All(p) {
		j := newStreamHandlerPrefix(b)

		copy(prefix[i*hl:], j[:])
	}

	return func(ctx context.Context, addr net.Addr, tlsConfig *tls.Config, quicConfig *quic.Config, h StreamHandler) (
		context.Context, error,
	) {
		return request(ctx, addr, tlsConfig, quicConfig,
			func(ctx context.Context, stream *quic.Stream) (context.Context, error) {
				if _, err := stream.Write(prefix); err != nil {
					return ctx, errors.WithStack(err)
				}

				return h(ctx, stream)
			},
		)
	}
}

func newStreamHandlerPrefix(b []byte) HandlerPrefix {
	return blake3.Sum256(b)
}

func readStreamHandlerPrefix(r io.Reader) (HandlerPrefix, error) {
	var p HandlerPrefix

	_, err := io.ReadFull(r, p[:])

	return p, errors.WithStack(err)
}
