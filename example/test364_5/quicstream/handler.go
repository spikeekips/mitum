package quicstream

import (
	"context"
	"io"
	"sync"

	"github.com/pkg/errors"
	"github.com/quic-go/quic-go"
	"github.com/spikeekips/mitum/example/test364_5/frame"
	dutil "github.com/spikeekips/mitum/example/test364_5/util"
	"golang.org/x/crypto/sha3"
)

type (
	StreamHandler             func(context.Context, quic.Stream) (context.Context, error)
	handlerOpenSubStreamsFunc = func(context.Context) (dutil.InErrorFunc[dutil.InErrorFunc[[]quic.Stream]], error)
	SubStreamsHandler         func(
		context.Context,
		quic.Stream,
		handlerOpenSubStreamsFunc,
	) (context.Context, error)
	newHandlerOpenSubStreamsFunc = func(context.Context, quic.Connection, quic.Stream) (
		dutil.InErrorFunc[dutil.InErrorFunc[[]quic.Stream]],
		error,
	)
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
	pool := map[string]StreamHandler{}

	return func(b []byte, h StreamHandler) bool {
			k := string(encodeStreamHandlerPrefix(b))
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
				h, found := pool[string(p)]
				if !found {
					stream.CancelRead(streamErrorCodeNoStreamHandler)
					stream.CancelWrite(streamErrorCodeNoStreamHandler)

					return ctx, stream.Close()
				}

				return h(ctx, stream)
			}
		},
		func() {
			clear(pool)
		}
}

func (h SubStreamsHandler) StreamHandler(n newHandlerOpenSubStreamsFunc) StreamHandler {
	return func(ctx context.Context, stream quic.Stream) (context.Context, error) {
		var conn quic.Connection

		connOnce := sync.OnceValue(func() error {
			switch c, err := loadQuicConnectionFromContext(ctx); {
			case err != nil:
				return err
			default:
				conn = c

				return nil
			}
		})

		return h(ctx, stream, func(ctx context.Context) (
			dutil.InErrorFunc[dutil.InErrorFunc[[]quic.Stream]],
			error,
		) {
			if err := connOnce(); err != nil {
				return nil, err
			}

			return n(ctx, conn, stream)
		})
	}
}

func encodeStreamHandlerPrefix(b []byte) []byte {
	s := sha3.Sum224(b)

	return s[:]
}

func readStreamHandlerPrefix(r io.Reader) ([]byte, error) {
	switch b, err := frame.ReadFixed(r); {
	case err != nil:
		return nil, err
	case len(b) != 28:
		return nil, errors.Errorf("invalid handler prefix length")
	default:
		return b, nil
	}
}

func writeStreamHandlerPrefix(w io.Writer, b []byte) error {
	return frame.WriteFixed(w, encodeStreamHandlerPrefix(b))
}

func newFuncAcceptSubStreams(maxSize uint16) newHandlerOpenSubStreamsFunc {
	return func(ctx context.Context, conn quic.Connection, stream quic.Stream) (
		dutil.InErrorFunc[dutil.InErrorFunc[[]quic.Stream]],
		error,
	) {
		return AcceptSubStreams(ctx, conn, stream, maxSize)
	}
}
