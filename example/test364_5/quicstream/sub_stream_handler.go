package quicstream

import (
	"context"
	"sync"

	"github.com/quic-go/quic-go"
	dutil "github.com/spikeekips/mitum/example/test364_5/util"
	"github.com/spikeekips/mitum/util"
)

type (
	AcceptSubStreamsHandlerOpenSubStreamsFunc = func(context.Context) (
		dutil.InErrorFunc[dutil.InErrorFunc[[]quic.Stream]], error)
	AcceptSubStreamHandler func(
		context.Context,
		quic.Stream,
		AcceptSubStreamsHandlerOpenSubStreamsFunc,
	) (context.Context, error)
	NewAcceptSubStreamsFunc = func(context.Context, quic.Connection, quic.Stream) (
		dutil.InErrorFunc[dutil.InErrorFunc[[]quic.Stream]],
		error,
	)
	OpenSubStreamsHandlerOpenSubStreamsFunc = func(_ context.Context, size uint16) (
		dutil.InErrorFunc[dutil.InErrorFunc[[]quic.Stream]], error)
	NewOpenSubStreamsFunc = func(_ context.Context, _ quic.Connection, _ quic.Stream, size uint16) (
		dutil.InErrorFunc[dutil.InErrorFunc[[]quic.Stream]],
		error,
	)
	OpenSubStreamsHandler func(
		context.Context,
		quic.Stream,
		OpenSubStreamsHandlerOpenSubStreamsFunc,
	) (context.Context, error)
)

func (h AcceptSubStreamHandler) StreamHandler(f NewAcceptSubStreamsFunc) StreamHandler {
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

			return f(ctx, conn, stream)
		})
	}
}

func NewFuncAcceptSubStreams(maxSize uint16) NewAcceptSubStreamsFunc {
	return func(ctx context.Context, conn quic.Connection, stream quic.Stream) (
		dutil.InErrorFunc[dutil.InErrorFunc[[]quic.Stream]],
		error,
	) {
		return AcceptSubStreams(ctx, conn, stream, maxSize)
	}
}

func (h OpenSubStreamsHandler) StreamHandler(f NewOpenSubStreamsFunc) StreamHandler {
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

		return h(ctx, stream, func(ctx context.Context, size uint16) (
			dutil.InErrorFunc[dutil.InErrorFunc[[]quic.Stream]],
			error,
		) {
			if err := connOnce(); err != nil {
				return nil, err
			}

			return f(ctx, conn, stream, size)
		})
	}
}

func NewFuncOpenSubStreams(open openConnSubStreamsFunc) NewOpenSubStreamsFunc {
	return func(ctx context.Context, conn quic.Connection, stream quic.Stream, size uint16) (
		dutil.InErrorFunc[dutil.InErrorFunc[[]quic.Stream]],
		error,
	) {
		return OpenSubStreams(
			ctx,
			open,
			conn,
			stream,
			util.UUID().String(),
			size,
		)
	}
}
