package quicstream

import (
	"context"
	"io"

	"github.com/spikeekips/mitum/util"
)

var ConnectionContextKey = util.ContextKey("connection")

type (
	Handler func(context.Context, io.Reader, io.WriteCloser) (context.Context, error)
)

func (h Handler) Next(i Handler) Handler {
	return func(ctx context.Context, r io.Reader, w io.WriteCloser) (context.Context, error) {
		switch nctx, err := h(ctx, r, w); {
		case err != nil:
			return nctx, err
		default:
			return i(nctx, r, w)
		}
	}
}

func DummyHandler(ctx context.Context, _ io.Reader, _ io.WriteCloser) (context.Context, error) {
	return ctx, nil
}

// FIXME streams bonding
