package quicstream

import (
	"context"
	"io"
	"net"
	"sync"
	"time"

	"github.com/pkg/errors"
	"github.com/spikeekips/mitum/util"
	"github.com/spikeekips/mitum/util/valuehash"
)

type (
	Handler      func(context.Context, net.Addr, io.Reader, io.WriteCloser) (context.Context, error)
	ErrorHandler func(context.Context, net.Addr, io.Reader, io.WriteCloser, error) (context.Context, error)
)

var ErrHandlerNotFound = util.NewIDError("handler not found")

var ZeroPrefix [32]byte

var PrefixHandlerPrefixContextKey = util.ContextKey("prefix-handler-prefix")

type PrefixHandler struct {
	handlers     map[[32]byte]Handler
	handlerFunc  func(Handler) Handler
	errorHandler ErrorHandler
	handlerslock sync.RWMutex
}

func NewPrefixHandler(errorHandler ErrorHandler) *PrefixHandler {
	if errorHandler == nil {
		errorHandler = func( //revive:disable-line:modifies-parameter
			ctx context.Context, _ net.Addr, _ io.Reader, _ io.WriteCloser, err error,
		) (context.Context, error) {
			return ctx, nil
		}
	}

	return &PrefixHandler{
		handlers:     map[[32]byte]Handler{},
		errorHandler: errorHandler,
		handlerFunc: func(h Handler) Handler {
			return h
		},
	}
}

func (h *PrefixHandler) Handler(
	ctx context.Context, addr net.Addr, r io.Reader, w io.WriteCloser,
) (context.Context, error) {
	prefix, handler, err := h.loadHandler(r)
	if err != nil {
		return h.errorHandler(ctx, addr, r, w, err)
	}

	nctx := context.WithValue(ctx, PrefixHandlerPrefixContextKey, prefix)

	switch i, err := h.handlerFunc(handler)(nctx, addr, r, w); {
	case err != nil:
		return h.errorHandler(i, addr, r, w, err)
	default:
		return i, nil
	}
}

func (h *PrefixHandler) SetHandlerFunc(f func(Handler) Handler) {
	h.handlerFunc = f
}

func (h *PrefixHandler) Add(prefix [32]byte, handler Handler) *PrefixHandler {
	if prefix == ZeroPrefix {
		panic("empty prefix")
	}

	h.handlerslock.Lock()
	defer h.handlerslock.Unlock()

	h.handlers[prefix] = handler

	return h
}

func (h *PrefixHandler) loadHandler(r io.Reader) ([32]byte, Handler, error) {
	e := util.StringError("load handler")

	var prefix [32]byte

	switch i, err := readPrefix(r); {
	case err != nil:
		return prefix, nil, e.Wrap(err)
	default:
		prefix = i
	}

	h.handlerslock.RLock()
	defer h.handlerslock.RUnlock()

	handler, found := h.handlers[prefix]
	if !found {
		return prefix, nil, e.Wrap(ErrHandlerNotFound.Errorf("handler not found"))
	}

	return prefix, handler, nil
}

func HashPrefix(s string) [32]byte {
	return [32]byte(valuehash.NewSHA256([]byte(s)).Bytes())
}

func readPrefix(r io.Reader) (prefix [32]byte, _ error) {
	switch _, err := util.EnsureRead(r, prefix[:]); {
	case err == nil:
	case errors.Is(err, io.EOF):
	default:
		return prefix, err
	}

	if prefix == ZeroPrefix {
		return prefix, errors.Errorf("empty prefix")
	}

	return prefix, nil
}

func WritePrefix(ctx context.Context, w io.Writer, prefix [32]byte) error {
	if prefix == ZeroPrefix {
		return errors.Errorf("empty prefix")
	}

	return util.AwareContext(ctx, func(context.Context) error {
		_, err := w.Write(prefix[:])

		return errors.WithStack(err)
	})
}

func TimeoutHandler(handler Handler, f func() time.Duration) Handler { // FIXME use context
	return func(ctx context.Context, addr net.Addr, r io.Reader, w io.WriteCloser) (context.Context, error) {
		if timeout := f(); timeout > 0 {
			var cancel func()

			ctx, cancel = context.WithTimeout(ctx, timeout)
			defer cancel()
		}

		return util.AwareContextValue[context.Context](ctx, func(ctx context.Context) (context.Context, error) {
			return handler(ctx, addr, r, w)
		})
	}
}
