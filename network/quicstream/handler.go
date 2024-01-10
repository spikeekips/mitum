package quicstream

import (
	"context"
	"io"
	"net"
	"sync"
	"time"

	"github.com/pkg/errors"
	"github.com/rs/zerolog"
	"github.com/spikeekips/mitum/util"
	"github.com/spikeekips/mitum/util/logging"
	"github.com/spikeekips/mitum/util/valuehash"
)

type (
	Handler      func(context.Context, net.Addr, io.Reader, io.WriteCloser) (context.Context, error)
	ErrorHandler func(context.Context, net.Addr, io.Reader, io.WriteCloser, error) (context.Context, error)
)

var ErrHandlerNotFound = util.NewIDError("handler not found")

type (
	HandlerName   string
	HandlerPrefix [32]byte
)

var ZeroPrefix HandlerPrefix

func (h HandlerName) String() string {
	return string(h)
}

func (h HandlerName) Prefix() HandlerPrefix {
	return HashPrefix(h)
}

func (p HandlerPrefix) String() string {
	return util.EncodeHash(p[:])
}

var PrefixHandlerPrefixContextKey = util.ContextKey("prefix-handler-prefix")

type PrefixHandler struct {
	*logging.Logging
	handlers     map[HandlerPrefix]Handler
	handlerNames map[HandlerPrefix]HandlerName
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
		Logging: logging.NewLogging(func(zctx zerolog.Context) zerolog.Context {
			return zctx.Str("module", "quicstream-prefix-handler")
		}),
		handlers:     map[HandlerPrefix]Handler{},
		handlerNames: map[HandlerPrefix]HandlerName{},
		errorHandler: errorHandler,
		handlerFunc: func(h Handler) Handler {
			return h
		},
	}
}

func (h *PrefixHandler) Handler(
	ctx context.Context, addr net.Addr, r io.Reader, w io.WriteCloser,
) (context.Context, error) {
	name, prefix, handler, err := h.loadHandler(ctx, r)
	if err != nil {
		return h.errorHandler(ctx, addr, r, w, err)
	}

	l := ConnectionLoggerFromContext(ctx, h.Log()).With().
		Stringer("handler", name).
		Stringer("prefix", prefix).
		Logger()

	l.Trace().Msg("handler requested")
	defer logging.TimeElapsed()(l.Trace(), "handler done")

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

func (h *PrefixHandler) Add(name HandlerName, handler Handler) *PrefixHandler {
	if len(name) < 1 {
		panic("empty name")
	}

	prefix := name.Prefix()

	h.handlerslock.Lock()
	defer h.handlerslock.Unlock()

	if _, found := h.handlers[prefix]; found {
		h.Log().Warn().Stringer("name", name).Msg("handler already found; overwrite")
	}

	h.handlers[prefix] = handler
	h.handlerNames[prefix] = name

	return h
}

func (h *PrefixHandler) loadHandler(ctx context.Context, r io.Reader) (HandlerName, HandlerPrefix, Handler, error) {
	e := util.StringError("load handler")

	var prefix HandlerPrefix

	switch i, err := readPrefix(ctx, r); {
	case err != nil:
		return "", prefix, nil, e.Wrap(err)
	default:
		prefix = i
	}

	h.handlerslock.RLock()
	defer h.handlerslock.RUnlock()

	handler, found := h.handlers[prefix]
	if !found {
		return "", prefix, nil, e.Wrap(ErrHandlerNotFound.Errorf("handler not found"))
	}

	return h.handlerNames[prefix], prefix, handler, nil
}

func HashPrefix(s HandlerName) HandlerPrefix {
	return HandlerPrefix(valuehash.NewSHA256([]byte(s)).Bytes())
}

func readPrefix(ctx context.Context, r io.Reader) (prefix HandlerPrefix, _ error) {
	switch _, err := util.EnsureRead(ctx, r, prefix[:]); {
	case prefix == ZeroPrefix:
		return prefix, errors.Errorf("empty prefix")
	default:
		if errors.Is(err, io.EOF) {
			err = nil
		}

		return prefix, err
	}
}

func WritePrefix(ctx context.Context, w io.Writer, prefix HandlerPrefix) error {
	if prefix == ZeroPrefix {
		return errors.Errorf("empty prefix")
	}

	return util.AwareContext(ctx, func(context.Context) error {
		_, err := w.Write(prefix[:])

		return errors.WithStack(err)
	})
}

func TimeoutHandler(handler Handler, f func() time.Duration) Handler {
	return func(ctx context.Context, addr net.Addr, r io.Reader, w io.WriteCloser) (context.Context, error) {
		if timeout := f(); timeout > 0 {
			var cancel func()

			ctx, cancel = context.WithTimeout(ctx, timeout)
			defer cancel()
		}

		i, err := util.AwareContextValue(ctx, func(ctx context.Context) (context.Context, error) {
			return handler(ctx, addr, r, w)
		})

		if i == nil {
			i = ctx
		}

		return i, err
	}
}
