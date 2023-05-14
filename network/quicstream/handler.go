package quicstream

import (
	"context"
	"io"
	"net"
	"sync"

	"github.com/pkg/errors"
	"github.com/spikeekips/mitum/util"
	"github.com/spikeekips/mitum/util/valuehash"
)

type (
	Handler      func(net.Addr, io.Reader, io.Writer) error
	ErrorHandler func(net.Addr, io.Reader, io.Writer, error) error
)

var ErrHandlerNotFound = util.NewIDError("handler not found")

var ZeroPrefix [32]byte

type PrefixHandler struct {
	handlers     map[[32]byte]Handler
	errorHandler ErrorHandler
	handlerslock sync.RWMutex
	// TODO support rate limit
}

func NewPrefixHandler(errorHandler ErrorHandler) *PrefixHandler {
	nerrorHandler := func(_ net.Addr, _ io.Reader, _ io.Writer, err error) error {
		return err
	}

	if errorHandler != nil {
		nerrorHandler = errorHandler
	}

	return &PrefixHandler{
		handlers:     map[[32]byte]Handler{},
		errorHandler: nerrorHandler,
	}
}

func (h *PrefixHandler) Handler(addr net.Addr, r io.Reader, w io.Writer) error {
	handler, err := h.loadHandler(r)
	if err != nil {
		return h.errorHandler(addr, r, w, err)
	}

	if err := handler(addr, r, w); err != nil {
		return h.errorHandler(addr, r, w, err)
	}

	return nil
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

func (h *PrefixHandler) loadHandler(r io.Reader) (Handler, error) {
	e := util.StringError("load handler")

	var prefix [32]byte

	switch i, err := readPrefix(r); {
	case err != nil:
		return nil, e.Wrap(err)
	default:
		prefix = i
	}

	h.handlerslock.RLock()
	defer h.handlerslock.RUnlock()

	handler, found := h.handlers[prefix]
	if !found {
		return nil, e.Wrap(ErrHandlerNotFound.Errorf("handler not found"))
	}

	return handler, nil
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
