package quicstream

import (
	"io"
	"net"

	"github.com/pkg/errors"
	"github.com/spikeekips/mitum/util"
	"github.com/spikeekips/mitum/util/valuehash"
)

type (
	Handler      func(net.Addr, io.Reader, io.Writer) error
	ErrorHandler func(net.Addr, io.Reader, io.Writer, error) error
)

var ErrHandlerNotFound = util.NewMError("handler not found")

type PrefixHandler struct {
	handlers     map[string]Handler
	errorHandler ErrorHandler
	// TODO support rate limit
}

func NewPrefixHandler(errorHandler ErrorHandler) *PrefixHandler {
	nerrorHandler := errorHandler
	if nerrorHandler == nil {
		nerrorHandler = func(_ net.Addr, _ io.Reader, _ io.Writer, err error) error {
			return err
		}
	}

	return &PrefixHandler{
		handlers:     map[string]Handler{},
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

func (h *PrefixHandler) Add(prefix []byte, handler Handler) *PrefixHandler {
	h.handlers[string(prefix)] = handler

	return h
}

func (h *PrefixHandler) loadHandler(r io.Reader) (Handler, error) {
	e := util.StringErrorFunc("load handler")

	prefix, err := readPrefix(r)
	if err != nil {
		return nil, e(err, "")
	}

	handler, found := h.handlers[string(prefix)]
	if !found {
		return nil, e(ErrHandlerNotFound.Errorf("handler not found"), "")
	}

	return handler, nil
}

func HashPrefix(s string) []byte {
	return valuehash.NewSHA256([]byte(s)).Bytes()
}

func readPrefix(r io.Reader) ([]byte, error) {
	p := make([]byte, 32)

	switch _, err := util.EnsureRead(r, p); {
	case err == nil:
	case errors.Is(err, io.EOF):
	default:
		return nil, err
	}

	return p, nil
}

func WritePrefix(w io.Writer, prefix []byte) error {
	_, err := w.Write(prefix)

	return errors.WithStack(err)
}
