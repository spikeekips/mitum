package quicstream

import (
	"io"
	"net"

	"github.com/pkg/errors"
	"github.com/spikeekips/mitum/util"
	"github.com/spikeekips/mitum/util/valuehash"
)

const handlerPrefixSize = 32

type (
	Handler      func(net.Addr, io.Reader, io.Writer) error
	ErrorHandler func(net.Addr, io.Reader, io.Writer, error) error
)

type PrefixHandler struct { // BLOCK remove; unused
	handlers     map[string]Handler
	errorHandler ErrorHandler
}

func NewPrefixHandler(errorHandler ErrorHandler) *PrefixHandler {
	nerrorHandler := errorHandler
	if nerrorHandler == nil {
		nerrorHandler = func(_ net.Addr, _ io.Reader, _ io.Writer, err error) error {
			return errors.Wrap(err, "")
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
		return h.errorHandler(addr, r, w, errors.Errorf("handler not found"))
	}

	if err := handler(addr, r, w); err != nil {
		return h.errorHandler(addr, r, w, err)
	}

	return nil
}

func (h *PrefixHandler) Add(prefix string, handler Handler) *PrefixHandler {
	h.handlers[string(valuehash.NewSHA256([]byte(prefix)).Bytes())] = handler

	return h
}

func (h *PrefixHandler) loadHandler(r io.Reader) (Handler, error) {
	e := util.StringErrorFunc("failed to load handler")

	p := make([]byte, handlerPrefixSize)

	switch n, err := r.Read(p); {
	case err != nil:
		return nil, e(err, "failed to read handler prefix")
	case n < handlerPrefixSize:
		return nil, e(nil, "too short prefix")
	}

	handler, found := h.handlers[string(p)]
	if !found {
		return nil, errors.Errorf("handler not found")
	}

	return handler, nil
}

func BodyWithPrefix(prefix string, b []byte) []byte {
	n := make([]byte, len(b)+handlerPrefixSize)
	copy(n[:handlerPrefixSize], valuehash.NewSHA256([]byte(prefix)).Bytes())
	copy(n[handlerPrefixSize:], b)

	return n
}
