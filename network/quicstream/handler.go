package quicstream

import (
	"io"
	"net"

	"github.com/pkg/errors"
	"github.com/spikeekips/mitum/util"
	"github.com/spikeekips/mitum/util/valuehash"
)

const handlerPrefixSize = 32

type Handler func(net.Addr, io.ReadCloser, io.WriteCloser) error

type PrefixHandler struct {
	handlers     map[string]Handler
	errorHandler Handler
}

func NewPrefixHandler(errorHandler Handler) *PrefixHandler {
	nerrorHandler := errorHandler
	if nerrorHandler == nil {
		nerrorHandler = func(net.Addr, io.ReadCloser, io.WriteCloser) error {
			return errors.Errorf("handler not found")
		}
	}

	return &PrefixHandler{
		handlers:     map[string]Handler{},
		errorHandler: nerrorHandler,
	}
}

func (h *PrefixHandler) Handler(addr net.Addr, r io.ReadCloser, w io.WriteCloser) error {
	handler, err := h.loadHandler(r)
	if err != nil {
		return h.doErrorHandler(addr, r, w)
	}

	return handler(addr, r, w)
}

func (h *PrefixHandler) Add(prefix string, handler Handler) *PrefixHandler {
	h.handlers[string(valuehash.NewSHA256([]byte(prefix)).Bytes())] = handler

	return h
}

func (h *PrefixHandler) doErrorHandler(addr net.Addr, r io.ReadCloser, w io.WriteCloser) error {
	return h.errorHandler(addr, r, w)
}

func (h *PrefixHandler) loadHandler(r io.ReadCloser) (Handler, error) {
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
