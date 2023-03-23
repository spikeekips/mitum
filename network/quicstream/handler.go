package quicstream

import (
	"io"
	"net"

	"github.com/spikeekips/mitum/util"
	"github.com/spikeekips/mitum/util/valuehash"
)

type (
	Handler      func(net.Addr, io.Reader, io.Writer) error
	ErrorHandler func(net.Addr, io.Reader, io.Writer, error) error
)

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

func (h *PrefixHandler) Add(prefix string, handler Handler) *PrefixHandler {
	h.handlers[string(hashPrefix(prefix))] = handler

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
		return nil, e(nil, "handler not found")
	}

	return handler, nil
}

func hashPrefix(prefix string) []byte {
	return valuehash.NewSHA256([]byte(prefix)).Bytes()
}

func readPrefix(r io.Reader) ([]byte, error) {
	return util.ReadLengthedBytesFromReader(r)
}

func WritePrefix(w io.Writer, prefix string) error {
	return util.WriteLengthedBytes(w, hashPrefix(prefix))
}
