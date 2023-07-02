package quicstreamheader

import (
	"context"

	"github.com/pkg/errors"
	"github.com/spikeekips/mitum/network/quicstream"
	"github.com/spikeekips/mitum/util"
	"github.com/spikeekips/mitum/util/hint"
)

type (
	DataType [1]byte
	BodyType [1]byte
)

var (
	UnknownDataType        DataType = [1]byte{0x00}
	RequestHeaderDataType  DataType = [1]byte{0x01}
	BodyDataType           DataType = [1]byte{0x02}
	ResponseHeaderDataType DataType = [1]byte{0x03}
)

var (
	UnknownBodyType     BodyType = [1]byte{0x00}
	EmptyBodyType       BodyType = [1]byte{0x01}
	FixedLengthBodyType BodyType = [1]byte{0x02}
	StreamBodyType      BodyType = [1]byte{0x03}
)

func (t DataType) IsValid([]byte) error {
	switch t {
	case RequestHeaderDataType,
		BodyDataType,
		ResponseHeaderDataType:
		return nil
	default:
		return util.ErrInvalid.Errorf("unknown data type, %v", t)
	}
}

func (t BodyType) IsValid([]byte) error {
	switch t {
	case EmptyBodyType,
		FixedLengthBodyType,
		StreamBodyType:
		return nil
	default:
		return util.ErrInvalid.Errorf("unknown body type, %v", t)
	}
}

type Header interface {
	util.IsValider
	QUICStreamHeader()
}

type RequestHeader interface {
	Header
	Handler() [32]byte // NOTE prefix of prefix handler
}

type ResponseHeader interface {
	Header
	Err() error
	OK() bool
}

var DefaultResponseHeaderHint = hint.MustNewHint("quicstream-default-response-header-v0.0.1")

type BaseHeader struct {
	hint.BaseHinter
}

func NewBaseHeader(ht hint.Hint) BaseHeader {
	return BaseHeader{
		BaseHinter: hint.NewBaseHinter(ht),
	}
}

func (h BaseHeader) IsValid([]byte) error {
	if err := h.BaseHinter.IsValid(h.Hint().Type().Bytes()); err != nil {
		return util.ErrInvalid.WithMessage(err, "BaseHeader")
	}

	return nil
}

func (BaseHeader) QUICStreamHeader() {}

type BaseRequestHeader struct {
	BaseHeader
	prefix [32]byte
}

func NewBaseRequestHeader(ht hint.Hint, prefix [32]byte) BaseRequestHeader {
	return BaseRequestHeader{
		BaseHeader: NewBaseHeader(ht),
		prefix:     prefix,
	}
}

func (h BaseRequestHeader) Handler() [32]byte {
	return h.prefix
}

type BaseResponseHeader struct {
	err error
	BaseHeader
	ok bool
}

func NewBaseResponseHeader(ht hint.Hint, ok bool, err error) BaseResponseHeader {
	// NOTE detailed internal error like network error or storage error will be
	// hidden.
	rerr := err

	switch {
	case err == nil:
	case errors.Is(err, context.Canceled), errors.Is(err, context.DeadlineExceeded):
	case quicstream.IsSeriousError(err):
		rerr = util.ErrInternal.WithStack()
	}

	return BaseResponseHeader{
		BaseHeader: NewBaseHeader(ht),
		ok:         ok,
		err:        rerr,
	}
}

func (r BaseResponseHeader) OK() bool {
	return r.ok
}

func (r BaseResponseHeader) Err() error {
	return r.err
}

type DefaultResponseHeader struct {
	BaseResponseHeader
}

func NewDefaultResponseHeader(ok bool, err error) DefaultResponseHeader {
	return DefaultResponseHeader{
		BaseResponseHeader: NewBaseResponseHeader(DefaultResponseHeaderHint, ok, err),
	}
}
