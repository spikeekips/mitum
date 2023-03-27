package quicstream

import (
	"github.com/pkg/errors"
	"github.com/spikeekips/mitum/util"
	"github.com/spikeekips/mitum/util/hint"
)

var DefaultResponseHeaderHint = hint.MustNewHint("quicstream-default-response-header-v0.0.1")

type Header interface {
	util.IsValider
	QUICStreamHeader()
}

type RequestHeader interface {
	Header
	Handler() []byte
}

type ResponseHeader interface {
	Header
	Err() error
	OK() bool
}

type BaseHeader struct {
	hint.BaseHinter
}

func NewBaseHeader(ht hint.Hint) BaseHeader {
	return BaseHeader{
		BaseHinter: hint.NewBaseHinter(ht),
	}
}

func (BaseHeader) IsValid([]byte) error {
	return nil
}

func (BaseHeader) QUICStreamHeader() {}

type BaseHeaderJSONMarshaler struct {
	hint.BaseHinter
}

func (h BaseHeader) JSONMarshaler() BaseHeaderJSONMarshaler {
	return BaseHeaderJSONMarshaler(h)
}

func (h *BaseHeader) UnmarshalJSON(b []byte) error {
	var u BaseHeaderJSONMarshaler
	if err := util.UnmarshalJSON(b, &u); err != nil {
		return err
	}

	h.BaseHinter = u.BaseHinter

	return nil
}

type BaseRequestHeader struct {
	prefix []byte
	BaseHeader
}

func NewBaseRequestHeader(ht hint.Hint, prefix []byte) BaseRequestHeader {
	return BaseRequestHeader{
		BaseHeader: NewBaseHeader(ht),
		prefix:     prefix,
	}
}

func (h BaseRequestHeader) Handler() []byte {
	return h.prefix
}

func (h *BaseRequestHeader) UnmarshalJSON(b []byte) error {
	return util.UnmarshalJSON(b, &h.BaseHeader)
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
	case IsNetworkError(err):
		rerr = util.ErrInternal.Call()
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

type BaseResponseHeaderJSONUnmarshaler struct {
	Err string `json:"error,omitempty"` //nolint:tagliatelle //...
	OK  bool   `json:"ok"`
}

type BaseResponseHeaderJSONMarshaler struct {
	BaseResponseHeaderJSONUnmarshaler
	BaseHeaderJSONMarshaler
}

func (r BaseResponseHeader) JSONMarshaler() BaseResponseHeaderJSONMarshaler {
	var err string
	if r.err != nil {
		err = r.err.Error()
	}

	return BaseResponseHeaderJSONMarshaler{
		BaseHeaderJSONMarshaler: r.BaseHeader.JSONMarshaler(),
		BaseResponseHeaderJSONUnmarshaler: BaseResponseHeaderJSONUnmarshaler{
			Err: err,
			OK:  r.ok,
		},
	}
}

func (r *BaseResponseHeader) UnmarshalJSON(b []byte) error {
	if err := util.UnmarshalJSON(b, &r.BaseHeader); err != nil {
		return err
	}

	var u BaseResponseHeaderJSONUnmarshaler
	if err := util.UnmarshalJSON(b, &u); err != nil {
		return err
	}

	if len(u.Err) > 0 {
		r.err = errors.Errorf(u.Err)
	}

	r.ok = u.OK

	return nil
}

type DefaultResponseHeader struct {
	BaseResponseHeader
}

func NewDefaultResponseHeader(ok bool, err error) DefaultResponseHeader {
	return DefaultResponseHeader{
		BaseResponseHeader: NewBaseResponseHeader(DefaultResponseHeaderHint, ok, err),
	}
}

func (h DefaultResponseHeader) MarshalJSON() ([]byte, error) {
	return util.MarshalJSON(h.BaseResponseHeader.JSONMarshaler())
}

func (h *DefaultResponseHeader) UnmarshalJSON(b []byte) error {
	return util.UnmarshalJSON(b, &h.BaseResponseHeader)
}
