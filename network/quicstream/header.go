package quicstream

import (
	"github.com/pkg/errors"
	"github.com/spikeekips/mitum/util"
	"github.com/spikeekips/mitum/util/hint"
)

var DefaultResponseHeaderHint = hint.MustNewHint("quicstream-default-response-header-v0.0.1")

type ContentType string

var (
	HinterContentType ContentType
	RawContentType    ContentType = "raw"
)

type Header interface {
	util.IsValider
	Handler() string
}

type ResponseHeader interface {
	Header
	Err() error
	OK() bool
	ContentType() ContentType
}

type BaseHeader struct {
	prefix string
	hint.BaseHinter
}

func NewBaseHeader(ht hint.Hint, prefix string) BaseHeader {
	return BaseHeader{
		BaseHinter: hint.NewBaseHinter(ht),
		prefix:     prefix,
	}
}

func (h BaseHeader) Handler() string {
	return h.prefix
}

type BaseHeaderJSONMarshaler struct {
	hint.BaseHinter
}

func (h BaseHeader) JSONMarshaler() BaseHeaderJSONMarshaler {
	return BaseHeaderJSONMarshaler{BaseHinter: h.BaseHinter}
}

func (h *BaseHeader) UnmarshalJSON(b []byte) error {
	var u BaseHeaderJSONMarshaler
	if err := util.UnmarshalJSON(b, &u); err != nil {
		return err
	}

	h.BaseHinter = u.BaseHinter

	return nil
}

type BaseResponseHeader struct {
	contentType ContentType
	err         error
	BaseHeader
	ok bool
}

func NewBaseResponseHeader(
	ht hint.Hint,
	ok bool,
	err error,
	contentType ContentType,
) BaseResponseHeader {
	// NOTE detailed internal error like network error or storage error will be
	// hidden
	rerr := err

	switch {
	case err == nil:
	case IsNetworkError(err):
		rerr = util.ErrInternal.Call()
	}

	return BaseResponseHeader{
		BaseHeader:  NewBaseHeader(ht, ""),
		ok:          ok,
		err:         rerr,
		contentType: contentType,
	}
}

func (r BaseResponseHeader) OK() bool {
	return r.ok
}

func (r BaseResponseHeader) Err() error {
	return r.err
}

func (r BaseResponseHeader) ContentType() ContentType {
	return r.contentType
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
			ContentType: r.contentType,
			Err:         err,
			OK:          r.ok,
		},
	}
}

type BaseResponseHeaderJSONUnmarshaler struct {
	ContentType ContentType `json:"content_type"`
	Err         string      `json:"error"` //nolint:tagliatelle //...
	OK          bool        `json:"ok"`
}

func (r *BaseResponseHeader) UnmarshalJSON(b []byte) error {
	if err := util.UnmarshalJSON(b, &r.BaseHeader); err != nil {
		return err
	}

	var u BaseResponseHeaderJSONUnmarshaler
	if err := util.UnmarshalJSON(b, &u); err != nil {
		return err
	}

	r.contentType = u.ContentType
	if len(u.Err) > 0 {
		r.err = errors.Errorf(u.Err)
	}

	r.ok = u.OK

	return nil
}

type DefaultResponseHeader struct {
	BaseResponseHeader
}

func NewDefaultResponseHeader(ok bool, err error, contentType ContentType) DefaultResponseHeader {
	return DefaultResponseHeader{
		BaseResponseHeader: NewBaseResponseHeader(DefaultResponseHeaderHint, ok, err, contentType),
	}
}

func (h DefaultResponseHeader) MarshalJSON() ([]byte, error) {
	return util.MarshalJSON(h.BaseResponseHeader.JSONMarshaler())
}

func (h *DefaultResponseHeader) UnmarshalJSON(b []byte) error {
	return util.UnmarshalJSON(b, &h.BaseResponseHeader)
}
