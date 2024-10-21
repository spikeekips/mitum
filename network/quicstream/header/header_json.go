package quicstreamheader

import (
	"github.com/pkg/errors"
	"github.com/spikeekips/mitum/util"
	"github.com/spikeekips/mitum/util/hint"
)

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

func (h *BaseRequestHeader) UnmarshalJSON(b []byte) error {
	return util.UnmarshalJSON(b, &h.BaseHeader)
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

	if u.Err != "" {
		r.err = errors.New(u.Err)
	}

	r.ok = u.OK

	return nil
}

func (h DefaultResponseHeader) MarshalJSON() ([]byte, error) {
	return util.MarshalJSON(h.BaseResponseHeader.JSONMarshaler())
}

func (h *DefaultResponseHeader) UnmarshalJSON(b []byte) error {
	return util.UnmarshalJSON(b, &h.BaseResponseHeader)
}
