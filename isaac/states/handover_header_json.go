package isaacstates

import (
	"github.com/spikeekips/mitum/base"
	"github.com/spikeekips/mitum/network"
	"github.com/spikeekips/mitum/network/quicstream"
	"github.com/spikeekips/mitum/util"
	jsonenc "github.com/spikeekips/mitum/util/encoder/json"
)

func (h *baseHeader) UnmarshalJSON(b []byte) error {
	if err := util.UnmarshalJSON(b, &h.BaseRequestHeader); err != nil {
		return err
	}

	h.BaseRequestHeader = quicstream.NewBaseRequestHeader(h.Hint(), headerPrefixByHint(h.Hint()))

	return nil
}

type caHandoverHeaderJSONMarshaler struct {
	ConnInfo network.ConnInfo `json:"conn_info"`
	Address  base.Address     `json:"address"`
}

func (h caHandoverHeader) MarshalJSON() ([]byte, error) {
	return util.MarshalJSON(struct {
		caHandoverHeaderJSONMarshaler
		quicstream.BaseRequestHeader
	}{
		BaseRequestHeader: h.BaseRequestHeader,
		caHandoverHeaderJSONMarshaler: caHandoverHeaderJSONMarshaler{
			ConnInfo: h.connInfo,
			Address:  h.address,
		},
	})
}

type caHandoverHeaderJSONUnmarshaler struct {
	ConnInfo string `json:"conn_info"`
	Address  string `json:"address"`
}

func (h *caHandoverHeader) DecodeJSON(b []byte, enc *jsonenc.Encoder) error {
	if err := util.UnmarshalJSON(b, &h.baseHeader); err != nil {
		return err
	}

	var u caHandoverHeaderJSONUnmarshaler
	if err := util.UnmarshalJSON(b, &u); err != nil {
		return err
	}

	switch i, err := quicstream.NewUDPConnInfoFromString(u.ConnInfo); {
	case err != nil:
		return err
	default:
		h.connInfo = i
	}

	switch i, err := base.DecodeAddress(u.Address, enc); {
	case err != nil:
		return err
	default:
		h.address = i
	}

	return nil
}

type askHandoverResponseHeaderJSONMarshaler struct {
	ID string `json:"id"`
}

func (h AskHandoverResponseHeader) MarshalJSON() ([]byte, error) {
	return util.MarshalJSON(struct {
		askHandoverResponseHeaderJSONMarshaler
		quicstream.BaseResponseHeaderJSONMarshaler
	}{
		BaseResponseHeaderJSONMarshaler: h.BaseResponseHeader.JSONMarshaler(),
		askHandoverResponseHeaderJSONMarshaler: askHandoverResponseHeaderJSONMarshaler{
			ID: h.id,
		},
	})
}

func (h *AskHandoverResponseHeader) UnmarshalJSON(b []byte) error {
	e := util.StringErrorFunc("unmarshal AskHandoverResponseHeader")

	if err := util.UnmarshalJSON(b, &h.BaseResponseHeader); err != nil {
		return e(err, "")
	}

	var u askHandoverResponseHeaderJSONMarshaler
	if err := util.UnmarshalJSON(b, &u); err != nil {
		return e(err, "")
	}

	h.id = u.ID

	return nil
}
