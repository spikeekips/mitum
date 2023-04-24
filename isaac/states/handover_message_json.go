package isaacstates

import (
	"encoding/json"
	"fmt"

	"github.com/pkg/errors"
	"github.com/spikeekips/mitum/base"
	"github.com/spikeekips/mitum/util"
	"github.com/spikeekips/mitum/util/encoder"
	jsonenc "github.com/spikeekips/mitum/util/encoder/json"
	"github.com/spikeekips/mitum/util/hint"
)

type baseHandoverMessageJSONMarshaler struct {
	ID string `json:"id"`
	hint.BaseHinter
}

func (h baseHandoverMessage) JSONMarshaller() baseHandoverMessageJSONMarshaler {
	return baseHandoverMessageJSONMarshaler{
		BaseHinter: h.BaseHinter,
		ID:         h.id,
	}
}

type baseHandoverMessageJSONUnmarshaler struct {
	ID string `json:"id"`
}

func (h *baseHandoverMessage) UnmarshalJSON(b []byte) error {
	if err := util.UnmarshalJSON(b, &h.BaseHinter); err != nil {
		return err
	}

	var u baseHandoverMessageJSONUnmarshaler
	if err := util.UnmarshalJSON(b, &u); err != nil {
		return errors.Errorf("unmarshal baseHandoverMessage")
	}

	h.id = u.ID

	return nil
}

type HandoverMessageReadyJSONMarshaler struct {
	Point base.StagePoint `json:"point"`
}

func (h HandoverMessageReady) MarshalJSON() ([]byte, error) {
	return util.MarshalJSON(struct {
		HandoverMessageReadyJSONMarshaler
		baseHandoverMessageJSONMarshaler
	}{
		baseHandoverMessageJSONMarshaler: h.baseHandoverMessage.JSONMarshaller(),
		HandoverMessageReadyJSONMarshaler: HandoverMessageReadyJSONMarshaler{
			Point: h.point,
		},
	})
}

func (h *HandoverMessageReady) UnmarshalJSON(b []byte) error {
	e := util.StringErrorFunc("unmarshal HandoverMessageReady")

	if err := util.UnmarshalJSON(b, &h.baseHandoverMessage); err != nil {
		return e(err, "")
	}

	var u HandoverMessageReadyJSONMarshaler
	if err := util.UnmarshalJSON(b, &u); err != nil {
		return e(err, "")
	}

	h.point = u.Point

	return nil
}

type HandoverMessageReadyResponseJSONMarshaler struct {
	Err   string          `json:"error,omitempty"` //nolint:tagliatelle //...
	Point base.StagePoint `json:"point"`
	OK    bool            `json:"ok"`
}

func (h HandoverMessageReadyResponse) MarshalJSON() ([]byte, error) {
	var err string
	if h.err != nil {
		err = h.err.Error()
	}

	return util.MarshalJSON(struct {
		HandoverMessageReadyResponseJSONMarshaler
		baseHandoverMessageJSONMarshaler
	}{
		baseHandoverMessageJSONMarshaler: h.baseHandoverMessage.JSONMarshaller(),
		HandoverMessageReadyResponseJSONMarshaler: HandoverMessageReadyResponseJSONMarshaler{
			Point: h.point,
			Err:   err,
			OK:    h.ok,
		},
	})
}

func (h *HandoverMessageReadyResponse) UnmarshalJSON(b []byte) error {
	e := util.StringErrorFunc("unmarshal HandoverMessageReadyResponse")

	if err := util.UnmarshalJSON(b, &h.baseHandoverMessage); err != nil {
		return e(err, "")
	}

	var u HandoverMessageReadyResponseJSONMarshaler
	if err := util.UnmarshalJSON(b, &u); err != nil {
		return e(err, "")
	}

	h.point = u.Point
	h.ok = u.OK

	if len(u.Err) > 0 {
		h.err = fmt.Errorf(u.Err) //nolint:goerr113 // no meaning to wrap
	}

	return nil
}

type HandoverMessageFinishJSONMarshaler struct {
	Voteproof base.Voteproof `json:"voteproof"`
}

func (h HandoverMessageFinish) MarshalJSON() ([]byte, error) {
	return util.MarshalJSON(struct {
		HandoverMessageFinishJSONMarshaler
		baseHandoverMessageJSONMarshaler
	}{
		baseHandoverMessageJSONMarshaler: h.baseHandoverMessage.JSONMarshaller(),
		HandoverMessageFinishJSONMarshaler: HandoverMessageFinishJSONMarshaler{
			Voteproof: h.vp,
		},
	})
}

func (h *HandoverMessageFinish) DecodeJSON(b []byte, enc *jsonenc.Encoder) error {
	e := util.StringErrorFunc("unmarshal HandoverMessageFinish")

	if err := util.UnmarshalJSON(b, &h.baseHandoverMessage); err != nil {
		return e(err, "")
	}

	var u struct {
		Voteproof json.RawMessage `json:"voteproof"`
	}

	if err := util.UnmarshalJSON(b, &u); err != nil {
		return e(err, "")
	}

	if err := encoder.Decode(enc, u.Voteproof, &h.vp); err != nil {
		return e(err, "finish voteproof")
	}

	return nil
}

type HandoverMessageChallengeStagePointJSONMarshaler struct {
	Point base.StagePoint `json:"point"`
}

func (h HandoverMessageChallengeStagePoint) MarshalJSON() ([]byte, error) {
	return util.MarshalJSON(struct {
		HandoverMessageChallengeStagePointJSONMarshaler
		baseHandoverMessageJSONMarshaler
	}{
		baseHandoverMessageJSONMarshaler: h.baseHandoverMessage.JSONMarshaller(),
		HandoverMessageChallengeStagePointJSONMarshaler: HandoverMessageChallengeStagePointJSONMarshaler{
			Point: h.point,
		},
	})
}

func (h *HandoverMessageChallengeStagePoint) UnmarshalJSON(b []byte) error {
	e := util.StringErrorFunc("unmarshal HandoverMessageChallengeStagePoint")

	if err := util.UnmarshalJSON(b, &h.baseHandoverMessage); err != nil {
		return e(err, "")
	}

	var u HandoverMessageChallengeStagePointJSONMarshaler
	if err := util.UnmarshalJSON(b, &u); err != nil {
		return e(err, "")
	}

	h.point = u.Point

	return nil
}

type HandoverMessageChallengeBlockMapJSONMarshaler struct {
	BlockMap base.BlockMap   `json:"blockmap"` //nolint:tagliatelle //...
	Point    base.StagePoint `json:"point"`
}

func (h HandoverMessageChallengeBlockMap) MarshalJSON() ([]byte, error) {
	return util.MarshalJSON(struct {
		HandoverMessageChallengeBlockMapJSONMarshaler
		baseHandoverMessageJSONMarshaler
	}{
		baseHandoverMessageJSONMarshaler: h.baseHandoverMessage.JSONMarshaller(),
		HandoverMessageChallengeBlockMapJSONMarshaler: HandoverMessageChallengeBlockMapJSONMarshaler{
			Point:    h.point,
			BlockMap: h.m,
		},
	})
}

func (h *HandoverMessageChallengeBlockMap) DecodeJSON(b []byte, enc *jsonenc.Encoder) error {
	e := util.StringErrorFunc("unmarshal HandoverMessageChallengeBlockMap")

	if err := util.UnmarshalJSON(b, &h.baseHandoverMessage); err != nil {
		return e(err, "")
	}

	var u struct {
		BlockMap json.RawMessage `json:"blockmap"` //nolint:tagliatelle //...
		Point    base.StagePoint `json:"point"`
	}

	if err := util.UnmarshalJSON(b, &u); err != nil {
		return e(err, "")
	}

	if err := encoder.Decode(enc, u.BlockMap, &h.m); err != nil {
		return e(err, "")
	}

	h.point = u.Point

	return nil
}

type HandoverMessageDataJSONMarshaler struct {
	Data interface{} `json:"data"`
}

func (h HandoverMessageData) MarshalJSON() ([]byte, error) {
	return util.MarshalJSON(struct {
		HandoverMessageDataJSONMarshaler
		baseHandoverMessageJSONMarshaler
	}{
		baseHandoverMessageJSONMarshaler: h.baseHandoverMessage.JSONMarshaller(),
		HandoverMessageDataJSONMarshaler: HandoverMessageDataJSONMarshaler{
			Data: h.i,
		},
	})
}

func (h *HandoverMessageData) DecodeJSON(b []byte, enc *jsonenc.Encoder) error {
	e := util.StringErrorFunc("unmarshal HandoverMessageData")

	if err := util.UnmarshalJSON(b, &h.baseHandoverMessage); err != nil {
		return e(err, "")
	}

	var u struct {
		Data json.RawMessage `json:"data"`
	}

	if err := util.UnmarshalJSON(b, &u); err != nil {
		return e(err, "")
	}

	switch o, err := enc.Decode(u.Data); {
	case err != nil:
		return e(err, "")
	default:
		h.i = o
	}

	return nil
}

func (h HandoverMessageCancel) MarshalJSON() ([]byte, error) {
	return util.MarshalJSON(h.baseHandoverMessage.JSONMarshaller())
}

func (h *HandoverMessageCancel) UnmarshalJSON(b []byte) error {
	if err := util.UnmarshalJSON(b, &h.baseHandoverMessage); err != nil {
		return errors.Errorf("unmarshal HandoverMessageCancel")
	}

	return nil
}
