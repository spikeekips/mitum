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

type HandoverMessageChallengeResponseJSONMarshaler struct {
	Err   string          `json:"error,omitempty"` //nolint:tagliatelle //...
	Point base.StagePoint `json:"point"`
	OK    bool            `json:"ok"`
}

func (h HandoverMessageChallengeResponse) MarshalJSON() ([]byte, error) {
	var err string
	if h.err != nil {
		err = h.err.Error()
	}

	return util.MarshalJSON(struct {
		HandoverMessageChallengeResponseJSONMarshaler
		baseHandoverMessageJSONMarshaler
	}{
		baseHandoverMessageJSONMarshaler: h.baseHandoverMessage.JSONMarshaller(),
		HandoverMessageChallengeResponseJSONMarshaler: HandoverMessageChallengeResponseJSONMarshaler{
			Point: h.point,
			Err:   err,
			OK:    h.ok,
		},
	})
}

func (h *HandoverMessageChallengeResponse) UnmarshalJSON(b []byte) error {
	e := util.StringError("unmarshal HandoverMessageChallengeResponse")

	if err := util.UnmarshalJSON(b, &h.baseHandoverMessage); err != nil {
		return e.Wrap(err)
	}

	var u HandoverMessageChallengeResponseJSONMarshaler
	if err := util.UnmarshalJSON(b, &u); err != nil {
		return e.Wrap(err)
	}

	h.point = u.Point
	h.ok = u.OK

	if len(u.Err) > 0 {
		h.err = fmt.Errorf(u.Err) //nolint:goerr113 // no meaning to wrap
	}

	return nil
}

type HandoverMessageFinishJSONMarshaler struct {
	Proposal  base.ProposalSignFact `json:"proposal"`
	Voteproof base.Voteproof        `json:"voteproof"`
}

func (h HandoverMessageFinish) MarshalJSON() ([]byte, error) {
	return util.MarshalJSON(struct {
		HandoverMessageFinishJSONMarshaler
		baseHandoverMessageJSONMarshaler
	}{
		baseHandoverMessageJSONMarshaler: h.baseHandoverMessage.JSONMarshaller(),
		HandoverMessageFinishJSONMarshaler: HandoverMessageFinishJSONMarshaler{
			Proposal:  h.pr,
			Voteproof: h.vp,
		},
	})
}

func (h *HandoverMessageFinish) DecodeJSON(b []byte, enc *jsonenc.Encoder) error {
	e := util.StringError("unmarshal HandoverMessageFinish")

	if err := util.UnmarshalJSON(b, &h.baseHandoverMessage); err != nil {
		return e.Wrap(err)
	}

	var u struct {
		Proposal  json.RawMessage `json:"proposal"`
		Voteproof json.RawMessage `json:"voteproof"`
	}

	if err := util.UnmarshalJSON(b, &u); err != nil {
		return e.Wrap(err)
	}

	if err := encoder.Decode(enc, u.Proposal, &h.pr); err != nil {
		return e.WithMessage(err, "finish voteproof")
	}

	if err := encoder.Decode(enc, u.Voteproof, &h.vp); err != nil {
		return e.WithMessage(err, "finish voteproof")
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
	e := util.StringError("unmarshal HandoverMessageChallengeStagePoint")

	if err := util.UnmarshalJSON(b, &h.baseHandoverMessage); err != nil {
		return e.Wrap(err)
	}

	var u HandoverMessageChallengeStagePointJSONMarshaler
	if err := util.UnmarshalJSON(b, &u); err != nil {
		return e.Wrap(err)
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
	e := util.StringError("unmarshal HandoverMessageChallengeBlockMap")

	if err := util.UnmarshalJSON(b, &h.baseHandoverMessage); err != nil {
		return e.Wrap(err)
	}

	var u struct {
		BlockMap json.RawMessage `json:"blockmap"` //nolint:tagliatelle //...
		Point    base.StagePoint `json:"point"`
	}

	if err := util.UnmarshalJSON(b, &u); err != nil {
		return e.Wrap(err)
	}

	if err := encoder.Decode(enc, u.BlockMap, &h.m); err != nil {
		return e.Wrap(err)
	}

	h.point = u.Point

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

type HandoverMessageDataJSONMarshaler struct {
	Data     interface{}             `json:"data"`
	DataType HandoverMessageDataType `json:"data_type"`
}

func (h HandoverMessageData) MarshalJSON() ([]byte, error) {
	return util.MarshalJSON(struct {
		HandoverMessageDataJSONMarshaler
		baseHandoverMessageJSONMarshaler
	}{
		baseHandoverMessageJSONMarshaler: h.baseHandoverMessage.JSONMarshaller(),
		HandoverMessageDataJSONMarshaler: HandoverMessageDataJSONMarshaler{
			Data:     h.data,
			DataType: h.dataType,
		},
	})
}

func (h *HandoverMessageData) DecodeJSON(b []byte, enc *jsonenc.Encoder) error {
	e := util.StringError("unmarshal HandoverMessageData")

	if err := util.UnmarshalJSON(b, &h.baseHandoverMessage); err != nil {
		return e.Wrap(err)
	}

	var u struct {
		DataType HandoverMessageDataType `json:"data_type"`
		Data     json.RawMessage         `json:"data"`
	}

	if err := util.UnmarshalJSON(b, &u); err != nil {
		return e.Wrap(err)
	}

	switch data, err := h.decodeDataJSON(u.DataType, u.Data, enc); {
	case err != nil:
		return e.WithMessage(err, "data")
	default:
		h.dataType = u.DataType
		h.data = data
	}

	return nil
}

func (h *HandoverMessageData) decodeDataJSON(
	dataType HandoverMessageDataType, b []byte, enc *jsonenc.Encoder,
) (interface{}, error) {
	switch dataType {
	case HandoverMessageDataTypeVoteproof:
		return h.decodeJSONVoteproof(b, enc)
	case HandoverMessageDataTypeINITVoteproof:
		return h.decodeJSONINITVoteproof(b, enc)
	default:
		return nil, errors.Errorf("unknown handover message data type, %q", dataType)
	}
}

func (h *HandoverMessageData) decodeJSONVoteproof(b []byte, enc *jsonenc.Encoder) (interface{}, error) {
	var vp base.Voteproof

	if err := encoder.Decode(enc, b, &vp); err != nil {
		return nil, errors.WithMessage(err, "voteproof")
	}

	return vp, nil
}

func (h *HandoverMessageData) decodeJSONINITVoteproof(b []byte, enc *jsonenc.Encoder) (interface{}, error) {
	var u []json.RawMessage

	if err := enc.Unmarshal(b, &u); err != nil {
		return nil, errors.WithMessage(err, "init voteproof")
	}

	if len(u) < 2 { //nolint:gomnd //...
		return nil, errors.Errorf("init voteproof, not [2]interface{}")
	}

	var pr base.ProposalSignFact

	if len(u[0]) > 0 {
		if err := encoder.Decode(enc, u[0], &pr); err != nil {
			return nil, errors.WithMessage(err, "voteproof")
		}
	}

	var vp base.Voteproof

	if len(u[1]) > 0 {
		if err := encoder.Decode(enc, u[1], &vp); err != nil {
			return nil, errors.WithMessage(err, "voteproof")
		}
	}

	return []interface{}{pr, vp}, nil
}
