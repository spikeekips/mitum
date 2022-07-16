package main

import (
	isaacnetwork "github.com/spikeekips/mitum/isaac/network"
	"github.com/spikeekips/mitum/util"
	"github.com/spikeekips/mitum/util/hint"
	"github.com/spikeekips/mitum/util/valuehash"
)

var (
	StateRequestHeaderHint                  = hint.MustNewHint("state-header-v0.0.1")
	ExistsInStateOperationRequestHeaderHint = hint.MustNewHint("exists-instate-operation-header-v0.0.1")
)

type StateRequestHeader struct {
	key string
	isaacnetwork.BaseHeader
}

func NewStateRequestHeader(key string) StateRequestHeader {
	return StateRequestHeader{
		BaseHeader: isaacnetwork.NewBaseHeader(StateRequestHeaderHint),
		key:        key,
	}
}

func (h StateRequestHeader) IsValid([]byte) error {
	e := util.StringErrorFunc("invalid stateHeader")

	if err := h.BaseHinter.IsValid(h.Hint().Type().Bytes()); err != nil {
		return e(err, "")
	}

	if len(h.key) < 1 {
		return e(nil, "empty state key")
	}

	return nil
}

func (StateRequestHeader) HandlerPrefix() string {
	return HandlerPrefixRequestState
}

type stateRequestHeaderJSONMarshaler struct {
	Key string `json:"key"`
}

func (h StateRequestHeader) MarshalJSON() ([]byte, error) {
	return util.MarshalJSON(struct {
		stateRequestHeaderJSONMarshaler
		isaacnetwork.BaseHeader
	}{
		BaseHeader: h.BaseHeader,
		stateRequestHeaderJSONMarshaler: stateRequestHeaderJSONMarshaler{
			Key: h.key,
		},
	})
}

func (h *StateRequestHeader) UnmarshalJSON(b []byte) error {
	e := util.StringErrorFunc("failed to unmarshal stateRequestHeader")

	var u stateRequestHeaderJSONMarshaler

	if err := util.UnmarshalJSON(b, &u); err != nil {
		return e(err, "")
	}

	h.key = u.Key

	return nil
}

type ExistsInStateOperationRequestHeader struct {
	op util.Hash
	isaacnetwork.BaseHeader
}

func NewExistsInStateOperationRequestHeader(op util.Hash) ExistsInStateOperationRequestHeader {
	return ExistsInStateOperationRequestHeader{
		BaseHeader: isaacnetwork.NewBaseHeader(ExistsInStateOperationRequestHeaderHint),
		op:         op,
	}
}

func (h ExistsInStateOperationRequestHeader) IsValid([]byte) error {
	e := util.StringErrorFunc("invalid existsInStateOperationHeader")

	if err := h.BaseHinter.IsValid(h.Hint().Type().Bytes()); err != nil {
		return e(err, "")
	}

	if h.op == nil {
		return e(nil, "empty op")
	}

	if err := h.op.IsValid(nil); err != nil {
		return e(err, "")
	}

	return nil
}

func (ExistsInStateOperationRequestHeader) HandlerPrefix() string {
	return HandlerPrefixRequestExistsInStateOperation
}

type existsInStateOperationRequestHeaderJSONMarshaler struct {
	OP util.Hash `json:"op"`
}

func (h ExistsInStateOperationRequestHeader) MarshalJSON() ([]byte, error) {
	return util.MarshalJSON(struct {
		existsInStateOperationRequestHeaderJSONMarshaler
		isaacnetwork.BaseHeader
	}{
		BaseHeader: h.BaseHeader,
		existsInStateOperationRequestHeaderJSONMarshaler: existsInStateOperationRequestHeaderJSONMarshaler{
			OP: h.op,
		},
	})
}

func (h *ExistsInStateOperationRequestHeader) UnmarshalJSON(b []byte) error {
	e := util.StringErrorFunc("failed to unmarshal existsInStateOperationRequestHeader")

	var u struct {
		OP valuehash.HashDecoder `json:"op"`
	}

	if err := util.UnmarshalJSON(b, &u); err != nil {
		return e(err, "")
	}

	h.op = u.OP.Hash()

	return nil
}
