package main

import (
	isaacnetwork "github.com/spikeekips/mitum/isaac/network"
	"github.com/spikeekips/mitum/util"
	"github.com/spikeekips/mitum/util/hint"
	"github.com/spikeekips/mitum/util/valuehash"
)

var (
	stateRequestHeaderHint                  = hint.MustNewHint("state-header-v0.0.1")
	existsInStateOperationRequestHeaderHint = hint.MustNewHint("exists-instate-operation-header-v0.0.1")
)

type stateRequestHeader struct {
	key string
	isaacnetwork.BaseHeader
}

func newStateRequestHeader(key string) stateRequestHeader {
	return stateRequestHeader{
		BaseHeader: isaacnetwork.NewBaseHeader(stateRequestHeaderHint),
		key:        key,
	}
}

func (h stateRequestHeader) IsValid([]byte) error {
	e := util.StringErrorFunc("invalid stateHeader")

	if err := h.BaseHinter.IsValid(h.Hint().Type().Bytes()); err != nil {
		return e(err, "")
	}

	if len(h.key) < 1 {
		return e(nil, "empty state key")
	}

	return nil
}

func (stateRequestHeader) HandlerPrefix() string {
	return handlerPrefixRequestState
}

type stateRequestHeaderJSONMarshaler struct {
	Key string `json:"key"`
}

func (h stateRequestHeader) MarshalJSON() ([]byte, error) {
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

func (h *stateRequestHeader) UnmarshalJSON(b []byte) error {
	e := util.StringErrorFunc("failed to unmarshal stateRequestHeader")

	var u stateRequestHeaderJSONMarshaler

	if err := util.UnmarshalJSON(b, &u); err != nil {
		return e(err, "")
	}

	h.key = u.Key

	return nil
}

type existsInStateOperationRequestHeader struct {
	op util.Hash
	isaacnetwork.BaseHeader
}

func newExistsInStateOperationRequestHeader(op util.Hash) existsInStateOperationRequestHeader {
	return existsInStateOperationRequestHeader{
		BaseHeader: isaacnetwork.NewBaseHeader(existsInStateOperationRequestHeaderHint),
		op:         op,
	}
}

func (h existsInStateOperationRequestHeader) IsValid([]byte) error {
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

func (existsInStateOperationRequestHeader) HandlerPrefix() string {
	return handlerPrefixRequestExistsInStateOperation
}

type existsInStateOperationRequestHeaderJSONMarshaler struct {
	OP util.Hash `json:"op"`
}

func (h existsInStateOperationRequestHeader) MarshalJSON() ([]byte, error) {
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

func (h *existsInStateOperationRequestHeader) UnmarshalJSON(b []byte) error {
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
