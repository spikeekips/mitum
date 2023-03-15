package isaacnetwork

import (
	"github.com/pkg/errors"
	"github.com/spikeekips/mitum/base"
	"github.com/spikeekips/mitum/network/quicstream"
	"github.com/spikeekips/mitum/util"
	jsonenc "github.com/spikeekips/mitum/util/encoder/json"
	"github.com/spikeekips/mitum/util/valuehash"
)

func (h *baseHeader) UnmarshalJSON(b []byte) error {
	if err := util.UnmarshalJSON(b, &h.BaseHeader); err != nil {
		return err
	}

	h.BaseHeader = quicstream.NewBaseHeader(h.Hint(), headerPrefixByHint(h.Hint()))

	return nil
}

type operationRequestHeaderJSONMarshaler struct {
	Operation util.Hash `json:"operation"`
}

func (h OperationRequestHeader) MarshalJSON() ([]byte, error) {
	return util.MarshalJSON(struct {
		operationRequestHeaderJSONMarshaler
		quicstream.BaseHeaderJSONMarshaler
	}{
		BaseHeaderJSONMarshaler: h.BaseHeader.JSONMarshaler(),
		operationRequestHeaderJSONMarshaler: operationRequestHeaderJSONMarshaler{
			Operation: h.h,
		},
	})
}

type operationRequestHeaderJSONUnmarshaler struct {
	Operation valuehash.HashDecoder `json:"operation"`
}

func (h *OperationRequestHeader) DecodeJSON(b []byte, _ *jsonenc.Encoder) error {
	e := util.StringErrorFunc("decode OperationRequestHeader")

	if err := util.UnmarshalJSON(b, &h.baseHeader); err != nil {
		return e(err, "")
	}

	var u operationRequestHeaderJSONUnmarshaler
	if err := util.UnmarshalJSON(b, &u); err != nil {
		return e(err, "")
	}

	h.h = u.Operation.Hash()

	return nil
}

func (h SendOperationRequestHeader) MarshalJSON() ([]byte, error) {
	return util.MarshalJSON(h.JSONMarshaler())
}

func (h *SendOperationRequestHeader) UnmarshalJSON(b []byte) error {
	if err := util.UnmarshalJSON(b, &h.baseHeader); err != nil {
		return errors.WithMessage(err, "unmarshal SendOperationRequestHeader")
	}

	return nil
}

type requestProposalRequestHeaderJSONMarshaler struct {
	Proposer base.Address `json:"proposer"`
	Point    base.Point   `json:"point"`
}

func (h RequestProposalRequestHeader) MarshalJSON() ([]byte, error) {
	return util.MarshalJSON(struct {
		requestProposalRequestHeaderJSONMarshaler
		quicstream.BaseHeaderJSONMarshaler
	}{
		BaseHeaderJSONMarshaler: h.BaseHeader.JSONMarshaler(),
		requestProposalRequestHeaderJSONMarshaler: requestProposalRequestHeaderJSONMarshaler{
			Proposer: h.proposer,
			Point:    h.point,
		},
	})
}

type requestProposalRequestHeaderJSONUnmarshaler struct {
	Proposer string     `json:"proposer"`
	Point    base.Point `json:"point"`
}

func (h *RequestProposalRequestHeader) DecodeJSON(b []byte, enc *jsonenc.Encoder) error {
	e := util.StringErrorFunc("decode RequestProposalHeader")

	var u requestProposalRequestHeaderJSONUnmarshaler

	if err := util.UnmarshalJSON(b, &u); err != nil {
		return e(err, "")
	}

	if err := util.UnmarshalJSON(b, &h.baseHeader); err != nil {
		return e(err, "")
	}

	switch addr, err := base.DecodeAddress(u.Proposer, enc); {
	case err != nil:
		return e(err, "")
	default:
		h.proposer = addr
	}

	h.point = u.Point

	return nil
}

type proposalRequestHeaderJSONMarshaler struct {
	Proposal util.Hash `json:"proposal"`
}

func (h ProposalRequestHeader) MarshalJSON() ([]byte, error) {
	return util.MarshalJSON(struct {
		proposalRequestHeaderJSONMarshaler
		quicstream.BaseHeaderJSONMarshaler
	}{
		BaseHeaderJSONMarshaler: h.BaseHeader.JSONMarshaler(),
		proposalRequestHeaderJSONMarshaler: proposalRequestHeaderJSONMarshaler{
			Proposal: h.proposal,
		},
	})
}

type proposalRequestHeaderJSONUnmarshaler struct {
	Proposal valuehash.HashDecoder `json:"proposal"`
}

func (h *ProposalRequestHeader) UnmarshalJSON(b []byte) error {
	e := util.StringErrorFunc("unmarshal ProposalRequestHeader")

	var u proposalRequestHeaderJSONUnmarshaler

	if err := util.UnmarshalJSON(b, &u); err != nil {
		return e(err, "")
	}

	if err := util.UnmarshalJSON(b, &h.baseHeader); err != nil {
		return e(err, "")
	}

	h.proposal = u.Proposal.Hash()

	return nil
}

type lastSuffrageProofRequestHeaderJSONMarshaler struct {
	State util.Hash `json:"state"`
}

func (h LastSuffrageProofRequestHeader) MarshalJSON() ([]byte, error) {
	return util.MarshalJSON(struct {
		lastSuffrageProofRequestHeaderJSONMarshaler
		quicstream.BaseHeaderJSONMarshaler
	}{
		BaseHeaderJSONMarshaler: h.BaseHeader.JSONMarshaler(),
		lastSuffrageProofRequestHeaderJSONMarshaler: lastSuffrageProofRequestHeaderJSONMarshaler{
			State: h.state,
		},
	})
}

type lastSuffrageProofRequestHeaderJSONUnmarshaler struct {
	State valuehash.HashDecoder `json:"state"`
}

func (h *LastSuffrageProofRequestHeader) UnmarshalJSON(b []byte) error {
	e := util.StringErrorFunc("unmarshal LastSuffrageProofHeader")

	var u lastSuffrageProofRequestHeaderJSONUnmarshaler

	if err := util.UnmarshalJSON(b, &u); err != nil {
		return e(err, "")
	}

	if err := util.UnmarshalJSON(b, &h.baseHeader); err != nil {
		return e(err, "")
	}

	h.state = u.State.Hash()

	return nil
}

type suffrageProofRequestHeaderJSONMarshaler struct {
	SuffrageHeight base.Height `json:"suffrage_height"`
}

func (h SuffrageProofRequestHeader) MarshalJSON() ([]byte, error) {
	return util.MarshalJSON(struct {
		quicstream.BaseHeaderJSONMarshaler
		suffrageProofRequestHeaderJSONMarshaler
	}{
		BaseHeaderJSONMarshaler: h.BaseHeader.JSONMarshaler(),
		suffrageProofRequestHeaderJSONMarshaler: suffrageProofRequestHeaderJSONMarshaler{
			SuffrageHeight: h.suffrageheight,
		},
	})
}

type suffrageProofRequestHeaderJSONUnmarshaler struct {
	SuffrageHeight base.Height `json:"suffrage_height"`
}

func (h *SuffrageProofRequestHeader) UnmarshalJSON(b []byte) error {
	e := util.StringErrorFunc("unmarshal SuffrageProofHeader")
	var u suffrageProofRequestHeaderJSONUnmarshaler

	if err := util.UnmarshalJSON(b, &u); err != nil {
		return e(err, "")
	}

	if err := util.UnmarshalJSON(b, &h.baseHeader); err != nil {
		return e(err, "")
	}

	h.suffrageheight = u.SuffrageHeight

	return nil
}

type lastBlockMapRequestHeaderJSONMarshaler struct {
	Manifest util.Hash `json:"manifest"`
}

func (h LastBlockMapRequestHeader) MarshalJSON() ([]byte, error) {
	return util.MarshalJSON(struct {
		lastBlockMapRequestHeaderJSONMarshaler
		quicstream.BaseHeaderJSONMarshaler
	}{
		BaseHeaderJSONMarshaler: h.BaseHeader.JSONMarshaler(),
		lastBlockMapRequestHeaderJSONMarshaler: lastBlockMapRequestHeaderJSONMarshaler{
			Manifest: h.manifest,
		},
	})
}

type lastBlockMapRequestHeaderJSONUnmarshaler struct {
	Manifest valuehash.HashDecoder `json:"manifest"`
}

func (h *LastBlockMapRequestHeader) UnmarshalJSON(b []byte) error {
	e := util.StringErrorFunc("unmarshal LastBlockMapHeader")
	var u lastBlockMapRequestHeaderJSONUnmarshaler

	if err := util.UnmarshalJSON(b, &u); err != nil {
		return e(err, "")
	}

	if err := util.UnmarshalJSON(b, &h.baseHeader); err != nil {
		return e(err, "")
	}

	h.manifest = u.Manifest.Hash()

	return nil
}

type BlockMapRequestHeaderJSONMarshaler struct {
	Height base.Height `json:"height"`
}

func (h BlockMapRequestHeader) MarshalJSON() ([]byte, error) {
	return util.MarshalJSON(struct {
		quicstream.BaseHeaderJSONMarshaler
		BlockMapRequestHeaderJSONMarshaler
	}{
		BaseHeaderJSONMarshaler: h.BaseHeader.JSONMarshaler(),
		BlockMapRequestHeaderJSONMarshaler: BlockMapRequestHeaderJSONMarshaler{
			Height: h.height,
		},
	})
}

type BlockMapRequestHeaderJSONUnmarshaler struct {
	Height base.Height `json:"height"`
}

func (h *BlockMapRequestHeader) UnmarshalJSON(b []byte) error {
	e := util.StringErrorFunc("unmarshal BlockMapHeader")
	var u BlockMapRequestHeaderJSONUnmarshaler

	if err := util.UnmarshalJSON(b, &u); err != nil {
		return e(err, "")
	}

	if err := util.UnmarshalJSON(b, &h.baseHeader); err != nil {
		return e(err, "")
	}

	h.height = u.Height

	return nil
}

type BlockMapItemRequestHeaderJSONMarshaler struct {
	Item   base.BlockMapItemType `json:"item"`
	Height base.Height           `json:"height"`
}

func (h BlockMapItemRequestHeader) MarshalJSON() ([]byte, error) {
	return util.MarshalJSON(struct {
		BlockMapItemRequestHeaderJSONMarshaler
		quicstream.BaseHeaderJSONMarshaler
	}{
		BaseHeaderJSONMarshaler: h.BaseHeader.JSONMarshaler(),
		BlockMapItemRequestHeaderJSONMarshaler: BlockMapItemRequestHeaderJSONMarshaler{
			Height: h.height,
			Item:   h.item,
		},
	})
}

type BlockMapItemRequestHeaderJSONUnmarshaler struct {
	Item   base.BlockMapItemType `json:"item"`
	Height base.Height           `json:"height"`
}

func (h *BlockMapItemRequestHeader) UnmarshalJSON(b []byte) error {
	e := util.StringErrorFunc("unmarshal BlockMapItemHeader")
	var u BlockMapItemRequestHeaderJSONUnmarshaler

	if err := util.UnmarshalJSON(b, &u); err != nil {
		return e(err, "")
	}

	if err := util.UnmarshalJSON(b, &h.baseHeader); err != nil {
		return e(err, "")
	}

	h.height = u.Height
	h.item = u.Item

	return nil
}

type NodeChallengeRequestHeaderJSONMarshaler struct {
	Input []byte `json:"input"`
}

func (h NodeChallengeRequestHeader) MarshalJSON() ([]byte, error) {
	return util.MarshalJSON(struct {
		NodeChallengeRequestHeaderJSONMarshaler
		quicstream.BaseHeaderJSONMarshaler
	}{
		BaseHeaderJSONMarshaler: h.BaseHeader.JSONMarshaler(),
		NodeChallengeRequestHeaderJSONMarshaler: NodeChallengeRequestHeaderJSONMarshaler{
			Input: h.input,
		},
	})
}

func (h *NodeChallengeRequestHeader) UnmarshalJSON(b []byte) error {
	e := util.StringErrorFunc("unmarshal NodeChallengeHeader")
	var u NodeChallengeRequestHeaderJSONMarshaler

	if err := util.UnmarshalJSON(b, &u); err != nil {
		return e(err, "")
	}

	if err := util.UnmarshalJSON(b, &h.baseHeader); err != nil {
		return e(err, "")
	}

	h.input = u.Input

	return nil
}

func (h SuffrageNodeConnInfoRequestHeader) MarshalJSON() ([]byte, error) {
	return util.MarshalJSON(h.BaseHeader.JSONMarshaler())
}

func (h *SuffrageNodeConnInfoRequestHeader) UnmarshalJSON(b []byte) error {
	if err := util.UnmarshalJSON(b, &h.baseHeader); err != nil {
		return errors.WithMessage(err, "unmarshal SuffrageNodeConnInfoHeader")
	}

	return nil
}

func (h SyncSourceConnInfoRequestHeader) MarshalJSON() ([]byte, error) {
	return util.MarshalJSON(h.BaseHeader.JSONMarshaler())
}

func (h *SyncSourceConnInfoRequestHeader) UnmarshalJSON(b []byte) error {
	if err := util.UnmarshalJSON(b, &h.baseHeader); err != nil {
		return errors.WithMessage(err, "unmarshal SyncSourceConnInfoRequestHeader")
	}

	return nil
}

type stateRequestHeaderJSONMarshaler struct {
	Hash util.Hash `json:"hash"`
	Key  string    `json:"key"`
}

func (h StateRequestHeader) MarshalJSON() ([]byte, error) {
	return util.MarshalJSON(struct {
		stateRequestHeaderJSONMarshaler
		quicstream.BaseHeaderJSONMarshaler
	}{
		BaseHeaderJSONMarshaler: h.BaseHeader.JSONMarshaler(),
		stateRequestHeaderJSONMarshaler: stateRequestHeaderJSONMarshaler{
			Key:  h.key,
			Hash: h.h,
		},
	})
}

type stateRequestHeaderJSONUnmarshaler struct {
	Hash valuehash.HashDecoder `json:"hash"`
	Key  string                `json:"key"`
}

func (h *StateRequestHeader) UnmarshalJSON(b []byte) error {
	e := util.StringErrorFunc("unmarshal StateRequestHeader")

	var u stateRequestHeaderJSONUnmarshaler

	if err := util.UnmarshalJSON(b, &u); err != nil {
		return e(err, "")
	}

	if err := util.UnmarshalJSON(b, &h.baseHeader); err != nil {
		return errors.WithMessage(err, "unmarshal StateRequestHeader")
	}

	h.key = u.Key
	h.h = u.Hash.Hash()

	return nil
}

type existsInStateOperationRequestHeaderJSONMarshaler struct {
	Fact util.Hash `json:"fact"`
}

func (h ExistsInStateOperationRequestHeader) MarshalJSON() ([]byte, error) {
	return util.MarshalJSON(struct {
		existsInStateOperationRequestHeaderJSONMarshaler
		quicstream.BaseHeaderJSONMarshaler
	}{
		BaseHeaderJSONMarshaler: h.BaseHeader.JSONMarshaler(),
		existsInStateOperationRequestHeaderJSONMarshaler: existsInStateOperationRequestHeaderJSONMarshaler{
			Fact: h.facthash,
		},
	})
}

func (h *ExistsInStateOperationRequestHeader) UnmarshalJSON(b []byte) error {
	e := util.StringErrorFunc("unmarshal ExistsInStateOperationRequestHeader")

	var u struct {
		Fact valuehash.HashDecoder `json:"fact"`
	}

	if err := util.UnmarshalJSON(b, &u); err != nil {
		return e(err, "")
	}

	if err := util.UnmarshalJSON(b, &h.baseHeader); err != nil {
		return errors.WithMessage(err, "unmarshal ExistsInStateOperationRequestHeader")
	}

	h.facthash = u.Fact.Hash()

	return nil
}

func (h NodeInfoRequestHeader) MarshalJSON() ([]byte, error) {
	return util.MarshalJSON(h.BaseHeader.JSONMarshaler())
}

func (h *NodeInfoRequestHeader) UnmarshalJSON(b []byte) error {
	if err := util.UnmarshalJSON(b, &h.baseHeader); err != nil {
		return errors.WithMessage(err, "unmarshal NodeInfoHeader")
	}

	return nil
}

func (h SendBallotsHeader) MarshalJSON() ([]byte, error) {
	return util.MarshalJSON(h.BaseHeader.JSONMarshaler())
}

func (h *SendBallotsHeader) UnmarshalJSON(b []byte) error {
	if err := util.UnmarshalJSON(b, &h.baseHeader); err != nil {
		return errors.WithMessage(err, "unmarshal SendBallotsHeader")
	}

	return nil
}
