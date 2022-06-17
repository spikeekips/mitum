package isaacnetwork

import (
	"github.com/pkg/errors"
	"github.com/spikeekips/mitum/base"
	"github.com/spikeekips/mitum/util"
	jsonenc "github.com/spikeekips/mitum/util/encoder/json"
	"github.com/spikeekips/mitum/util/hint"
	"github.com/spikeekips/mitum/util/valuehash"
)

func (h *BaseHeader) unmarshalJSON(b []byte) error {
	var u hint.BaseHinter

	if err := util.UnmarshalJSON(b, &u); err != nil {
		return errors.Wrap(err, "")
	}

	h.BaseHinter = u

	switch u.Hint().Type() {
	case RequestProposalRequestHeaderHint.Type():
		h.prefix = HandlerPrefixRequestProposal
	case ProposalRequestHeaderHint.Type():
		h.prefix = HandlerPrefixProposal
	case LastSuffrageProofRequestHeaderHint.Type():
		h.prefix = HandlerPrefixLastSuffrageProof
	case SuffrageProofRequestHeaderHint.Type():
		h.prefix = HandlerPrefixSuffrageProof
	case LastBlockMapRequestHeaderHint.Type():
		h.prefix = HandlerPrefixLastBlockMap
	case BlockMapRequestHeaderHint.Type():
		h.prefix = HandlerPrefixBlockMap
	case BlockMapItemRequestHeaderHint.Type():
		h.prefix = HandlerPrefixBlockMapItem
	}

	return nil
}

type requestProposalRequestHeaderJSONMarshaler struct {
	Proposer base.Address `json:"proposer"`
	Point    base.Point   `json:"point"`
}

func (h RequestProposalRequestHeader) MarshalJSON() ([]byte, error) {
	return util.MarshalJSON(struct {
		BaseHeader
		requestProposalRequestHeaderJSONMarshaler
	}{
		BaseHeader: h.BaseHeader,
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
	e := util.StringErrorFunc("failed to unmarshal RequestProposalHeader")

	var u requestProposalRequestHeaderJSONUnmarshaler

	if err := util.UnmarshalJSON(b, &u); err != nil {
		return e(err, "")
	}

	if err := h.BaseHeader.unmarshalJSON(b); err != nil {
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
		BaseHeader
	}{
		BaseHeader: h.BaseHeader,
		proposalRequestHeaderJSONMarshaler: proposalRequestHeaderJSONMarshaler{
			Proposal: h.proposal,
		},
	})
}

type proposalRequestHeaderJSONUnmarshaler struct {
	Proposal valuehash.HashDecoder `json:"proposal"`
}

func (h *ProposalRequestHeader) UnmarshalJSON(b []byte) error {
	e := util.StringErrorFunc("failed to unmarshal proposalHeader")

	var u proposalRequestHeaderJSONUnmarshaler

	if err := util.UnmarshalJSON(b, &u); err != nil {
		return e(err, "")
	}

	if err := h.BaseHeader.unmarshalJSON(b); err != nil {
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
		BaseHeader
	}{
		BaseHeader: h.BaseHeader,
		lastSuffrageProofRequestHeaderJSONMarshaler: lastSuffrageProofRequestHeaderJSONMarshaler{
			State: h.state,
		},
	})
}

type lastSuffrageProofRequestHeaderJSONUnmarshaler struct {
	State valuehash.HashDecoder `json:"state"`
}

func (h *LastSuffrageProofRequestHeader) UnmarshalJSON(b []byte) error {
	e := util.StringErrorFunc("failed to unmarshal LastSuffrageProofHeader")

	var u lastSuffrageProofRequestHeaderJSONUnmarshaler

	if err := util.UnmarshalJSON(b, &u); err != nil {
		return e(err, "")
	}

	if err := h.BaseHeader.unmarshalJSON(b); err != nil {
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
		BaseHeader
		suffrageProofRequestHeaderJSONMarshaler
	}{
		BaseHeader: h.BaseHeader,
		suffrageProofRequestHeaderJSONMarshaler: suffrageProofRequestHeaderJSONMarshaler{
			SuffrageHeight: h.suffrageheight,
		},
	})
}

type suffrageProofRequestHeaderJSONUnmarshaler struct {
	SuffrageHeight base.Height `json:"suffrage_height"`
}

func (h *SuffrageProofRequestHeader) UnmarshalJSON(b []byte) error {
	e := util.StringErrorFunc("failed to unmarshal SuffrageProofHeader")
	var u suffrageProofRequestHeaderJSONUnmarshaler

	if err := util.UnmarshalJSON(b, &u); err != nil {
		return e(err, "")
	}

	if err := h.BaseHeader.unmarshalJSON(b); err != nil {
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
		BaseHeader
	}{
		BaseHeader: h.BaseHeader,
		lastBlockMapRequestHeaderJSONMarshaler: lastBlockMapRequestHeaderJSONMarshaler{
			Manifest: h.manifest,
		},
	})
}

type lastBlockMapRequestHeaderJSONUnmarshaler struct {
	Manifest valuehash.HashDecoder `json:"manifest"`
}

func (h *LastBlockMapRequestHeader) UnmarshalJSON(b []byte) error {
	e := util.StringErrorFunc("failed to unmarshal LastBlockMapHeader")
	var u lastBlockMapRequestHeaderJSONUnmarshaler

	if err := util.UnmarshalJSON(b, &u); err != nil {
		return e(err, "")
	}

	if err := h.BaseHeader.unmarshalJSON(b); err != nil {
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
		BaseHeader
		BlockMapRequestHeaderJSONMarshaler
	}{
		BaseHeader: h.BaseHeader,
		BlockMapRequestHeaderJSONMarshaler: BlockMapRequestHeaderJSONMarshaler{
			Height: h.height,
		},
	})
}

type BlockMapRequestHeaderJSONUnmarshaler struct {
	Height base.Height `json:"height"`
}

func (h *BlockMapRequestHeader) UnmarshalJSON(b []byte) error {
	e := util.StringErrorFunc("failed to unmarshal BlockMapHeader")
	var u BlockMapRequestHeaderJSONUnmarshaler

	if err := util.UnmarshalJSON(b, &u); err != nil {
		return e(err, "")
	}

	if err := h.BaseHeader.unmarshalJSON(b); err != nil {
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
		BaseHeader
	}{
		BaseHeader: h.BaseHeader,
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
	e := util.StringErrorFunc("failed to unmarshal BlockMapItemHeader")
	var u BlockMapItemRequestHeaderJSONUnmarshaler

	if err := util.UnmarshalJSON(b, &u); err != nil {
		return e(err, "")
	}

	if err := h.BaseHeader.unmarshalJSON(b); err != nil {
		return e(err, "")
	}

	h.height = u.Height
	h.item = u.Item

	return nil
}

type ResponseHeaderJSONMarshaler struct {
	Error string `json:"error,omitempty"`
	OK    bool   `json:"ok,omitempty"`
}

func (r ResponseHeader) MarshalJSON() ([]byte, error) {
	var err string
	if r.err != nil {
		err = r.err.Error()
	}

	return util.MarshalJSON(struct {
		ResponseHeaderJSONMarshaler
		BaseHeader
	}{
		BaseHeader: r.BaseHeader,
		ResponseHeaderJSONMarshaler: ResponseHeaderJSONMarshaler{
			OK:    r.ok,
			Error: err,
		},
	})
}

func (r *ResponseHeader) UnmarshalJSON(b []byte) error {
	var u ResponseHeaderJSONMarshaler

	if err := util.UnmarshalJSON(b, &u); err != nil {
		return errors.Wrap(err, "failed to unmarshal ResponseHeader")
	}

	r.ok = u.OK

	if len(u.Error) > 0 {
		r.err = errors.Errorf(u.Error)
	}

	return nil
}
