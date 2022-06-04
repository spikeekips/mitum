package isaacnetwork

import (
	"github.com/pkg/errors"
	"github.com/spikeekips/mitum/base"
	"github.com/spikeekips/mitum/util"
	jsonenc "github.com/spikeekips/mitum/util/encoder/json"
	"github.com/spikeekips/mitum/util/valuehash"
)

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

	var u struct {
		BaseHeader
		requestProposalRequestHeaderJSONUnmarshaler
	}

	if err := util.UnmarshalJSON(b, &u); err != nil {
		return e(err, "")
	}

	h.BaseHeader = u.BaseHeader

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
	var u struct {
		proposalRequestHeaderJSONUnmarshaler
		BaseHeader
	}

	if err := util.UnmarshalJSON(b, &u); err != nil {
		return errors.Wrap(err, "failed to unmarshal proposalHeader")
	}

	h.BaseHeader = u.BaseHeader
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
	var u struct {
		lastSuffrageProofRequestHeaderJSONUnmarshaler
		BaseHeader
	}

	if err := util.UnmarshalJSON(b, &u); err != nil {
		return errors.Wrap(err, "failed to unmarshal LastSuffrageProofHeader")
	}

	h.BaseHeader = u.BaseHeader
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
	var u struct {
		BaseHeader
		suffrageProofRequestHeaderJSONUnmarshaler
	}

	if err := util.UnmarshalJSON(b, &u); err != nil {
		return errors.Wrap(err, "failed to unmarshal SuffrageProofHeader")
	}

	h.BaseHeader = u.BaseHeader
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
	var u struct {
		lastBlockMapRequestHeaderJSONUnmarshaler
		BaseHeader
	}

	if err := util.UnmarshalJSON(b, &u); err != nil {
		return errors.Wrap(err, "failed to unmarshal LastBlockMapHeader")
	}

	h.BaseHeader = u.BaseHeader
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
	var u struct {
		BaseHeader
		BlockMapRequestHeaderJSONUnmarshaler
	}

	if err := util.UnmarshalJSON(b, &u); err != nil {
		return errors.Wrap(err, "failed to unmarshal BlockMapHeader")
	}

	h.BaseHeader = u.BaseHeader
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
	var u struct {
		BlockMapItemRequestHeaderJSONUnmarshaler
		BaseHeader
	}

	if err := util.UnmarshalJSON(b, &u); err != nil {
		return errors.Wrap(err, "failed to unmarshal BlockMapItemHeader")
	}

	h.BaseHeader = u.BaseHeader
	h.height = u.Height
	h.item = u.Item

	return nil
}

type ErrorResponseHeaderJSONMarshaler struct {
	Error string `json:"error,omitempty"`
}

func (r ErrorResponseHeader) MarshalJSON() ([]byte, error) {
	if r.err == nil {
		return nil, nil
	}

	return util.MarshalJSON(struct {
		ErrorResponseHeaderJSONMarshaler
		BaseHeader
	}{
		BaseHeader: r.BaseHeader,
		ErrorResponseHeaderJSONMarshaler: ErrorResponseHeaderJSONMarshaler{
			Error: r.err.Error(),
		},
	})
}

func (r *ErrorResponseHeader) UnmarshalJSON(b []byte) error {
	var u struct {
		ErrorResponseHeaderJSONMarshaler
		BaseHeader
	}

	if err := util.UnmarshalJSON(b, &u); err != nil {
		return errors.Wrap(err, "failed to unmarshal ErrorResponseHeader")
	}

	r.BaseHeader = u.BaseHeader

	if len(u.Error) > 0 {
		r.err = errors.Errorf(u.Error)
	}

	return nil
}

type OKResponseHeaderJSONMarshaler struct {
	Error string `json:"error,omitempty"`
	OK    bool   `json:"ok,omitempty"`
}

func (r OKResponseHeader) MarshalJSON() ([]byte, error) {
	var err string
	if r.err != nil {
		err = r.err.Error()
	}

	return util.MarshalJSON(struct {
		OKResponseHeaderJSONMarshaler
		BaseHeader
	}{
		BaseHeader: r.BaseHeader,
		OKResponseHeaderJSONMarshaler: OKResponseHeaderJSONMarshaler{
			OK:    r.ok,
			Error: err,
		},
	})
}

func (r *OKResponseHeader) UnmarshalJSON(b []byte) error {
	var u struct {
		OKResponseHeaderJSONMarshaler
		BaseHeader
	}

	if err := util.UnmarshalJSON(b, &u); err != nil {
		return errors.Wrap(err, "failed to unmarshal OKResponseHeader")
	}

	r.BaseHeader = u.BaseHeader

	r.ok = u.OK

	if len(u.Error) > 0 {
		r.err = errors.Errorf(u.Error)
	}

	return nil
}
