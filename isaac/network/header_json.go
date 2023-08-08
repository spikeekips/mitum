package isaacnetwork

import (
	"github.com/pkg/errors"
	"github.com/spikeekips/mitum/base"
	"github.com/spikeekips/mitum/network/quicstream"
	quicstreamheader "github.com/spikeekips/mitum/network/quicstream/header"
	"github.com/spikeekips/mitum/util"
	jsonenc "github.com/spikeekips/mitum/util/encoder/json"
	"github.com/spikeekips/mitum/util/valuehash"
)

type BaseHeaderJSONMarshaler struct {
	ClientID string `json:"client_id"`
	quicstreamheader.BaseHeaderJSONMarshaler
}

type BaseHeaderJSONUnmarshaler struct {
	ClientID string `json:"client_id"`
}

func (h baseHeader) JSONMarshaler() BaseHeaderJSONMarshaler {
	return BaseHeaderJSONMarshaler{
		BaseHeaderJSONMarshaler: h.BaseRequestHeader.JSONMarshaler(),
		ClientID:                h.clientID,
	}
}

func (h *baseHeader) UnmarshalJSON(b []byte) error {
	if err := util.UnmarshalJSON(b, &h.BaseRequestHeader); err != nil {
		return err
	}

	var u BaseHeaderJSONUnmarshaler
	if err := util.UnmarshalJSON(b, &u); err != nil {
		return err
	}

	h.BaseRequestHeader = quicstreamheader.NewBaseRequestHeader(h.Hint(), headerPrefixByHint(h.Hint()))
	h.clientID = u.ClientID

	return nil
}

type operationRequestHeaderJSONMarshaler struct {
	Operation util.Hash `json:"operation"`
}

func (h OperationRequestHeader) MarshalJSON() ([]byte, error) {
	return util.MarshalJSON(struct {
		operationRequestHeaderJSONMarshaler
		BaseHeaderJSONMarshaler
	}{
		BaseHeaderJSONMarshaler: h.baseHeader.JSONMarshaler(),
		operationRequestHeaderJSONMarshaler: operationRequestHeaderJSONMarshaler{
			Operation: h.h,
		},
	})
}

type operationRequestHeaderJSONUnmarshaler struct {
	Operation valuehash.HashDecoder `json:"operation"`
}

func (h *OperationRequestHeader) DecodeJSON(b []byte, _ *jsonenc.Encoder) error {
	e := util.StringError("decode OperationRequestHeader")

	if err := util.UnmarshalJSON(b, &h.baseHeader); err != nil {
		return e.Wrap(err)
	}

	var u operationRequestHeaderJSONUnmarshaler
	if err := util.UnmarshalJSON(b, &u); err != nil {
		return e.Wrap(err)
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
	PreviousBlock util.Hash    `json:"previous_block"`
	Proposer      base.Address `json:"proposer"`
	Point         base.Point   `json:"point"`
}

func (h RequestProposalRequestHeader) MarshalJSON() ([]byte, error) {
	return util.MarshalJSON(struct {
		requestProposalRequestHeaderJSONMarshaler
		BaseHeaderJSONMarshaler
	}{
		BaseHeaderJSONMarshaler: h.baseHeader.JSONMarshaler(),
		requestProposalRequestHeaderJSONMarshaler: requestProposalRequestHeaderJSONMarshaler{
			Proposer:      h.proposer,
			Point:         h.point,
			PreviousBlock: h.previousBlock,
		},
	})
}

type requestProposalRequestHeaderJSONUnmarshaler struct {
	PreviousBlock valuehash.HashDecoder `json:"previous_block"`
	Proposer      string                `json:"proposer"`
	Point         base.Point            `json:"point"`
}

func (h *RequestProposalRequestHeader) DecodeJSON(b []byte, enc *jsonenc.Encoder) error {
	e := util.StringError("decode RequestProposalHeader")

	var u requestProposalRequestHeaderJSONUnmarshaler

	if err := util.UnmarshalJSON(b, &u); err != nil {
		return e.Wrap(err)
	}

	if err := util.UnmarshalJSON(b, &h.baseHeader); err != nil {
		return e.Wrap(err)
	}

	switch addr, err := base.DecodeAddress(u.Proposer, enc); {
	case err != nil:
		return e.Wrap(err)
	default:
		h.proposer = addr
	}

	h.point = u.Point
	h.previousBlock = u.PreviousBlock.Hash()

	return nil
}

type proposalRequestHeaderJSONMarshaler struct {
	Proposal util.Hash `json:"proposal"`
}

func (h ProposalRequestHeader) MarshalJSON() ([]byte, error) {
	return util.MarshalJSON(struct {
		proposalRequestHeaderJSONMarshaler
		BaseHeaderJSONMarshaler
	}{
		BaseHeaderJSONMarshaler: h.baseHeader.JSONMarshaler(),
		proposalRequestHeaderJSONMarshaler: proposalRequestHeaderJSONMarshaler{
			Proposal: h.proposal,
		},
	})
}

type proposalRequestHeaderJSONUnmarshaler struct {
	Proposal valuehash.HashDecoder `json:"proposal"`
}

func (h *ProposalRequestHeader) UnmarshalJSON(b []byte) error {
	e := util.StringError("unmarshal ProposalRequestHeader")

	var u proposalRequestHeaderJSONUnmarshaler

	if err := util.UnmarshalJSON(b, &u); err != nil {
		return e.Wrap(err)
	}

	if err := util.UnmarshalJSON(b, &h.baseHeader); err != nil {
		return e.Wrap(err)
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
		BaseHeaderJSONMarshaler
	}{
		BaseHeaderJSONMarshaler: h.baseHeader.JSONMarshaler(),
		lastSuffrageProofRequestHeaderJSONMarshaler: lastSuffrageProofRequestHeaderJSONMarshaler{
			State: h.state,
		},
	})
}

type lastSuffrageProofRequestHeaderJSONUnmarshaler struct {
	State valuehash.HashDecoder `json:"state"`
}

func (h *LastSuffrageProofRequestHeader) UnmarshalJSON(b []byte) error {
	e := util.StringError("unmarshal LastSuffrageProofHeader")

	var u lastSuffrageProofRequestHeaderJSONUnmarshaler

	if err := util.UnmarshalJSON(b, &u); err != nil {
		return e.Wrap(err)
	}

	if err := util.UnmarshalJSON(b, &h.baseHeader); err != nil {
		return e.Wrap(err)
	}

	h.state = u.State.Hash()

	return nil
}

type suffrageProofRequestHeaderJSONMarshaler struct {
	SuffrageHeight base.Height `json:"suffrage_height"`
}

func (h SuffrageProofRequestHeader) MarshalJSON() ([]byte, error) {
	return util.MarshalJSON(struct {
		BaseHeaderJSONMarshaler
		suffrageProofRequestHeaderJSONMarshaler
	}{
		BaseHeaderJSONMarshaler: h.baseHeader.JSONMarshaler(),
		suffrageProofRequestHeaderJSONMarshaler: suffrageProofRequestHeaderJSONMarshaler{
			SuffrageHeight: h.suffrageheight,
		},
	})
}

type suffrageProofRequestHeaderJSONUnmarshaler struct {
	SuffrageHeight base.Height `json:"suffrage_height"`
}

func (h *SuffrageProofRequestHeader) UnmarshalJSON(b []byte) error {
	e := util.StringError("unmarshal SuffrageProofHeader")
	var u suffrageProofRequestHeaderJSONUnmarshaler

	if err := util.UnmarshalJSON(b, &u); err != nil {
		return e.Wrap(err)
	}

	if err := util.UnmarshalJSON(b, &h.baseHeader); err != nil {
		return e.Wrap(err)
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
		BaseHeaderJSONMarshaler
	}{
		BaseHeaderJSONMarshaler: h.baseHeader.JSONMarshaler(),
		lastBlockMapRequestHeaderJSONMarshaler: lastBlockMapRequestHeaderJSONMarshaler{
			Manifest: h.manifest,
		},
	})
}

type lastBlockMapRequestHeaderJSONUnmarshaler struct {
	Manifest valuehash.HashDecoder `json:"manifest"`
}

func (h *LastBlockMapRequestHeader) UnmarshalJSON(b []byte) error {
	e := util.StringError("unmarshal LastBlockMapHeader")
	var u lastBlockMapRequestHeaderJSONUnmarshaler

	if err := util.UnmarshalJSON(b, &u); err != nil {
		return e.Wrap(err)
	}

	if err := util.UnmarshalJSON(b, &h.baseHeader); err != nil {
		return e.Wrap(err)
	}

	h.manifest = u.Manifest.Hash()

	return nil
}

type BlockMapRequestHeaderJSONMarshaler struct {
	Height base.Height `json:"height"`
}

func (h BlockMapRequestHeader) MarshalJSON() ([]byte, error) {
	return util.MarshalJSON(struct {
		BaseHeaderJSONMarshaler
		BlockMapRequestHeaderJSONMarshaler
	}{
		BaseHeaderJSONMarshaler: h.baseHeader.JSONMarshaler(),
		BlockMapRequestHeaderJSONMarshaler: BlockMapRequestHeaderJSONMarshaler{
			Height: h.height,
		},
	})
}

type BlockMapRequestHeaderJSONUnmarshaler struct {
	Height base.Height `json:"height"`
}

func (h *BlockMapRequestHeader) UnmarshalJSON(b []byte) error {
	e := util.StringError("unmarshal BlockMapHeader")
	var u BlockMapRequestHeaderJSONUnmarshaler

	if err := util.UnmarshalJSON(b, &u); err != nil {
		return e.Wrap(err)
	}

	if err := util.UnmarshalJSON(b, &h.baseHeader); err != nil {
		return e.Wrap(err)
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
		BaseHeaderJSONMarshaler
	}{
		BaseHeaderJSONMarshaler: h.baseHeader.JSONMarshaler(),
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
	e := util.StringError("unmarshal BlockMapItemHeader")
	var u BlockMapItemRequestHeaderJSONUnmarshaler

	if err := util.UnmarshalJSON(b, &u); err != nil {
		return e.Wrap(err)
	}

	if err := util.UnmarshalJSON(b, &h.baseHeader); err != nil {
		return e.Wrap(err)
	}

	h.height = u.Height
	h.item = u.Item

	return nil
}

type NodeChallengeRequestHeaderJSONMarshaler struct {
	Me          base.Address   `json:"me,omitempty"`
	MePublickey base.Publickey `json:"me_publickey,omitempty"`
	Input       []byte         `json:"input"`
}

func (h NodeChallengeRequestHeader) MarshalJSON() ([]byte, error) {
	return util.MarshalJSON(struct {
		NodeChallengeRequestHeaderJSONMarshaler
		BaseHeaderJSONMarshaler
	}{
		BaseHeaderJSONMarshaler: h.baseHeader.JSONMarshaler(),
		NodeChallengeRequestHeaderJSONMarshaler: NodeChallengeRequestHeaderJSONMarshaler{
			Input:       h.input,
			Me:          h.me,
			MePublickey: h.mePub,
		},
	})
}

type NodeChallengeRequestHeaderJSONUnmarshaler struct {
	Me          string `json:"me,omitempty"`
	MePublickey string `json:"me_publickey,omitempty"`
	Input       []byte `json:"input"`
}

func (h *NodeChallengeRequestHeader) DecodeJSON(b []byte, enc *jsonenc.Encoder) error {
	e := util.StringError("unmarshal NodeChallengeHeader")
	var u NodeChallengeRequestHeaderJSONUnmarshaler

	if err := util.UnmarshalJSON(b, &u); err != nil {
		return e.Wrap(err)
	}

	switch i, err := base.DecodeAddress(u.Me, enc); {
	case err != nil:
		return e.WithMessage(err, "me")
	default:
		h.me = i
	}

	switch i, err := base.DecodePublickeyFromString(u.MePublickey, enc); {
	case err != nil:
		return e.WithMessage(err, "me pub")
	default:
		h.mePub = i
	}

	if err := util.UnmarshalJSON(b, &h.baseHeader); err != nil {
		return e.Wrap(err)
	}

	h.input = u.Input

	return nil
}

func (h SuffrageNodeConnInfoRequestHeader) MarshalJSON() ([]byte, error) {
	return util.MarshalJSON(h.baseHeader.JSONMarshaler())
}

func (h *SuffrageNodeConnInfoRequestHeader) UnmarshalJSON(b []byte) error {
	if err := util.UnmarshalJSON(b, &h.baseHeader); err != nil {
		return errors.WithMessage(err, "unmarshal SuffrageNodeConnInfoHeader")
	}

	return nil
}

func (h SyncSourceConnInfoRequestHeader) MarshalJSON() ([]byte, error) {
	return util.MarshalJSON(h.baseHeader.JSONMarshaler())
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
		BaseHeaderJSONMarshaler
	}{
		BaseHeaderJSONMarshaler: h.baseHeader.JSONMarshaler(),
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
	e := util.StringError("unmarshal StateRequestHeader")

	var u stateRequestHeaderJSONUnmarshaler

	if err := util.UnmarshalJSON(b, &u); err != nil {
		return e.Wrap(err)
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
		BaseHeaderJSONMarshaler
	}{
		BaseHeaderJSONMarshaler: h.baseHeader.JSONMarshaler(),
		existsInStateOperationRequestHeaderJSONMarshaler: existsInStateOperationRequestHeaderJSONMarshaler{
			Fact: h.facthash,
		},
	})
}

func (h *ExistsInStateOperationRequestHeader) UnmarshalJSON(b []byte) error {
	e := util.StringError("unmarshal ExistsInStateOperationRequestHeader")

	var u struct {
		Fact valuehash.HashDecoder `json:"fact"`
	}

	if err := util.UnmarshalJSON(b, &u); err != nil {
		return e.Wrap(err)
	}

	if err := util.UnmarshalJSON(b, &h.baseHeader); err != nil {
		return errors.WithMessage(err, "unmarshal ExistsInStateOperationRequestHeader")
	}

	h.facthash = u.Fact.Hash()

	return nil
}

func (h NodeInfoRequestHeader) MarshalJSON() ([]byte, error) {
	return util.MarshalJSON(h.baseHeader.JSONMarshaler())
}

func (h *NodeInfoRequestHeader) UnmarshalJSON(b []byte) error {
	if err := util.UnmarshalJSON(b, &h.baseHeader); err != nil {
		return errors.WithMessage(err, "unmarshal NodeInfoHeader")
	}

	return nil
}

func (h SendBallotsHeader) MarshalJSON() ([]byte, error) {
	return util.MarshalJSON(h.baseHeader.JSONMarshaler())
}

func (h *SendBallotsHeader) UnmarshalJSON(b []byte) error {
	if err := util.UnmarshalJSON(b, &h.baseHeader); err != nil {
		return errors.WithMessage(err, "unmarshal SendBallotsHeader")
	}

	return nil
}

type setAllowConsensusHeaderJSONMarshaler struct {
	Allow bool `json:"allow"`
}

func (h SetAllowConsensusHeader) MarshalJSON() ([]byte, error) {
	return util.MarshalJSON(struct {
		BaseHeaderJSONMarshaler
		setAllowConsensusHeaderJSONMarshaler
	}{
		BaseHeaderJSONMarshaler: h.baseHeader.JSONMarshaler(),
		setAllowConsensusHeaderJSONMarshaler: setAllowConsensusHeaderJSONMarshaler{
			Allow: h.allow,
		},
	})
}

func (h *SetAllowConsensusHeader) UnmarshalJSON(b []byte) error {
	e := util.StringError("decode SetAllowConsensusHeader")

	if err := util.UnmarshalJSON(b, &h.baseHeader); err != nil {
		return e.Wrap(err)
	}

	var u setAllowConsensusHeaderJSONMarshaler
	if err := util.UnmarshalJSON(b, &u); err != nil {
		return e.Wrap(err)
	}

	h.allow = u.Allow

	return nil
}

type streamOperationsHeaderJSONMarshaler struct {
	Offset []byte `json:"offset,omitempty"`
}

func (h StreamOperationsHeader) MarshalJSON() ([]byte, error) {
	return util.MarshalJSON(struct {
		streamOperationsHeaderJSONMarshaler
		BaseHeaderJSONMarshaler
	}{
		BaseHeaderJSONMarshaler: h.baseHeader.JSONMarshaler(),
		streamOperationsHeaderJSONMarshaler: streamOperationsHeaderJSONMarshaler{
			Offset: h.offset,
		},
	})
}

func (h *StreamOperationsHeader) UnmarshalJSON(b []byte) error {
	e := util.StringError("decode StreamOperationsHeader")

	if err := util.UnmarshalJSON(b, &h.baseHeader); err != nil {
		return e.Wrap(err)
	}

	var u streamOperationsHeaderJSONMarshaler
	if err := util.UnmarshalJSON(b, &u); err != nil {
		return e.Wrap(err)
	}

	h.offset = u.Offset

	return nil
}

type caHandoverHeaderJSONMarshaler struct {
	Address  base.Address        `json:"address"`
	ConnInfo quicstream.ConnInfo `json:"conn_info"`
}

func (h caHandoverHeader) MarshalJSON() ([]byte, error) {
	return util.MarshalJSON(struct {
		caHandoverHeaderJSONMarshaler
		BaseHeaderJSONMarshaler
	}{
		BaseHeaderJSONMarshaler: h.baseHeader.JSONMarshaler(),
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

	switch i, err := quicstream.NewConnInfoFromFullString(u.ConnInfo); {
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
		quicstreamheader.BaseResponseHeaderJSONMarshaler
	}{
		BaseResponseHeaderJSONMarshaler: h.BaseResponseHeader.JSONMarshaler(),
		askHandoverResponseHeaderJSONMarshaler: askHandoverResponseHeaderJSONMarshaler{
			ID: h.id,
		},
	})
}

func (h *AskHandoverResponseHeader) UnmarshalJSON(b []byte) error {
	e := util.StringError("unmarshal AskHandoverResponseHeader")

	if err := util.UnmarshalJSON(b, &h.BaseResponseHeader); err != nil {
		return e.Wrap(err)
	}

	var u askHandoverResponseHeaderJSONMarshaler
	if err := util.UnmarshalJSON(b, &u); err != nil {
		return e.Wrap(err)
	}

	h.id = u.ID

	return nil
}

type checkHandoverXHeaderJSONMarshaler struct {
	Address base.Address `json:"address"`
}

func (h CheckHandoverXHeader) MarshalJSON() ([]byte, error) {
	return util.MarshalJSON(struct {
		checkHandoverXHeaderJSONMarshaler
		BaseHeaderJSONMarshaler
	}{
		BaseHeaderJSONMarshaler: h.baseHeader.JSONMarshaler(),
		checkHandoverXHeaderJSONMarshaler: checkHandoverXHeaderJSONMarshaler{
			Address: h.address,
		},
	})
}

type checkHandoverXHeaderJSONUnmarshaler struct {
	Address string `json:"address"`
}

func (h *CheckHandoverXHeader) DecodeJSON(b []byte, enc *jsonenc.Encoder) error {
	if err := util.UnmarshalJSON(b, &h.baseHeader); err != nil {
		return err
	}

	var u checkHandoverXHeaderJSONUnmarshaler
	if err := util.UnmarshalJSON(b, &u); err != nil {
		return err
	}

	switch i, err := base.DecodeAddress(u.Address, enc); {
	case err != nil:
		return err
	default:
		h.address = i
	}

	return nil
}
