package isaacnetwork

import (
	"bytes"

	"github.com/pkg/errors"
	"github.com/spikeekips/mitum/base"
	"github.com/spikeekips/mitum/util"
	"github.com/spikeekips/mitum/util/encoder"
	jsonenc "github.com/spikeekips/mitum/util/encoder/json"
	"github.com/spikeekips/mitum/util/hint"
	"github.com/spikeekips/mitum/util/valuehash"
)

var (
	RequestProposalBodyHint = hint.MustNewHint("request-proposal-body-v0.0.1")
	ProposalBodyHint        = hint.MustNewHint("proposal-body-v0.0.1")
)

var (
	HandlerPrefixRequestProposal = "request_proposal"
	HandlerPrefixProposal        = "proposal"
	HandlerPrefixLastSuffrage    = "last_suffrage_"
)

type baseNodeNetwork struct {
	encs *encoder.Encoders
	enc  encoder.Encoder
}

func newBaseNodeNetwork(
	encs *encoder.Encoders,
	enc encoder.Encoder,
) *baseNodeNetwork {
	return &baseNodeNetwork{
		encs: encs,
		enc:  enc,
	}
}

func (c *baseNodeNetwork) marshal(i interface{}) ([]byte, error) {
	b, err := c.enc.Marshal(i)
	if err != nil {
		return nil, err
	}

	return c.marshalWithEncoder(b), nil
}

func (c *baseNodeNetwork) marshalWithEncoder(b []byte) []byte {
	h := make([]byte, hint.MaxHintLength)
	copy(h, c.enc.Hint().Bytes())

	return util.ConcatBytesSlice(h, b)
}

func (c *baseNodeNetwork) readEncoder(b []byte) (encoder.Encoder, []byte, error) {
	if b == nil {
		return nil, nil, nil
	}

	var ht hint.Hint
	ht, raw, err := c.readHint(b)
	if err != nil {
		return nil, nil, err
	}

	switch enc := c.encs.Find(ht); {
	case enc == nil:
		return nil, nil, util.NotFoundError.Errorf("encoder not found for %q", ht)
	default:
		return enc, raw, nil
	}
}

func (c *baseNodeNetwork) readHinter(b []byte) (interface{}, error) {
	switch enc, raw, err := c.readEncoder(b); {
	case err != nil:
		return nil, err
	case enc == nil:
		return nil, nil
	default:
		return enc.Decode(raw)
	}
}

func (*baseNodeNetwork) readHint(b []byte) (hint.Hint, []byte, error) {
	if len(b) < hint.MaxHintLength {
		return hint.Hint{}, nil, errors.Errorf("none hinted string; too short")
	}

	ht, err := hint.ParseHint(string(bytes.TrimRight(b[:hint.MaxHintLength], "\x00")))
	if err != nil {
		return hint.Hint{}, nil, err
	}

	return ht, b[hint.MaxHintLength:], nil
}

type RequestProposalBody struct {
	Proposer base.Address
	hint.BaseHinter
	Point base.Point
}

func NewRequestProposalBody(point base.Point, proposer base.Address) RequestProposalBody {
	return RequestProposalBody{
		BaseHinter: hint.NewBaseHinter(RequestProposalBodyHint),
		Point:      point,
		Proposer:   proposer,
	}
}

func (body RequestProposalBody) IsValid([]byte) error {
	e := util.StringErrorFunc("invalid RequestProposalBody")

	if err := body.BaseHinter.IsValid(body.Hint().Type().Bytes()); err != nil {
		return e(err, "")
	}

	if err := util.CheckIsValid(nil, false, body.Point, body.Proposer); err != nil {
		return e(err, "")
	}

	return nil
}

type requestProposalBodyJSONUnmarshaler struct {
	Proposer string
	Point    base.Point
}

func (body *RequestProposalBody) DecodeJSON(b []byte, enc *jsonenc.Encoder) error {
	e := util.StringErrorFunc("failed to unmarshal RequestProposalBody")

	var u requestProposalBodyJSONUnmarshaler
	if err := util.UnmarshalJSON(b, &u); err != nil {
		return e(err, "")
	}

	switch addr, err := base.DecodeAddress(u.Proposer, enc); {
	case err != nil:
		return e(err, "")
	default:
		body.Proposer = addr
	}

	body.Point = u.Point

	return nil
}

type ProposalBody struct {
	Proposal util.Hash
	hint.BaseHinter
}

func NewProposalBody(pr util.Hash) ProposalBody {
	return ProposalBody{
		BaseHinter: hint.NewBaseHinter(ProposalBodyHint),
		Proposal:   pr,
	}
}

func (body ProposalBody) IsValid([]byte) error {
	e := util.StringErrorFunc("invalid ProposalBody")

	if err := body.BaseHinter.IsValid(body.Hint().Type().Bytes()); err != nil {
		return e(err, "")
	}

	if err := util.CheckIsValid(nil, false, body.Proposal); err != nil {
		return e(err, "")
	}

	return nil
}

type proposalBodyJSONUnmarshaler struct {
	Proposal valuehash.HashDecoder
}

func (body *ProposalBody) UnmarshalJSON(b []byte) error {
	var u proposalBodyJSONUnmarshaler
	if err := util.UnmarshalJSON(b, &u); err != nil {
		return errors.Wrap(err, "failed to unmarshal proposalBody")
	}

	body.Proposal = u.Proposal.Hash()

	return nil
}
