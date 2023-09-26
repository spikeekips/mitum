package isaac

import (
	"encoding/json"

	"github.com/spikeekips/mitum/base"
	"github.com/spikeekips/mitum/util"
	"github.com/spikeekips/mitum/util/encoder"
	jsonenc "github.com/spikeekips/mitum/util/encoder/json"
	"github.com/spikeekips/mitum/util/hint"
	"github.com/spikeekips/mitum/util/localtime"
	"github.com/spikeekips/mitum/util/valuehash"
)

type proposalFactJSONMarshaler struct {
	ProposedAt    localtime.Time `json:"proposed_at"`
	Proposer      base.Address   `json:"proposer"`
	PreviousBlock util.Hash      `json:"previous_block"`
	Operations    [][2]util.Hash `json:"operations"`
	base.BaseFactJSONMarshaler
	Point base.Point `json:"point"`
}

type proposalFactJSONUnmarshaler struct {
	ProposedAt    localtime.Time        `json:"proposed_at"`
	Proposer      string                `json:"proposer"`
	PreviousBlock valuehash.HashDecoder `json:"previous_block"`
	base.BaseFactJSONUnmarshaler
	Operations [][2]valuehash.HashDecoder `json:"operations"`
	Point      base.Point                 `json:"point"`
}

func (fact ProposalFact) MarshalJSON() ([]byte, error) {
	return util.MarshalJSON(proposalFactJSONMarshaler{
		BaseFactJSONMarshaler: fact.BaseFact.JSONMarshaler(),
		Point:                 fact.point,
		Proposer:              fact.proposer,
		Operations:            fact.operations,
		ProposedAt:            localtime.New(fact.proposedAt),
		PreviousBlock:         fact.previousBlock,
	})
}

func (fact *ProposalFact) DecodeJSON(b []byte, enc *jsonenc.Encoder) error {
	e := util.StringError("decode ProposalFact")

	var u proposalFactJSONUnmarshaler
	if err := enc.Unmarshal(b, &u); err != nil {
		return e.Wrap(err)
	}

	fact.BaseFact.SetJSONUnmarshaler(u.BaseFactJSONUnmarshaler)
	fact.point = u.Point

	switch i, err := base.DecodeAddress(u.Proposer, enc); {
	case err != nil:
		return e.WithMessage(err, "decode proposer address")
	default:
		fact.proposer = i
	}

	hs := make([][2]util.Hash, len(u.Operations))

	for i := range u.Operations {
		hs[i][0] = u.Operations[i][0].Hash()
		hs[i][1] = u.Operations[i][1].Hash()
	}

	fact.operations = hs
	fact.proposedAt = u.ProposedAt.Time
	fact.previousBlock = u.PreviousBlock.Hash()

	return nil
}

type proposalSignFactJSONMarshaler struct {
	Fact base.ProposalFact `json:"fact"`
	Sign base.BaseSign     `json:"sign"`
	hint.BaseHinter
}

type proposalSignFactJSONUnmarshaler struct {
	Fact json.RawMessage `json:"fact"`
	Sign json.RawMessage `json:"sign"`
	hint.BaseHinter
}

func (sf ProposalSignFact) MarshalJSON() ([]byte, error) {
	return util.MarshalJSON(proposalSignFactJSONMarshaler{
		BaseHinter: sf.BaseHinter,
		Fact:       sf.fact,
		Sign:       sf.sign,
	})
}

func (sf *ProposalSignFact) DecodeJSON(b []byte, enc *jsonenc.Encoder) error {
	e := util.StringError("decode proposalSignFact")

	var u proposalSignFactJSONUnmarshaler
	if err := enc.Unmarshal(b, &u); err != nil {
		return e.Wrap(err)
	}

	if err := encoder.Decode(enc, u.Fact, &sf.fact); err != nil {
		return e.WithMessage(err, "decode fact")
	}

	if err := sf.sign.DecodeJSON(u.Sign, enc); err != nil {
		return e.WithMessage(err, "decode sign")
	}

	return nil
}
