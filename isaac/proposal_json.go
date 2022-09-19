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
	ProposedAt localtime.Time `json:"proposed_at"`
	Proposer   base.Address   `json:"proposer"`
	base.BaseFactJSONMarshaler
	Operations []util.Hash `json:"operations"`
	Point      base.Point  `json:"point"`
}

type proposalFactJSONUnmarshaler struct {
	ProposedAt localtime.Time `json:"proposed_at"`
	Proposer   string         `json:"proposer"`
	base.BaseFactJSONUnmarshaler
	Operations []valuehash.HashDecoder `json:"operations"`
	Point      base.Point              `json:"point"`
}

func (fact ProposalFact) MarshalJSON() ([]byte, error) {
	return util.MarshalJSON(proposalFactJSONMarshaler{
		BaseFactJSONMarshaler: fact.BaseFact.JSONMarshaler(),
		Point:                 fact.point,
		Proposer:              fact.proposer,
		Operations:            fact.operations,
		ProposedAt:            localtime.New(fact.proposedAt),
	})
}

func (fact *ProposalFact) DecodeJSON(b []byte, enc *jsonenc.Encoder) error {
	e := util.StringErrorFunc("failed to decode ProposalFact")

	var u proposalFactJSONUnmarshaler
	if err := enc.Unmarshal(b, &u); err != nil {
		return e(err, "")
	}

	fact.BaseFact.SetJSONUnmarshaler(u.BaseFactJSONUnmarshaler)
	fact.point = u.Point

	switch i, err := base.DecodeAddress(u.Proposer, enc); {
	case err != nil:
		return e(err, "failed to decode proposer address")
	default:
		fact.proposer = i
	}

	hs := make([]util.Hash, len(u.Operations))

	for i := range u.Operations {
		hs[i] = u.Operations[i].Hash()
	}

	fact.operations = hs
	fact.proposedAt = u.ProposedAt.Time

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
	e := util.StringErrorFunc("failed to decode proposalSignFact")

	var u proposalSignFactJSONUnmarshaler
	if err := enc.Unmarshal(b, &u); err != nil {
		return e(err, "")
	}

	if err := encoder.Decode(enc, u.Fact, &sf.fact); err != nil {
		return e(err, "failed to decode fact")
	}

	if err := sf.sign.DecodeJSON(u.Sign, enc); err != nil {
		return e(err, "failed to decode sign")
	}

	return nil
}
