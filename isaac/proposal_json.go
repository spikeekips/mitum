package isaac

import (
	"encoding/json"

	"github.com/spikeekips/mitum/base"
	"github.com/spikeekips/mitum/util"
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

type proposalSignedFactJSONMarshaler struct {
	Fact   base.ProposalFact `json:"fact"`
	Signed base.BaseSigned   `json:"signed"`
	hint.BaseHinter
}

type proposalSignedFactJSONUnmarshaler struct {
	Fact   json.RawMessage `json:"fact"`
	Signed json.RawMessage `json:"signed"`
	hint.BaseHinter
}

func (sf ProposalSignedFact) MarshalJSON() ([]byte, error) {
	return util.MarshalJSON(proposalSignedFactJSONMarshaler{
		BaseHinter: sf.BaseHinter,
		Fact:       sf.fact,
		Signed:     sf.signed,
	})
}

func (sf *ProposalSignedFact) DecodeJSON(b []byte, enc *jsonenc.Encoder) error {
	e := util.StringErrorFunc("failed to decode proposalSignedFact")

	var u proposalSignedFactJSONUnmarshaler
	if err := enc.Unmarshal(b, &u); err != nil {
		return e(err, "")
	}

	switch i, err := enc.Decode(u.Fact); {
	case err != nil:
		return e(err, "failed to decode fact")
	default:
		j, ok := i.(ProposalFact)
		if !ok {
			return e(err, "decoded fact not ProposalFact, %T", i)
		}

		sf.fact = j
	}

	if err := sf.signed.DecodeJSON(u.Signed, enc); err != nil {
		return e(err, "failed to decode signed")
	}

	return nil
}
