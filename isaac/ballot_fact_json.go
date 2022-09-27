package isaac

import (
	"encoding/json"

	"github.com/spikeekips/mitum/base"
	"github.com/spikeekips/mitum/util"
	"github.com/spikeekips/mitum/util/encoder"
	jsonenc "github.com/spikeekips/mitum/util/encoder/json"
	"github.com/spikeekips/mitum/util/valuehash"
)

type baseBallotFactJSONMarshaler struct {
	base.BaseFactJSONMarshaler
	Point base.StagePoint `json:"point"`
}

type baseBallotFactJSONUnmarshaler struct {
	base.BaseFactJSONUnmarshaler
	Point base.StagePoint `json:"point"`
}

type INITBallotFactJSONMarshaler struct {
	PreviousBlock util.Hash              `json:"previous_block"`
	Proposal      util.Hash              `json:"proposal"`
	WithdrawFacts []SuffrageWithdrawFact `json:"withdraw_facts"`
	baseBallotFactJSONMarshaler
}

type INITBallotFactJSONUnmarshaler struct {
	PreviousBlock valuehash.HashDecoder `json:"previous_block"`
	Proposal      valuehash.HashDecoder `json:"proposal"`
	WithdrawFacts []json.RawMessage     `json:"withdraw_facts"`
	baseBallotFactJSONUnmarshaler
}

type ACCEPTBallotFactJSONMarshaler struct {
	Proposal util.Hash `json:"proposal"`
	NewBlock util.Hash `json:"new_block"`
	baseBallotFactJSONMarshaler
}

type ACCEPTBallotFactJSONUnmarshaler struct {
	Proposal valuehash.HashDecoder `json:"proposal"`
	NewBlock valuehash.HashDecoder `json:"new_block"`
	baseBallotFactJSONUnmarshaler
}

func (fact baseBallotFact) jsonMarshaler() baseBallotFactJSONMarshaler {
	return baseBallotFactJSONMarshaler{
		BaseFactJSONMarshaler: fact.BaseFact.JSONMarshaler(),
		Point:                 fact.point,
	}
}

func (fact *baseBallotFact) DecodeJSON(b []byte, enc *jsonenc.Encoder) error {
	e := util.StringErrorFunc("failed to decode baseBallotFact")

	var u baseBallotFactJSONUnmarshaler
	if err := enc.Unmarshal(b, &u); err != nil {
		return e(err, "")
	}

	fact.BaseFact.SetJSONUnmarshaler(u.BaseFactJSONUnmarshaler)
	fact.point = u.Point

	return nil
}

func (fact INITBallotFact) MarshalJSON() ([]byte, error) {
	return util.MarshalJSON(INITBallotFactJSONMarshaler{
		baseBallotFactJSONMarshaler: fact.jsonMarshaler(),
		PreviousBlock:               fact.previousBlock,
		Proposal:                    fact.proposal,
		WithdrawFacts:               fact.withdrawfacts,
	})
}

func (fact *INITBallotFact) DecodeJSON(b []byte, enc *jsonenc.Encoder) error {
	e := util.StringErrorFunc("failed to decode INITBallotFact")

	var ub baseBallotFact
	if err := ub.DecodeJSON(b, enc); err != nil {
		return e(err, "")
	}

	fact.baseBallotFact = ub

	var u INITBallotFactJSONUnmarshaler
	if err := enc.Unmarshal(b, &u); err != nil {
		return e(err, "")
	}

	fact.previousBlock = u.PreviousBlock.Hash()
	fact.proposal = u.Proposal.Hash()

	if len(u.WithdrawFacts) > 0 {
		fact.withdrawfacts = make([]SuffrageWithdrawFact, len(u.WithdrawFacts))

		for i := range u.WithdrawFacts {
			if err := encoder.Decode(enc, u.WithdrawFacts[i], &fact.withdrawfacts[i]); err != nil {
				return e(err, "")
			}
		}
	}

	return nil
}

func (fact ACCEPTBallotFact) MarshalJSON() ([]byte, error) {
	return util.MarshalJSON(ACCEPTBallotFactJSONMarshaler{
		baseBallotFactJSONMarshaler: fact.jsonMarshaler(),
		Proposal:                    fact.proposal,
		NewBlock:                    fact.newBlock,
	})
}

func (fact *ACCEPTBallotFact) DecodeJSON(b []byte, enc *jsonenc.Encoder) error {
	e := util.StringErrorFunc("failed to decode ACCEPTBallotFact")

	var ub baseBallotFact
	if err := ub.DecodeJSON(b, enc); err != nil {
		return e(err, "")
	}

	fact.baseBallotFact = ub

	var u ACCEPTBallotFactJSONUnmarshaler
	if err := enc.Unmarshal(b, &u); err != nil {
		return e(err, "")
	}

	fact.proposal = u.Proposal.Hash()
	fact.newBlock = u.NewBlock.Hash()

	return nil
}
