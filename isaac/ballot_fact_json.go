package isaac

import (
	"github.com/spikeekips/mitum/base"
	"github.com/spikeekips/mitum/util"
	jsonenc "github.com/spikeekips/mitum/util/encoder/json"
	"github.com/spikeekips/mitum/util/valuehash"
)

type baseBallotFactJSONMarshaler struct {
	WithdrawFacts []util.Hash     `json:"withdraw_facts,omitempty"`
	Point         base.StagePoint `json:"point"`
	base.BaseFactJSONMarshaler
}

type baseBallotFactJSONUnmarshaler struct {
	WithdrawFacts []valuehash.HashDecoder `json:"withdraw_facts"`
	base.BaseFactJSONUnmarshaler
	Point base.StagePoint `json:"point"`
}

type INITBallotFactJSONMarshaler struct {
	PreviousBlock util.Hash `json:"previous_block"`
	Proposal      util.Hash `json:"proposal"`
	baseBallotFactJSONMarshaler
}

type INITBallotFactJSONUnmarshaler struct {
	PreviousBlock valuehash.HashDecoder `json:"previous_block"`
	Proposal      valuehash.HashDecoder `json:"proposal"`
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
		WithdrawFacts:         fact.withdrawfacts,
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

	if len(u.WithdrawFacts) > 0 {
		fact.withdrawfacts = make([]util.Hash, len(u.WithdrawFacts))

		for i := range u.WithdrawFacts {
			fact.withdrawfacts[i] = u.WithdrawFacts[i].Hash()
		}
	}

	return nil
}

func (fact INITBallotFact) MarshalJSON() ([]byte, error) {
	return util.MarshalJSON(INITBallotFactJSONMarshaler{
		baseBallotFactJSONMarshaler: fact.jsonMarshaler(),
		PreviousBlock:               fact.previousBlock,
		Proposal:                    fact.proposal,
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
