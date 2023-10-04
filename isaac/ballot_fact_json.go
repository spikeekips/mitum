package isaac

import (
	"github.com/spikeekips/mitum/base"
	"github.com/spikeekips/mitum/util"
	jsonenc "github.com/spikeekips/mitum/util/encoder/json"
	"github.com/spikeekips/mitum/util/valuehash"
)

type baseBallotFactJSONMarshaler struct {
	ExpelFacts []util.Hash     `json:"expel_facts,omitempty"`
	Point      base.StagePoint `json:"point"`
	base.BaseFactJSONMarshaler
}

type baseBallotFactJSONUnmarshaler struct {
	ExpelFacts []valuehash.HashDecoder `json:"expel_facts"`
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
		ExpelFacts:            fact.expelfacts,
	}
}

func (fact *baseBallotFact) DecodeJSON(b []byte, enc *jsonenc.Encoder) error {
	e := util.StringError("decode baseBallotFact")

	var u baseBallotFactJSONUnmarshaler
	if err := enc.Unmarshal(b, &u); err != nil {
		return e.Wrap(err)
	}

	fact.BaseFact.SetJSONUnmarshaler(u.BaseFactJSONUnmarshaler)
	fact.point = u.Point

	if len(u.ExpelFacts) > 0 {
		fact.expelfacts = make([]util.Hash, len(u.ExpelFacts))

		for i := range u.ExpelFacts {
			fact.expelfacts[i] = u.ExpelFacts[i].Hash()
		}
	}

	return nil
}

func (fact INITBallotFact) jsonMarshaler() INITBallotFactJSONMarshaler {
	return INITBallotFactJSONMarshaler{
		baseBallotFactJSONMarshaler: fact.baseBallotFact.jsonMarshaler(),
		PreviousBlock:               fact.previousBlock,
		Proposal:                    fact.proposal,
	}
}

func (fact INITBallotFact) MarshalJSON() ([]byte, error) {
	return util.MarshalJSON(fact.jsonMarshaler())
}

func (fact *INITBallotFact) DecodeJSON(b []byte, enc *jsonenc.Encoder) error {
	e := util.StringError("decode INITBallotFact")

	var ub baseBallotFact
	if err := ub.DecodeJSON(b, enc); err != nil {
		return e.Wrap(err)
	}

	fact.baseBallotFact = ub

	var u INITBallotFactJSONUnmarshaler
	if err := enc.Unmarshal(b, &u); err != nil {
		return e.Wrap(err)
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
	e := util.StringError("decode ACCEPTBallotFact")

	var ub baseBallotFact
	if err := ub.DecodeJSON(b, enc); err != nil {
		return e.Wrap(err)
	}

	fact.baseBallotFact = ub

	var u ACCEPTBallotFactJSONUnmarshaler
	if err := enc.Unmarshal(b, &u); err != nil {
		return e.Wrap(err)
	}

	fact.proposal = u.Proposal.Hash()
	fact.newBlock = u.NewBlock.Hash()

	return nil
}

type EmptyProposalINITBallotFactJSONMarshaler struct {
	R string `json:"r"`
	INITBallotFactJSONMarshaler
}

func (fact EmptyProposalINITBallotFact) MarshalJSON() ([]byte, error) {
	return util.MarshalJSON(EmptyProposalINITBallotFactJSONMarshaler{
		INITBallotFactJSONMarshaler: fact.INITBallotFact.jsonMarshaler(),
		R:                           fact.r,
	})
}

func (fact *EmptyProposalINITBallotFact) DecodeJSON(b []byte, enc *jsonenc.Encoder) error {
	e := util.StringError("decode EmptyProposalINITBallotFact")

	if err := fact.INITBallotFact.DecodeJSON(b, enc); err != nil {
		return e.Wrap(err)
	}

	var u struct {
		R string `json:"r"`
	}

	if err := enc.Unmarshal(b, &u); err != nil {
		return e.Wrap(err)
	}

	fact.r = u.R

	return nil
}
