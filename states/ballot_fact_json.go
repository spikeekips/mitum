package states

import (
	"github.com/spikeekips/mitum/base"
	"github.com/spikeekips/mitum/util"
	jsonenc "github.com/spikeekips/mitum/util/encoder/json"
	"github.com/spikeekips/mitum/util/hint"
	"github.com/spikeekips/mitum/util/localtime"
	"github.com/spikeekips/mitum/util/valuehash"
)

type baseBallotFactJSONMarshaler struct {
	hint.BaseHinter
	H     util.Hash       `json:"hash"`
	Stage base.Stage      `json:"stage"`
	Point base.StagePoint `json:"point"`
}

type baseBallotFactJSONUnmarshaler struct {
	hint.BaseHinter
	H     valuehash.Bytes `json:"hash"`
	Stage base.Stage      `json:"stage"`
	Point base.StagePoint `json:"point"`
}

type INITBallotFactJSONMarshaler struct {
	baseBallotFactJSONMarshaler
	PreviousBlock util.Hash `json:"previous_block"`
}

type INITBallotFactJSONUnmarshaler struct {
	baseBallotFactJSONUnmarshaler
	PreviousBlock valuehash.Bytes `json:"previous_block"`
}

type ProposalFactJSONMarshaler struct {
	baseBallotFactJSONMarshaler
	Operations []util.Hash    `json:"operations"`
	ProposedAt localtime.Time `json:"proposed_at"`
}

type ProposalFactJSONUnmarshaler struct {
	baseBallotFactJSONUnmarshaler
	Operations []valuehash.Bytes `json:"operations"`
	ProposedAt localtime.Time    `json:"proposed_at"`
}

type ACCEPTBallotFactJSONMarshaler struct {
	baseBallotFactJSONMarshaler
	Proposal util.Hash `json:"proposal"`
	NewBlock util.Hash `json:"new_block"`
}

type ACCEPTBallotFactJSONUnmarshaler struct {
	baseBallotFactJSONUnmarshaler
	Proposal valuehash.Bytes `json:"proposal"`
	NewBlock valuehash.Bytes `json:"new_block"`
}

func (fact baseBallotFact) jsonMarshaler() baseBallotFactJSONMarshaler {
	return baseBallotFactJSONMarshaler{
		BaseHinter: fact.BaseHinter,
		H:          fact.h,
		Point:      fact.point,
	}
}

func (fact *baseBallotFact) DecodeJSON(b []byte, enc *jsonenc.Encoder) error {
	e := util.StringErrorFunc("failed to decode baseBallotFact")

	var u baseBallotFactJSONUnmarshaler
	if err := enc.Unmarshal(b, &u); err != nil {
		return e(err, "")
	}

	fact.h = u.H
	fact.point = u.Point

	return nil
}

func (fact INITBallotFact) MarshalJSON() ([]byte, error) {
	return util.MarshalJSON(INITBallotFactJSONMarshaler{
		baseBallotFactJSONMarshaler: fact.jsonMarshaler(),
		PreviousBlock:               fact.previousBlock,
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

	fact.previousBlock = u.PreviousBlock

	return nil
}

func (fact ProposalFact) MarshalJSON() ([]byte, error) {
	return util.MarshalJSON(ProposalFactJSONMarshaler{
		baseBallotFactJSONMarshaler: fact.jsonMarshaler(),
		Operations:                  fact.operations,
		ProposedAt:                  localtime.NewTime(fact.proposedAt),
	})
}

func (fact *ProposalFact) DecodeJSON(b []byte, enc *jsonenc.Encoder) error {
	e := util.StringErrorFunc("failed to decode ProposalFact")

	var ub baseBallotFact
	if err := ub.DecodeJSON(b, enc); err != nil {
		return e(err, "")
	}
	fact.baseBallotFact = ub

	var u ProposalFactJSONUnmarshaler
	if err := enc.Unmarshal(b, &u); err != nil {
		return e(err, "")
	}

	hs := make([]util.Hash, len(u.Operations))
	for i := range u.Operations {
		hs[i] = u.Operations[i]
	}

	fact.operations = hs
	fact.proposedAt = u.ProposedAt.Time

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

	fact.proposal = u.Proposal
	fact.newBlock = u.NewBlock

	return nil
}
