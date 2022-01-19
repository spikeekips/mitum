package base

import (
	"github.com/spikeekips/mitum/util"
	jsonenc "github.com/spikeekips/mitum/util/encoder/json"
	"github.com/spikeekips/mitum/util/localtime"
)

func (fact BaseBallotFact) jsonMarshaler() baseBallotFactJSONMarshaler {
	return baseBallotFactJSONMarshaler{
		BaseHinter: fact.BaseHinter,
		H:          fact.h,
		Stage:      fact.stage,
		Point:      fact.point,
	}
}

func (fact *BaseBallotFact) DecodeJSON(b []byte, enc *jsonenc.Encoder) error {
	e := util.StringErrorFunc("failed to decode BaseBallotFact")

	var u baseBallotFactJSONUnmarshaler
	if err := enc.Unmarshal(b, &u); err != nil {
		return e(err, "")
	}

	fact.h = u.H
	fact.stage = u.Stage
	fact.point = u.Point

	return nil
}

func (fact BaseINITBallotFact) MarshalJSON() ([]byte, error) {
	return util.MarshalJSON(baseINITBallotFactJSONMarshaler{
		baseBallotFactJSONMarshaler: fact.jsonMarshaler(),
		PreviousBlock:               fact.previousBlock,
	})
}

func (fact *BaseINITBallotFact) DecodeJSON(b []byte, enc *jsonenc.Encoder) error {
	e := util.StringErrorFunc("failed to decode BaseINITBallotFact")

	var ub BaseBallotFact
	if err := ub.DecodeJSON(b, enc); err != nil {
		return e(err, "")
	}
	fact.BaseBallotFact = ub

	var u baseINITBallotFactJSONUnmarshaler
	if err := enc.Unmarshal(b, &u); err != nil {
		return e(err, "")
	}

	fact.previousBlock = u.PreviousBlock

	return nil
}

func (fact BaseProposalFact) MarshalJSON() ([]byte, error) {
	return util.MarshalJSON(baseProposalFactJSONMarshaler{
		baseBallotFactJSONMarshaler: fact.jsonMarshaler(),
		Operations:                  fact.operations,
		ProposedAt:                  localtime.NewTime(fact.proposedAt),
	})
}

func (fact *BaseProposalFact) DecodeJSON(b []byte, enc *jsonenc.Encoder) error {
	e := util.StringErrorFunc("failed to decode BaseProposalFact")

	var ub BaseBallotFact
	if err := ub.DecodeJSON(b, enc); err != nil {
		return e(err, "")
	}
	fact.BaseBallotFact = ub

	var u baseProposalFactJSONUnmarshaler
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

func (fact BaseACCEPTBallotFact) MarshalJSON() ([]byte, error) {
	return util.MarshalJSON(baseACCEPTBallotFactJSONMarshaler{
		baseBallotFactJSONMarshaler: fact.jsonMarshaler(),
		Proposal:                    fact.proposal,
		NewBlock:                    fact.newBlock,
	})
}

func (fact *BaseACCEPTBallotFact) DecodeJSON(b []byte, enc *jsonenc.Encoder) error {
	e := util.StringErrorFunc("failed to decode BaseACCEPTBallotFact")

	var ub BaseBallotFact
	if err := ub.DecodeJSON(b, enc); err != nil {
		return e(err, "")
	}
	fact.BaseBallotFact = ub

	var u baseACCEPTBallotFactJSONUnmarshaler
	if err := enc.Unmarshal(b, &u); err != nil {
		return e(err, "")
	}

	fact.proposal = u.Proposal
	fact.newBlock = u.NewBlock

	return nil
}
