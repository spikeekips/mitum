package base

import (
	"encoding/json"

	"github.com/spikeekips/mitum/util"
	jsonenc "github.com/spikeekips/mitum/util/encoder/json"
	"github.com/spikeekips/mitum/util/hint"
	"github.com/spikeekips/mitum/util/localtime"
	"github.com/spikeekips/mitum/util/valuehash"
)

type baseBallotSignedFactJSONMarshaler struct {
	hint.BaseHinter
	Fact   BallotFact `json:"fact"`
	Node   Address    `json:"node"`
	Signed BaseSigned `json:"signed"`
}

type baseBallotSignedFactJSONUnmarshaler struct {
	hint.BaseHinter
	Fact   json.RawMessage `json:"fact"`
	Node   string          `json:"node"`
	Signed json.RawMessage `json:"signed"`
}

type baseBallotFactJSONMarshaler struct {
	hint.BaseHinter
	H     util.Hash `json:"hash"`
	Stage Stage     `json:"stage"`
	Point Point     `json:"point"`
}

type baseBallotFactJSONUnmarshaler struct {
	hint.BaseHinter
	H     valuehash.Bytes `json:"hash"`
	Stage Stage           `json:"stage"`
	Point Point           `json:"point"`
}

type baseINITBallotFactJSONMarshaler struct {
	baseBallotFactJSONMarshaler
	PreviousBlock util.Hash `json:"previous_block"`
}

type baseINITBallotFactJSONUnmarshaler struct {
	baseBallotFactJSONUnmarshaler
	PreviousBlock valuehash.Bytes `json:"previous_block"`
}

type baseProposalFactJSONMarshaler struct {
	baseBallotFactJSONMarshaler
	Operations []util.Hash    `json:"operations"`
	ProposedAt localtime.Time `json:"proposed_at"`
}

type baseProposalFactJSONUnmarshaler struct {
	baseBallotFactJSONUnmarshaler
	Operations []valuehash.Bytes `json:"operations"`
	ProposedAt localtime.Time    `json:"proposed_at"`
}

type baseACCEPTBallotFactJSONMarshaler struct {
	baseBallotFactJSONMarshaler
	Proposal util.Hash `json:"proposal"`
	NewBlock util.Hash `json:"new_block"`
}

type baseACCEPTBallotFactJSONUnmarshaler struct {
	baseBallotFactJSONUnmarshaler
	Proposal valuehash.Bytes `json:"proposal"`
	NewBlock valuehash.Bytes `json:"new_block"`
}

func (sf BaseBallotSignedFact) MarshalJSON() ([]byte, error) {
	return util.MarshalJSON(baseBallotSignedFactJSONMarshaler{
		BaseHinter: sf.BaseHinter,
		Fact:       sf.fact,
		Node:       sf.node,
		Signed:     sf.signed,
	})
}

func (sf *BaseBallotSignedFact) DecodeJSON(b []byte, enc *jsonenc.Encoder) error {
	e := util.StringErrorFunc("failed to decode BaseBallotSignedFact")

	var u baseBallotSignedFactJSONUnmarshaler
	if err := enc.Unmarshal(b, &u); err != nil {
		return e(err, "")
	}

	switch i, err := enc.Decode(u.Fact); {
	case err != nil:
		return e(err, "failed to decode fact")
	default:
		j, ok := i.(BallotFact)
		if !ok {
			return e(err, "decoded fact not BallotFact, %T", i)
		}

		sf.fact = j
	}

	switch i, err := DecodeAddressFromString(u.Node, enc); {
	case err != nil:
		return e(err, "failed to decode address")
	default:
		sf.node = i
	}

	if err := sf.signed.DecodeJSON(u.Signed, enc); err != nil {
		return e(err, "failed to decode signed")
	}

	return nil
}
