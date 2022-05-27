package isaac

import (
	"encoding/json"

	"github.com/spikeekips/mitum/base"
	"github.com/spikeekips/mitum/util"
	"github.com/spikeekips/mitum/util/encoder"
	jsonenc "github.com/spikeekips/mitum/util/encoder/json"
	"github.com/spikeekips/mitum/util/hint"
)

type baseBallotJSONMarshaler struct {
	Voteproof  base.Voteproof        `json:"voteproof,omitempty"`
	SignedFact base.BallotSignedFact `json:"signed_fact"`
	hint.BaseHinter
}

func (bl baseBallot) MarshalJSON() ([]byte, error) {
	return util.MarshalJSON(baseBallotJSONMarshaler{
		BaseHinter: bl.BaseHinter,
		Voteproof:  bl.vp,
		SignedFact: bl.signedFact,
	})
}

type baseBallotJSONUnmarshaler struct {
	Voteproof  json.RawMessage `json:"voteproof"`
	SignedFact json.RawMessage `json:"signed_fact"`
}

func (bl *baseBallot) DecodeJSON(b []byte, enc *jsonenc.Encoder) error {
	e := util.StringErrorFunc("failed to decode baseBallot")

	var u baseBallotJSONUnmarshaler
	if err := enc.Unmarshal(b, &u); err != nil {
		return e(err, "")
	}

	if err := encoder.Decode(enc, u.Voteproof, &bl.vp); err != nil {
		return e(err, "")
	}

	if err := encoder.Decode(enc, u.SignedFact, &bl.signedFact); err != nil {
		return e(err, "")
	}

	return nil
}
