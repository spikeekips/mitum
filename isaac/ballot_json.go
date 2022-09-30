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
	Withdraws []SuffrageWithdraw  `json:"withdraws,omitempty"`
	Voteproof base.Voteproof      `json:"voteproof,omitempty"`
	SignFact  base.BallotSignFact `json:"sign_fact"`
	hint.BaseHinter
}

func (bl baseBallot) MarshalJSON() ([]byte, error) {
	return util.MarshalJSON(baseBallotJSONMarshaler{
		BaseHinter: bl.BaseHinter,
		Voteproof:  bl.vp,
		SignFact:   bl.signFact,
		Withdraws:  bl.withdraws,
	})
}

type baseBallotJSONUnmarshaler struct {
	Voteproof json.RawMessage   `json:"voteproof"`
	SignFact  json.RawMessage   `json:"sign_fact"`
	Withdraws []json.RawMessage `json:"withdraws,omitempty"`
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

	if err := encoder.Decode(enc, u.SignFact, &bl.signFact); err != nil {
		return e(err, "")
	}

	bl.withdraws = make([]SuffrageWithdraw, len(u.Withdraws))
	for i := range u.Withdraws {
		if err := encoder.Decode(enc, u.Withdraws[i], &bl.withdraws[i]); err != nil {
			return e(err, "")
		}
	}

	return nil
}
