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
	Expels    []base.SuffrageExpelOperation `json:"expels,omitempty"`
	Voteproof base.Voteproof                `json:"voteproof,omitempty"`
	SignFact  base.BallotSignFact           `json:"sign_fact"`
	hint.BaseHinter
}

func (bl baseBallot) MarshalJSON() ([]byte, error) {
	return util.MarshalJSON(baseBallotJSONMarshaler{
		BaseHinter: bl.BaseHinter,
		Voteproof:  bl.vp,
		SignFact:   bl.signFact,
		Expels:     bl.expels,
	})
}

type baseBallotJSONUnmarshaler struct {
	Voteproof json.RawMessage   `json:"voteproof"`
	SignFact  json.RawMessage   `json:"sign_fact"`
	Expels    []json.RawMessage `json:"expels,omitempty"`
}

func (bl *baseBallot) DecodeJSON(b []byte, enc *jsonenc.Encoder) error {
	e := util.StringErrorFunc("decode baseBallot")

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

	bl.expels = make([]base.SuffrageExpelOperation, len(u.Expels))
	for i := range u.Expels {
		if err := encoder.Decode(enc, u.Expels[i], &bl.expels[i]); err != nil {
			return e(err, "")
		}
	}

	return nil
}
