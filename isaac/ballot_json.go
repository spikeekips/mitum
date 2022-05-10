package isaac

import (
	"encoding/json"

	"github.com/spikeekips/mitum/base"
	"github.com/spikeekips/mitum/util"
	jsonenc "github.com/spikeekips/mitum/util/encoder/json"
	"github.com/spikeekips/mitum/util/hint"
)

type baseBallotJSONMarshaler struct {
	VP base.Voteproof        `json:"voteproof,omitempty"`
	SF base.BallotSignedFact `json:"signed_fact"`
	hint.BaseHinter
}

func (bl baseBallot) MarshalJSON() ([]byte, error) {
	return util.MarshalJSON(baseBallotJSONMarshaler{
		BaseHinter: bl.BaseHinter,
		VP:         bl.vp,
		SF:         bl.signedFact,
	})
}

type baseBallotJSONUnmarshaler struct {
	VP json.RawMessage `json:"voteproof"`
	SF json.RawMessage `json:"signed_fact"`
}

func (bl *baseBallot) DecodeJSON(b []byte, enc *jsonenc.Encoder) error {
	e := util.StringErrorFunc("failed to decode baseBallot")

	var u baseBallotJSONUnmarshaler
	if err := enc.Unmarshal(b, &u); err != nil {
		return e(err, "")
	}

	switch i, err := enc.Decode(u.VP); {
	case err != nil:
		return e(err, "")
	case i == nil:
	default:
		vp, ok := i.(base.Voteproof)
		if !ok {
			return e(err, "decoded not Voteproof, %T", i)
		}

		bl.vp = vp
	}

	switch i, err := enc.Decode(u.SF); {
	case err != nil:
		return e(err, "")
	case i == nil:
	default:
		sf, ok := i.(base.BallotSignedFact)
		if !ok {
			return e(err, "decoded not BallotSignedFact, %T", i)
		}

		bl.signedFact = sf
	}

	return nil
}
