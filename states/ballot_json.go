package states

import (
	"encoding/json"

	"github.com/spikeekips/mitum/base"
	"github.com/spikeekips/mitum/util"
	jsonenc "github.com/spikeekips/mitum/util/encoder/json"
	"github.com/spikeekips/mitum/util/hint"
)

type baseBallotJSONMarshaler struct {
	hint.BaseHinter
	IVP base.INITVoteproof    `json:"init_voteproof"`
	AVP base.ACCEPTVoteproof  `json:"accept_voteproof"`
	SF  base.BallotSignedFact `json:"signed_fact"`
}

func (bl baseBallot) MarshalJSON() ([]byte, error) {
	return util.MarshalJSON(baseBallotJSONMarshaler{
		BaseHinter: bl.BaseHinter,
		IVP:        bl.ivp,
		AVP:        bl.avp,
		SF:         bl.signedFact,
	})
}

type baseBallotJSONUnmarshaler struct {
	IVP json.RawMessage `json:"init_voteproof"`
	AVP json.RawMessage `json:"accept_voteproof"`
	SF  json.RawMessage `json:"signed_fact"`
}

func (bl *baseBallot) DecodeJSON(b []byte, enc *jsonenc.Encoder) error {
	e := util.StringErrorFunc("failed to decode baseBallot")

	var u baseBallotJSONUnmarshaler
	if err := enc.Unmarshal(b, &u); err != nil {
		return e(err, "")
	}

	switch i, err := enc.Decode(u.IVP); {
	case err != nil:
		return e(err, "")
	case i == nil:
	default:
		vp, ok := i.(base.INITVoteproof)
		if !ok {
			return e(err, "decoded not INITVoteproof, %T", i)
		}

		bl.ivp = vp
	}

	switch i, err := enc.Decode(u.AVP); {
	case err != nil:
		return e(err, "")
	case i == nil:
	default:
		vp, ok := i.(base.ACCEPTVoteproof)
		if !ok {
			return e(err, "decoded not ACCEPTVoteproof, %T", i)
		}

		bl.avp = vp
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