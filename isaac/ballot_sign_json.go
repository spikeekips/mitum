package isaac

import (
	"encoding/json"

	"github.com/spikeekips/mitum/base"
	"github.com/spikeekips/mitum/util"
	"github.com/spikeekips/mitum/util/encoder"
	"github.com/spikeekips/mitum/util/hint"
)

type baseBallotSignFactJSONMarshaler struct {
	Fact base.BallotFact   `json:"fact"`
	Sign base.BaseNodeSign `json:"sign"`
	hint.BaseHinter
}

type baseBallotSignFactJSONUnmarshaler struct {
	Fact json.RawMessage `json:"fact"`
	Sign json.RawMessage `json:"sign"`
	hint.BaseHinter
}

func (sf baseBallotSignFact) MarshalJSON() ([]byte, error) {
	return util.MarshalJSON(baseBallotSignFactJSONMarshaler{
		BaseHinter: sf.BaseHinter,
		Fact:       sf.fact,
		Sign:       sf.sign,
	})
}

func (sf *baseBallotSignFact) DecodeJSON(b []byte, enc encoder.Encoder) error {
	e := util.StringError("decode BaseBallotSignFact")

	var u baseBallotSignFactJSONUnmarshaler
	if err := enc.Unmarshal(b, &u); err != nil {
		return e.Wrap(err)
	}

	if err := encoder.Decode(enc, u.Fact, &sf.fact); err != nil {
		return e.WithMessage(err, "decode fact")
	}

	if err := sf.sign.DecodeJSON(u.Sign, enc); err != nil {
		return e.WithMessage(err, "decode sign")
	}

	return nil
}
