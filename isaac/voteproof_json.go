package isaac

import (
	"encoding/json"
	"time"

	"github.com/spikeekips/mitum/base"
	"github.com/spikeekips/mitum/util"
	jsonenc "github.com/spikeekips/mitum/util/encoder/json"
	"github.com/spikeekips/mitum/util/hint"
	"github.com/spikeekips/mitum/util/localtime"
)

type baseVoteproofJSONMarshaler struct {
	FinishedAt time.Time       `json:"finished_at"`
	Majority   base.BallotFact `json:"majority"`
	hint.BaseHinter
	Result      base.VoteResult         `json:"result"`
	ID          string                  `json:"id"`
	SignedFacts []base.BallotSignedFact `json:"signed_facts"`
	Point       base.StagePoint         `json:"point"`
	Threshold   base.Threshold          `json:"threshold"`
}

func (vp baseVoteproof) MarshalJSON() ([]byte, error) {
	return util.MarshalJSON(baseVoteproofJSONMarshaler{
		BaseHinter:  vp.BaseHinter,
		FinishedAt:  vp.finishedAt,
		Majority:    vp.majority,
		Point:       vp.point,
		Result:      vp.result,
		Threshold:   vp.threshold,
		SignedFacts: vp.sfs,
		ID:          vp.id,
	})
}

type baseVoteproofJSONUnmarshaler struct {
	FinishedAt  localtime.Time    `json:"finished_at"`
	Result      base.VoteResult   `json:"result"`
	ID          string            `json:"id"`
	Majority    json.RawMessage   `json:"majority"`
	SignedFacts []json.RawMessage `json:"signed_facts"`
	Point       base.StagePoint   `json:"point"`
	Threshold   base.Threshold    `json:"threshold"`
}

func (vp *baseVoteproof) DecodeJSON(b []byte, enc *jsonenc.Encoder) error {
	e := util.StringErrorFunc("failed to decode baseVoteproof")

	var u baseVoteproofJSONUnmarshaler
	if err := enc.Unmarshal(b, &u); err != nil {
		return e(err, "")
	}

	switch i, err := enc.Decode(u.Majority); {
	case err != nil:
	case i == nil:
	default:
		j, ok := i.(base.BallotFact)
		if !ok {
			return e(util.InvalidError.Errorf("expected BallotFact, not %T", i), "")
		}
		vp.majority = j
	}

	vp.threshold = u.Threshold

	vp.sfs = make([]base.BallotSignedFact, len(u.SignedFacts))
	for i := range u.SignedFacts {
		switch j, err := enc.Decode(u.SignedFacts[i]); {
		case err != nil:
			return e(err, "")
		case j == nil:
		default:
			k, ok := j.(base.BallotSignedFact)
			if !ok {
				return e(util.InvalidError.Errorf("expected BallotSignedFact, not %T", j), "")
			}

			vp.sfs[i] = k
		}
	}

	vp.finishedAt = u.FinishedAt.Time
	vp.point = u.Point
	vp.result = u.Result
	vp.id = u.ID

	return nil
}
