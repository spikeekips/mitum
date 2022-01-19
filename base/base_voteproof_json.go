package base

import (
	"encoding/json"
	"time"

	"github.com/spikeekips/mitum/util"
	jsonenc "github.com/spikeekips/mitum/util/encoder/json"
	"github.com/spikeekips/mitum/util/hint"
	"github.com/spikeekips/mitum/util/localtime"
)

type baseVoteproofJSONMarshaler struct {
	hint.BaseHinter
	FinishedAt time.Time          `json:"finished_at"`
	Majority   BallotFact         `json:"majority"`
	Point      Point              `json:"point"`
	Result     VoteResult         `json:"result"`
	Stage      Stage              `json:"stage"`
	Suffrage   SuffrageInfo       `json:"suffrage"`
	Votes      []BallotSignedFact `json:"votes"`
	ID         string             `json:"id"`
}

func (vp BaseVoteproof) MarshalJSON() ([]byte, error) {
	return util.MarshalJSON(baseVoteproofJSONMarshaler{
		BaseHinter: vp.BaseHinter,
		FinishedAt: vp.finishedAt,
		Majority:   vp.majority,
		Point:      vp.point,
		Result:     vp.result,
		Stage:      vp.stage,
		Suffrage:   vp.suffrage,
		Votes:      vp.sfs,
		ID:         vp.id,
	})
}

type baseVoteproofJSONUnmarshaler struct {
	FinishedAt localtime.Time    `json:"finished_at"`
	Majority   json.RawMessage   `json:"majority"`
	Point      Point             `json:"point"`
	Result     VoteResult        `json:"result"`
	Stage      Stage             `json:"stage"`
	Suffrage   json.RawMessage   `json:"suffrage"`
	Votes      []json.RawMessage `json:"votes"`
	ID         string            `json:"id"`
}

func (vp *BaseVoteproof) DecodeJSON(b []byte, enc *jsonenc.Encoder) error {
	e := util.StringErrorFunc("failed to decode BaseVoteproof")

	var u baseVoteproofJSONUnmarshaler
	if err := enc.Unmarshal(b, &u); err != nil {
		return e(err, "")
	}

	switch i, err := enc.Decode(u.Majority); {
	case err != nil:
	case i == nil:
	default:
		j, ok := i.(BallotFact)
		if !ok {
			return e(util.InvalidError.Errorf("expected BallotFact, not %T", i), "")
		}
		vp.majority = j
	}

	// BLOCK decode SuffrageInfo

	vp.sfs = make([]BallotSignedFact, len(u.Votes))
	for i := range u.Votes {
		switch j, err := enc.Decode(u.Votes[i]); {
		case err != nil:
		case j == nil:
		default:
			k, ok := j.(BallotSignedFact)
			if !ok {
				return e(util.InvalidError.Errorf("expected BallotSignedFact, not %T", j), "")
			}

			vp.sfs[i] = k
		}
	}

	vp.finishedAt = u.FinishedAt.Time
	vp.point = u.Point
	vp.result = u.Result
	vp.stage = u.Stage
	vp.id = u.ID

	return nil
}
