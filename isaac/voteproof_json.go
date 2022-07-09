package isaac

import (
	"encoding/json"
	"time"

	"github.com/spikeekips/mitum/base"
	"github.com/spikeekips/mitum/util"
	"github.com/spikeekips/mitum/util/encoder"
	jsonenc "github.com/spikeekips/mitum/util/encoder/json"
	"github.com/spikeekips/mitum/util/hint"
	"github.com/spikeekips/mitum/util/localtime"
	"github.com/spikeekips/mitum/util/valuehash"
)

type baseVoteproofJSONMarshaler struct {
	FinishedAt time.Time `json:"finished_at"`
	Majority   util.Hash `json:"majority"` // NOTE fact hash of majority BallotSignedFact
	hint.BaseHinter
	Result      base.VoteResult         `json:"result"`
	ID          string                  `json:"id"`
	SignedFacts []base.BallotSignedFact `json:"signed_facts"`
	Point       base.StagePoint         `json:"point"`
	Threshold   base.Threshold          `json:"threshold"`
}

func (vp baseVoteproof) MarshalJSON() ([]byte, error) {
	var majority util.Hash
	if vp.majority != nil {
		majority = vp.majority.Hash()
	}

	return util.MarshalJSON(baseVoteproofJSONMarshaler{
		BaseHinter:  vp.BaseHinter,
		FinishedAt:  vp.finishedAt,
		Majority:    majority,
		Point:       vp.point,
		Result:      vp.result,
		Threshold:   vp.threshold,
		SignedFacts: vp.sfs,
		ID:          vp.id,
	})
}

type baseVoteproofJSONUnmarshaler struct {
	FinishedAt  localtime.Time        `json:"finished_at"`
	Result      base.VoteResult       `json:"result"`
	ID          string                `json:"id"`
	Majority    valuehash.HashDecoder `json:"majority"`
	SignedFacts []json.RawMessage     `json:"signed_facts"`
	Point       base.StagePoint       `json:"point"`
	Threshold   base.Threshold        `json:"threshold"`
}

func (vp *baseVoteproof) DecodeJSON(b []byte, enc *jsonenc.Encoder) error {
	e := util.StringErrorFunc("failed to decode baseVoteproof")

	var u baseVoteproofJSONUnmarshaler
	if err := enc.Unmarshal(b, &u); err != nil {
		return e(err, "")
	}

	majority := u.Majority.Hash()

	vp.sfs = make([]base.BallotSignedFact, len(u.SignedFacts))

	for i := range u.SignedFacts {
		if err := encoder.Decode(enc, u.SignedFacts[i], &vp.sfs[i]); err != nil {
			return e(err, "")
		}

		sfs := vp.sfs[i]

		if majority != nil { // NOTE find in SignedFacts
			if sfs.Fact().Hash().Equal(majority) {
				if fact, ok := sfs.Fact().(base.BallotFact); ok {
					vp.majority = fact
				}
			}
		}
	}

	vp.threshold = u.Threshold
	vp.finishedAt = u.FinishedAt.Time
	vp.point = u.Point
	vp.result = u.Result
	vp.id = u.ID

	return nil
}
