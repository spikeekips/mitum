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
	Majority   util.Hash `json:"majority"` // NOTE fact hash of majority BallotSignFact
	hint.BaseHinter
	ID        string                           `json:"id"`
	SignFacts []base.BallotSignFact            `json:"sign_facts"`
	Withdraws []base.SuffrageWithdrawOperation `json:"withdraws,omitempty"`
	Point     base.StagePoint                  `json:"point"`
	Threshold base.Threshold                   `json:"threshold"`
}

func (vp baseVoteproof) MarshalJSON() ([]byte, error) {
	var majority util.Hash
	if vp.majority != nil {
		majority = vp.majority.Hash()
	}

	return util.MarshalJSON(baseVoteproofJSONMarshaler{
		BaseHinter: vp.BaseHinter,
		FinishedAt: vp.finishedAt,
		Majority:   majority,
		Point:      vp.point,
		Threshold:  vp.threshold,
		SignFacts:  vp.sfs,
		ID:         vp.id,
		Withdraws:  vp.withdraws,
	})
}

type baseVoteproofJSONUnmarshaler struct {
	FinishedAt localtime.Time        `json:"finished_at"`
	ID         string                `json:"id"`
	Majority   valuehash.HashDecoder `json:"majority"`
	SignFacts  []json.RawMessage     `json:"sign_facts"`
	Withdraws  []json.RawMessage     `json:"withdraws"`
	Point      base.StagePoint       `json:"point"`
	Threshold  base.Threshold        `json:"threshold"`
}

func (vp *baseVoteproof) DecodeJSON(b []byte, enc *jsonenc.Encoder) error {
	e := util.StringErrorFunc("failed to decode baseVoteproof")

	var u baseVoteproofJSONUnmarshaler
	if err := enc.Unmarshal(b, &u); err != nil {
		return e(err, "")
	}

	majority := u.Majority.Hash()

	vp.sfs = make([]base.BallotSignFact, len(u.SignFacts))

	for i := range u.SignFacts {
		if err := encoder.Decode(enc, u.SignFacts[i], &vp.sfs[i]); err != nil {
			return e(err, "")
		}

		sfs := vp.sfs[i]

		if majority != nil { // NOTE find in SignFacts
			if sfs.Fact().Hash().Equal(majority) {
				if fact, ok := sfs.Fact().(base.BallotFact); ok {
					vp.majority = fact
				}
			}
		}
	}

	vp.withdraws = make([]base.SuffrageWithdrawOperation, len(u.Withdraws))

	for i := range u.Withdraws {
		if err := encoder.Decode(enc, u.Withdraws[i], &vp.withdraws[i]); err != nil {
			return e(err, "")
		}
	}

	vp.threshold = u.Threshold
	vp.finishedAt = u.FinishedAt.Time
	vp.point = u.Point
	vp.id = u.ID

	return nil
}
