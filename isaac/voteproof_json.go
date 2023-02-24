package isaac

import (
	"encoding/json"
	"time"

	"github.com/pkg/errors"
	"github.com/spikeekips/mitum/base"
	"github.com/spikeekips/mitum/util"
	"github.com/spikeekips/mitum/util/encoder"
	jsonenc "github.com/spikeekips/mitum/util/encoder/json"
	"github.com/spikeekips/mitum/util/hint"
	"github.com/spikeekips/mitum/util/localtime"
	"github.com/spikeekips/mitum/util/valuehash"
)

type baseVoteproofJSONMarshaler struct {
	FinishedAt time.Time                        `json:"finished_at"`
	Majority   util.Hash                        `json:"majority"`
	ID         string                           `json:"id"`
	SignFacts  []base.BallotSignFact            `json:"sign_facts"`
	Withdraws  []base.SuffrageWithdrawOperation `json:"withdraws,omitempty"`
	Point      base.StagePoint                  `json:"point"`
	hint.BaseHinter
	Threshold base.Threshold `json:"threshold"`
}

func (vp baseVoteproof) jsonMarshaller() baseVoteproofJSONMarshaler {
	var majority util.Hash
	if vp.majority != nil {
		majority = vp.majority.Hash()
	}

	return baseVoteproofJSONMarshaler{
		BaseHinter: vp.BaseHinter,
		FinishedAt: vp.finishedAt,
		Majority:   majority,
		Point:      vp.point,
		Threshold:  vp.threshold,
		SignFacts:  vp.sfs,
		ID:         vp.id,
	}
}

func (vp baseVoteproof) MarshalJSON() ([]byte, error) {
	return util.MarshalJSON(vp.jsonMarshaller())
}

func (vp INITWithdrawVoteproof) MarshalJSON() ([]byte, error) {
	m := vp.jsonMarshaller()
	m.Withdraws = vp.withdraws

	return util.MarshalJSON(m)
}

func (vp INITStuckVoteproof) MarshalJSON() ([]byte, error) {
	m := vp.jsonMarshaller()
	m.Withdraws = vp.withdraws

	return util.MarshalJSON(m)
}

func (vp ACCEPTWithdrawVoteproof) MarshalJSON() ([]byte, error) {
	m := vp.jsonMarshaller()
	m.Withdraws = vp.withdraws

	return util.MarshalJSON(m)
}

func (vp ACCEPTStuckVoteproof) MarshalJSON() ([]byte, error) {
	m := vp.jsonMarshaller()
	m.Withdraws = vp.withdraws

	return util.MarshalJSON(m)
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

func (vp *baseVoteproof) decodeJSON(b []byte, enc *jsonenc.Encoder) (u baseVoteproofJSONUnmarshaler, _ error) {
	e := util.StringErrorFunc("failed to decode baseVoteproof")

	if err := enc.Unmarshal(b, &u); err != nil {
		return u, e(err, "")
	}

	majority := u.Majority.Hash()

	vp.sfs = make([]base.BallotSignFact, len(u.SignFacts))

	for i := range u.SignFacts {
		if err := encoder.Decode(enc, u.SignFacts[i], &vp.sfs[i]); err != nil {
			return u, e(err, "")
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

	vp.threshold = u.Threshold
	vp.finishedAt = u.FinishedAt.Time
	vp.point = u.Point
	vp.id = u.ID

	return u, nil
}

func (vp *baseVoteproof) DecodeJSON(b []byte, enc *jsonenc.Encoder) error {
	_, err := vp.decodeJSON(b, enc)

	return err
}

func decodeWithdrawVoteproofJSON(_ []byte, enc *jsonenc.Encoder, u baseVoteproofJSONUnmarshaler, i interface{}) error {
	withdraws := make([]base.SuffrageWithdrawOperation, len(u.Withdraws))

	for i := range u.Withdraws {
		if err := encoder.Decode(enc, u.Withdraws[i], &withdraws[i]); err != nil {
			return err
		}
	}

	switch t := i.(type) {
	case *baseWithdrawVoteproof:
		t.withdraws = withdraws
	case *baseStuckVoteproof:
		t.withdraws = withdraws
	default:
		return errors.Errorf("withdraws not found, %T", t)
	}

	return nil
}

func (vp *baseWithdrawVoteproof) decodeJSON(
	b []byte, enc *jsonenc.Encoder, u baseVoteproofJSONUnmarshaler,
) (err error) {
	return decodeWithdrawVoteproofJSON(b, enc, u, vp)
}

func (vp *baseStuckVoteproof) decodeJSON(b []byte, enc *jsonenc.Encoder, u baseVoteproofJSONUnmarshaler) (err error) {
	return decodeWithdrawVoteproofJSON(b, enc, u, vp)
}

func (vp *INITWithdrawVoteproof) DecodeJSON(b []byte, enc *jsonenc.Encoder) error {
	e := util.StringErrorFunc("failed to decode INITWithdrawVoteproof")

	u, err := vp.baseVoteproof.decodeJSON(b, enc)
	if err != nil {
		return e(err, "")
	}

	if err := vp.baseWithdrawVoteproof.decodeJSON(b, enc, u); err != nil {
		return e(err, "")
	}

	return nil
}

func (vp *INITStuckVoteproof) DecodeJSON(b []byte, enc *jsonenc.Encoder) error {
	e := util.StringErrorFunc("failed to decode INITStuckVoteproof")

	u, err := vp.baseVoteproof.decodeJSON(b, enc)
	if err != nil {
		return e(err, "")
	}

	if err := vp.baseStuckVoteproof.decodeJSON(b, enc, u); err != nil {
		return e(err, "")
	}

	return nil
}

func (vp *ACCEPTWithdrawVoteproof) DecodeJSON(b []byte, enc *jsonenc.Encoder) error {
	e := util.StringErrorFunc("failed to decode ACCEPTWithdrawVoteproof")

	u, err := vp.baseVoteproof.decodeJSON(b, enc)
	if err != nil {
		return e(err, "")
	}

	if err := vp.baseWithdrawVoteproof.decodeJSON(b, enc, u); err != nil {
		return e(err, "")
	}

	return nil
}

func (vp *ACCEPTStuckVoteproof) DecodeJSON(b []byte, enc *jsonenc.Encoder) error {
	e := util.StringErrorFunc("failed to decode ACCEPTStuckVoteproof")

	u, err := vp.baseVoteproof.decodeJSON(b, enc)
	if err != nil {
		return e(err, "")
	}

	if err := vp.baseStuckVoteproof.decodeJSON(b, enc, u); err != nil {
		return e(err, "")
	}

	return nil
}
