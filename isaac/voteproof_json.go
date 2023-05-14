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
	FinishedAt time.Time                     `json:"finished_at"`
	Majority   util.Hash                     `json:"majority"`
	ID         string                        `json:"id"`
	SignFacts  []base.BallotSignFact         `json:"sign_facts"`
	Expels     []base.SuffrageExpelOperation `json:"expels,omitempty"`
	Point      base.StagePoint               `json:"point"`
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

func (vp INITExpelVoteproof) MarshalJSON() ([]byte, error) {
	m := vp.jsonMarshaller()
	m.Expels = vp.expels

	return util.MarshalJSON(m)
}

func (vp INITStuckVoteproof) MarshalJSON() ([]byte, error) {
	m := vp.jsonMarshaller()
	m.Expels = vp.expels

	return util.MarshalJSON(m)
}

func (vp ACCEPTExpelVoteproof) MarshalJSON() ([]byte, error) {
	m := vp.jsonMarshaller()
	m.Expels = vp.expels

	return util.MarshalJSON(m)
}

func (vp ACCEPTStuckVoteproof) MarshalJSON() ([]byte, error) {
	m := vp.jsonMarshaller()
	m.Expels = vp.expels

	return util.MarshalJSON(m)
}

type baseVoteproofJSONUnmarshaler struct {
	FinishedAt localtime.Time        `json:"finished_at"`
	ID         string                `json:"id"`
	Majority   valuehash.HashDecoder `json:"majority"`
	SignFacts  []json.RawMessage     `json:"sign_facts"`
	Expels     []json.RawMessage     `json:"expels"`
	Point      base.StagePoint       `json:"point"`
	Threshold  base.Threshold        `json:"threshold"`
}

func (vp *baseVoteproof) decodeJSON(b []byte, enc *jsonenc.Encoder) (u baseVoteproofJSONUnmarshaler, _ error) {
	e := util.StringError("decode baseVoteproof")

	if err := enc.Unmarshal(b, &u); err != nil {
		return u, e.Wrap(err)
	}

	majority := u.Majority.Hash()

	vp.sfs = make([]base.BallotSignFact, len(u.SignFacts))

	for i := range u.SignFacts {
		if err := encoder.Decode(enc, u.SignFacts[i], &vp.sfs[i]); err != nil {
			return u, e.Wrap(err)
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

func decodeExpelVoteproofJSON(_ []byte, enc *jsonenc.Encoder, u baseVoteproofJSONUnmarshaler, i interface{}) error {
	expels := make([]base.SuffrageExpelOperation, len(u.Expels))

	for i := range u.Expels {
		if err := encoder.Decode(enc, u.Expels[i], &expels[i]); err != nil {
			return err
		}
	}

	switch t := i.(type) {
	case *baseExpelVoteproof:
		t.expels = expels
	case *baseStuckVoteproof:
		t.expels = expels
	default:
		return errors.Errorf("expels not found, %T", t)
	}

	return nil
}

func (vp *baseExpelVoteproof) decodeJSON(
	b []byte, enc *jsonenc.Encoder, u baseVoteproofJSONUnmarshaler,
) (err error) {
	return decodeExpelVoteproofJSON(b, enc, u, vp)
}

func (vp *baseStuckVoteproof) decodeJSON(b []byte, enc *jsonenc.Encoder, u baseVoteproofJSONUnmarshaler) (err error) {
	return decodeExpelVoteproofJSON(b, enc, u, vp)
}

func (vp *INITExpelVoteproof) DecodeJSON(b []byte, enc *jsonenc.Encoder) error {
	e := util.StringError("decode INITExpelVoteproof")

	u, err := vp.baseVoteproof.decodeJSON(b, enc)
	if err != nil {
		return e.Wrap(err)
	}

	if err := vp.baseExpelVoteproof.decodeJSON(b, enc, u); err != nil {
		return e.Wrap(err)
	}

	return nil
}

func (vp *INITStuckVoteproof) DecodeJSON(b []byte, enc *jsonenc.Encoder) error {
	e := util.StringError("decode INITStuckVoteproof")

	u, err := vp.baseVoteproof.decodeJSON(b, enc)
	if err != nil {
		return e.Wrap(err)
	}

	if err := vp.baseStuckVoteproof.decodeJSON(b, enc, u); err != nil {
		return e.Wrap(err)
	}

	return nil
}

func (vp *ACCEPTExpelVoteproof) DecodeJSON(b []byte, enc *jsonenc.Encoder) error {
	e := util.StringError("decode ACCEPTExpelVoteproof")

	u, err := vp.baseVoteproof.decodeJSON(b, enc)
	if err != nil {
		return e.Wrap(err)
	}

	if err := vp.baseExpelVoteproof.decodeJSON(b, enc, u); err != nil {
		return e.Wrap(err)
	}

	return nil
}

func (vp *ACCEPTStuckVoteproof) DecodeJSON(b []byte, enc *jsonenc.Encoder) error {
	e := util.StringError("decode ACCEPTStuckVoteproof")

	u, err := vp.baseVoteproof.decodeJSON(b, enc)
	if err != nil {
		return e.Wrap(err)
	}

	if err := vp.baseStuckVoteproof.decodeJSON(b, enc, u); err != nil {
		return e.Wrap(err)
	}

	return nil
}
