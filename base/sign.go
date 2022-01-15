package base

import (
	"time"

	"github.com/pkg/errors"
	"github.com/spikeekips/mitum/util"
	"github.com/spikeekips/mitum/util/encoder"
	"github.com/spikeekips/mitum/util/localtime"
)

type Sign interface {
	util.Byter
	util.IsValider
	Signer() Publickey
	Signature() Signature
	SignedAt() time.Time
	Verify([]byte) error
}

type BaseSign struct {
	signer    Publickey
	signature Signature
	signedAt  time.Time
}

func NewBaseSign(signer Publickey, signature Signature, signedAt time.Time) BaseSign {
	return BaseSign{
		signer:    signer,
		signature: signature,
		signedAt:  signedAt,
	}
}

func (si BaseSign) Signer() Publickey {
	return si.signer
}

func (si BaseSign) Signature() Signature {
	return si.signature
}

func (si BaseSign) SignedAt() time.Time {
	return si.signedAt
}

func (si BaseSign) Bytes() []byte {
	return util.ConcatByters(si.signer, si.signature, localtime.NewTime(si.signedAt))
}

func (si BaseSign) IsValid([]byte) error {
	err := util.CheckIsValid(nil, false,
		si.signer,
		si.signature,
		util.DummyIsValider(func([]byte) error {
			if si.signedAt.IsZero() {
				return util.InvalidError.Errorf("empty signedAt in BaseSign")
			}

			return nil
		}),
	)
	if err != nil {
		return errors.Wrap(err, "invalid BaseSign")
	}

	return nil
}

func (si BaseSign) Verify(input []byte) error {
	if err := si.signer.Verify(input, si.signature); err != nil {
		return errors.Wrap(err, "failed to verfiy sign")
	}

	return nil
}

type baseSignJSONMarshaler struct {
	Signer    Publickey
	Signature Signature
	SignedAt  localtime.Time `json:"signed_at"`
}

func (si BaseSign) MarshalJSON() ([]byte, error) {
	return util.MarshalJSON(baseSignJSONMarshaler{
		Signer:    si.signer,
		Signature: si.signature,
		SignedAt:  localtime.NewTime(si.signedAt),
	})
}

type baseSignJSONUnmarshaler struct {
	Signer    PublickeyDecoder
	Signature Signature
	SignedAt  localtime.Time `json:"signed_at"`
}

func (si *BaseSign) DecodeJSON(b []byte, enc encoder.Encoder) error {
	e := util.StringErrorFunc("faied to decode BaseSign")

	var u baseSignJSONUnmarshaler
	if err := util.UnmarshalJSON(b, &u); err != nil {
		return e(err, "")
	}

	signer, err := u.Signer.Decode(enc)
	if err != nil {
		return e(err, "")
	}

	si.signer = signer
	si.signature = u.Signature
	si.signedAt = u.SignedAt.Time

	return nil
}
