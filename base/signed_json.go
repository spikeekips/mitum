package base

import (
	"github.com/spikeekips/mitum/util"
	jsonenc "github.com/spikeekips/mitum/util/encoder/json"
	"github.com/spikeekips/mitum/util/localtime"
)

type baseSignedJSONMarshaler struct {
	Signer    Publickey      `json:"signer"`
	Signature Signature      `json:"signature"`
	SignedAt  localtime.Time `json:"signed_at"`
}

func (si BaseSigned) MarshalJSON() ([]byte, error) {
	return util.MarshalJSON(baseSignedJSONMarshaler{
		Signer:    si.signer,
		Signature: si.signature,
		SignedAt:  localtime.NewTime(si.signedAt),
	})
}

type baseSignedJSONUnmarshaler struct {
	Signer    string         `json:"signer"`
	Signature Signature      `json:"signature"`
	SignedAt  localtime.Time `json:"signed_at"`
}

func (si *BaseSigned) DecodeJSON(b []byte, enc *jsonenc.Encoder) error {
	e := util.StringErrorFunc("faied to decode BaseSign")

	var u baseSignedJSONUnmarshaler
	if err := util.UnmarshalJSON(b, &u); err != nil {
		return e(err, "")
	}

	signer, err := DecodePublickeyFromString(u.Signer, enc)
	if err != nil {
		return e(err, "")
	}

	si.signer = signer
	si.signature = u.Signature
	si.signedAt = u.SignedAt.Time

	return nil
}
