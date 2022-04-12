package base

import (
	"github.com/spikeekips/mitum/util"
	jsonenc "github.com/spikeekips/mitum/util/encoder/json"
	"github.com/spikeekips/mitum/util/localtime"
)

type BaseSignedJSONMarshaler struct {
	Signer    Publickey      `json:"signer"`
	Signature Signature      `json:"signature"`
	SignedAt  localtime.Time `json:"signed_at"`
}

func (si BaseSigned) JSONMarshaler() BaseSignedJSONMarshaler {
	return BaseSignedJSONMarshaler{
		Signer:    si.signer,
		Signature: si.signature,
		SignedAt:  localtime.New(si.signedAt),
	}
}

func (si BaseSigned) MarshalJSON() ([]byte, error) {
	return util.MarshalJSON(si.JSONMarshaler())
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

type BaseNodeSignedJSONMarshaler struct {
	BaseSignedJSONMarshaler
	Node Address `json:"node"`
}

func (si BaseNodeSigned) JSONMarshaler() BaseNodeSignedJSONMarshaler {
	return BaseNodeSignedJSONMarshaler{
		BaseSignedJSONMarshaler: si.BaseSigned.JSONMarshaler(),
		Node:                    si.node,
	}
}

func (si BaseNodeSigned) MarshalJSON() ([]byte, error) {
	return util.MarshalJSON(si.JSONMarshaler())
}

type baseNodeSignedJSONUnmarshaler struct {
	Node string `json:"node"`
}

func (si *BaseNodeSigned) DecodeJSON(b []byte, enc *jsonenc.Encoder) error {
	e := util.StringErrorFunc("failed to decode BaseNodeSigned")

	var u baseNodeSignedJSONUnmarshaler
	if err := util.UnmarshalJSON(b, &u); err != nil {
		return e(err, "")
	}

	switch ad, err := DecodeAddress(u.Node, enc); {
	case err != nil:
		return e(err, "failed to decode node")
	default:
		si.node = ad
	}

	if err := si.BaseSigned.DecodeJSON(b, enc); err != nil {
		return e(err, "")
	}

	return nil
}
