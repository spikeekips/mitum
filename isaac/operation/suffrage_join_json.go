package isaacoperation

import (
	"github.com/spikeekips/mitum/base"
	"github.com/spikeekips/mitum/util"
	jsonenc "github.com/spikeekips/mitum/util/encoder/json"
	"github.com/spikeekips/mitum/util/valuehash"
)

type SuffrageJoinPermissionFactJSONMarshaler struct {
	base.BaseFactJSONMarshaler
	Candidate base.Address
	State     util.Hash
}

func (suf SuffrageJoinPermissionFact) MarshalJSON() ([]byte, error) {
	return util.MarshalJSON(SuffrageJoinPermissionFactJSONMarshaler{
		BaseFactJSONMarshaler: suf.BaseFact.JSONMarshaler(),
		Candidate:             suf.candidate,
		State:                 suf.state,
	})
}

type SuffrageJoinPermissionFactJSONUnmarshaler struct {
	base.BaseFactJSONUnmarshaler
	Candidate string
	State     valuehash.HashDecoder
}

func (suf *SuffrageJoinPermissionFact) DecodeJSON(b []byte, enc *jsonenc.Encoder) error {
	e := util.StringErrorFunc("failed to decode SuffrageJoinPermissionFact")

	var u SuffrageJoinPermissionFactJSONUnmarshaler
	if err := enc.Unmarshal(b, &u); err != nil {
		return e(err, "")
	}

	suf.BaseFact.SetJSONUnmarshaler(u.BaseFactJSONUnmarshaler)

	switch i, err := base.DecodeAddress(u.Candidate, enc); {
	case err != nil:
		return e(err, "")
	default:
		suf.candidate = i
	}

	suf.state = u.State.Hash()

	return nil
}

type SuffrageGenesisJoinPermissionFactJSONMarshaler struct {
	base.BaseFactJSONMarshaler
	Node base.Address
	Pub  base.Publickey
}

func (suf SuffrageGenesisJoinPermissionFact) MarshalJSON() ([]byte, error) {
	return util.MarshalJSON(SuffrageGenesisJoinPermissionFactJSONMarshaler{
		BaseFactJSONMarshaler: suf.BaseFact.JSONMarshaler(),
		Node:                  suf.node,
		Pub:                   suf.pub,
	})
}

type SuffrageGenesisJoinPermissionFactJSONUnmarshaler struct {
	base.BaseFactJSONUnmarshaler
	Node string
	Pub  string
}

func (suf *SuffrageGenesisJoinPermissionFact) DecodeJSON(b []byte, enc *jsonenc.Encoder) error {
	e := util.StringErrorFunc("failed to decode SuffrageGenesisJoinPermissionFact")

	var u SuffrageGenesisJoinPermissionFactJSONUnmarshaler
	if err := enc.Unmarshal(b, &u); err != nil {
		return e(err, "")
	}

	suf.BaseFact.SetJSONUnmarshaler(u.BaseFactJSONUnmarshaler)

	switch i, err := base.DecodeAddress(u.Node, enc); {
	case err != nil:
		return e(err, "")
	default:
		suf.node = i
	}

	pub, err := base.DecodePublickeyFromString(u.Pub, enc)
	if err != nil {
		return e(err, "")
	}

	suf.pub = pub

	return nil
}
