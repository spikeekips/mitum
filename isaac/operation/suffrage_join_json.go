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

func (fact SuffrageJoinPermissionFact) MarshalJSON() ([]byte, error) {
	return util.MarshalJSON(SuffrageJoinPermissionFactJSONMarshaler{
		BaseFactJSONMarshaler: fact.BaseFact.JSONMarshaler(),
		Candidate:             fact.candidate,
		State:                 fact.state,
	})
}

type SuffrageJoinPermissionFactJSONUnmarshaler struct {
	base.BaseFactJSONUnmarshaler
	Candidate string
	State     valuehash.HashDecoder
}

func (fact *SuffrageJoinPermissionFact) DecodeJSON(b []byte, enc *jsonenc.Encoder) error {
	e := util.StringErrorFunc("failed to decode SuffrageJoinPermissionFact")

	var u SuffrageJoinPermissionFactJSONUnmarshaler
	if err := enc.Unmarshal(b, &u); err != nil {
		return e(err, "")
	}

	fact.BaseFact.SetJSONUnmarshaler(u.BaseFactJSONUnmarshaler)

	switch i, err := base.DecodeAddress(u.Candidate, enc); {
	case err != nil:
		return e(err, "")
	default:
		fact.candidate = i
	}

	fact.state = u.State.Hash()

	return nil
}

type SuffrageGenesisJoinPermissionFactJSONMarshaler struct {
	base.BaseFactJSONMarshaler
	Node base.Address
	Pub  base.Publickey
}

func (fact SuffrageGenesisJoinPermissionFact) MarshalJSON() ([]byte, error) {
	return util.MarshalJSON(SuffrageGenesisJoinPermissionFactJSONMarshaler{
		BaseFactJSONMarshaler: fact.BaseFact.JSONMarshaler(),
		Node:                  fact.node,
		Pub:                   fact.pub,
	})
}

type SuffrageGenesisJoinPermissionFactJSONUnmarshaler struct {
	base.BaseFactJSONUnmarshaler
	Node string
	Pub  string
}

func (fact *SuffrageGenesisJoinPermissionFact) DecodeJSON(b []byte, enc *jsonenc.Encoder) error {
	e := util.StringErrorFunc("failed to decode SuffrageGenesisJoinPermissionFact")

	var u SuffrageGenesisJoinPermissionFactJSONUnmarshaler
	if err := enc.Unmarshal(b, &u); err != nil {
		return e(err, "")
	}

	fact.BaseFact.SetJSONUnmarshaler(u.BaseFactJSONUnmarshaler)

	switch i, err := base.DecodeAddress(u.Node, enc); {
	case err != nil:
		return e(err, "")
	default:
		fact.node = i
	}

	pub, err := base.DecodePublickeyFromString(u.Pub, enc)
	if err != nil {
		return e(err, "")
	}

	fact.pub = pub

	return nil
}
