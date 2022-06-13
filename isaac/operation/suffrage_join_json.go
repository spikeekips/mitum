package isaacoperation

import (
	"encoding/json"

	"github.com/spikeekips/mitum/base"
	"github.com/spikeekips/mitum/util"
	"github.com/spikeekips/mitum/util/encoder"
	jsonenc "github.com/spikeekips/mitum/util/encoder/json"
	"github.com/spikeekips/mitum/util/valuehash"
)

type SuffrageJoinPermissionFactJSONMarshaler struct {
	Candidate base.Address `json:"candidate"`
	State     util.Hash    `json:"state"`
	base.BaseFactJSONMarshaler
}

func (fact SuffrageJoinPermissionFact) MarshalJSON() ([]byte, error) {
	return util.MarshalJSON(SuffrageJoinPermissionFactJSONMarshaler{
		BaseFactJSONMarshaler: fact.BaseFact.JSONMarshaler(),
		Candidate:             fact.candidate,
		State:                 fact.state,
	})
}

type SuffrageJoinPermissionFactJSONUnmarshaler struct {
	Candidate string                `json:"candidate"`
	State     valuehash.HashDecoder `json:"state"`
	base.BaseFactJSONUnmarshaler
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
	Nodes []base.Node `json:"nodes"`
	base.BaseFactJSONMarshaler
}

func (fact SuffrageGenesisJoinPermissionFact) MarshalJSON() ([]byte, error) {
	return util.MarshalJSON(SuffrageGenesisJoinPermissionFactJSONMarshaler{
		BaseFactJSONMarshaler: fact.BaseFact.JSONMarshaler(),
		Nodes:                 fact.nodes,
	})
}

type SuffrageGenesisJoinPermissionFactJSONUnmarshaler struct {
	Nodes []json.RawMessage `json:"nodes"`
	base.BaseFactJSONUnmarshaler
}

func (fact *SuffrageGenesisJoinPermissionFact) DecodeJSON(b []byte, enc *jsonenc.Encoder) error {
	e := util.StringErrorFunc("failed to decode SuffrageGenesisJoinPermissionFact")

	var u SuffrageGenesisJoinPermissionFactJSONUnmarshaler
	if err := enc.Unmarshal(b, &u); err != nil {
		return e(err, "")
	}

	fact.BaseFact.SetJSONUnmarshaler(u.BaseFactJSONUnmarshaler)

	fact.nodes = make([]base.Node, len(u.Nodes))

	for i := range u.Nodes {
		if err := encoder.Decode(enc, u.Nodes[i], &fact.nodes[i]); err != nil {
			return e(err, "")
		}
	}

	return nil
}
