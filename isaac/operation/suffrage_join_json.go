package isaacoperation

import (
	"encoding/json"

	"github.com/spikeekips/mitum/base"
	"github.com/spikeekips/mitum/util"
	"github.com/spikeekips/mitum/util/encoder"
	jsonenc "github.com/spikeekips/mitum/util/encoder/json"
	"github.com/spikeekips/mitum/util/valuehash"
)

type SuffrageJoinFactJSONMarshaler struct {
	Candidate base.Address `json:"candidate"`
	State     util.Hash    `json:"state"`
	base.BaseFactJSONMarshaler
}

func (fact SuffrageJoinFact) MarshalJSON() ([]byte, error) {
	return util.MarshalJSON(SuffrageJoinFactJSONMarshaler{
		BaseFactJSONMarshaler: fact.BaseFact.JSONMarshaler(),
		Candidate:             fact.candidate,
		State:                 fact.state,
	})
}

type SuffrageJoinFactJSONUnmarshaler struct {
	Candidate string                `json:"candidate"`
	State     valuehash.HashDecoder `json:"state"`
	base.BaseFactJSONUnmarshaler
}

func (fact *SuffrageJoinFact) DecodeJSON(b []byte, enc *jsonenc.Encoder) error {
	e := util.StringErrorFunc("failed to decode SuffrageJoinFact")

	var u SuffrageJoinFactJSONUnmarshaler
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

type SuffrageGenesisJoinFactJSONMarshaler struct {
	Nodes []base.Node `json:"nodes"`
	base.BaseFactJSONMarshaler
}

func (fact SuffrageGenesisJoinFact) MarshalJSON() ([]byte, error) {
	return util.MarshalJSON(SuffrageGenesisJoinFactJSONMarshaler{
		BaseFactJSONMarshaler: fact.BaseFact.JSONMarshaler(),
		Nodes:                 fact.nodes,
	})
}

type SuffrageGenesisJoinFactJSONUnmarshaler struct {
	Nodes []json.RawMessage `json:"nodes"`
	base.BaseFactJSONUnmarshaler
}

func (fact *SuffrageGenesisJoinFact) DecodeJSON(b []byte, enc *jsonenc.Encoder) error {
	e := util.StringErrorFunc("failed to decode SuffrageGenesisJoinFact")

	var u SuffrageGenesisJoinFactJSONUnmarshaler
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
