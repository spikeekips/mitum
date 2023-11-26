package isaacoperation

import (
	"encoding/json"

	"github.com/spikeekips/mitum/base"
	"github.com/spikeekips/mitum/util"
	"github.com/spikeekips/mitum/util/encoder"
)

type suffrageJoinFactJSONMarshaler struct {
	Candidate base.Address `json:"candidate"`
	base.BaseFactJSONMarshaler
	StartHeight base.Height `json:"start_height"`
}

func (fact SuffrageJoinFact) MarshalJSON() ([]byte, error) {
	return util.MarshalJSON(suffrageJoinFactJSONMarshaler{
		BaseFactJSONMarshaler: fact.BaseFact.JSONMarshaler(),
		Candidate:             fact.candidate,
		StartHeight:           fact.start,
	})
}

type suffrageJoinFactJSONUnmarshaler struct {
	Candidate string `json:"candidate"`
	base.BaseFactJSONUnmarshaler
	StartHeight base.Height `json:"start_height"`
}

func (fact *SuffrageJoinFact) DecodeJSON(b []byte, enc encoder.Encoder) error {
	e := util.StringError("decode SuffrageJoinFact")

	var u suffrageJoinFactJSONUnmarshaler
	if err := enc.Unmarshal(b, &u); err != nil {
		return e.Wrap(err)
	}

	fact.BaseFact.SetJSONUnmarshaler(u.BaseFactJSONUnmarshaler)

	switch i, err := base.DecodeAddress(u.Candidate, enc); {
	case err != nil:
		return e.Wrap(err)
	default:
		fact.candidate = i
	}

	fact.start = u.StartHeight

	return nil
}

type suffrageGenesisJoinFactJSONMarshaler struct {
	Nodes []base.Node `json:"nodes"`
	base.BaseFactJSONMarshaler
}

func (fact SuffrageGenesisJoinFact) MarshalJSON() ([]byte, error) {
	return util.MarshalJSON(suffrageGenesisJoinFactJSONMarshaler{
		BaseFactJSONMarshaler: fact.BaseFact.JSONMarshaler(),
		Nodes:                 fact.nodes,
	})
}

type suffrageGenesisJoinFactJSONUnmarshaler struct {
	Nodes []json.RawMessage `json:"nodes"`
	base.BaseFactJSONUnmarshaler
}

func (fact *SuffrageGenesisJoinFact) DecodeJSON(b []byte, enc encoder.Encoder) error {
	e := util.StringError("decode SuffrageGenesisJoinFact")

	var u suffrageGenesisJoinFactJSONUnmarshaler
	if err := enc.Unmarshal(b, &u); err != nil {
		return e.Wrap(err)
	}

	fact.BaseFact.SetJSONUnmarshaler(u.BaseFactJSONUnmarshaler)

	fact.nodes = make([]base.Node, len(u.Nodes))

	for i := range u.Nodes {
		if err := encoder.Decode(enc, u.Nodes[i], &fact.nodes[i]); err != nil {
			return e.Wrap(err)
		}
	}

	return nil
}
