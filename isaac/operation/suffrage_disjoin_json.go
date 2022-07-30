package isaacoperation

import (
	"github.com/spikeekips/mitum/base"
	"github.com/spikeekips/mitum/util"
	jsonenc "github.com/spikeekips/mitum/util/encoder/json"
)

type suffrageDisjoinFactJSONMarshaler struct {
	Node base.Address `json:"node"`
	base.BaseFactJSONMarshaler
	Start base.Height `json:"start"`
}

func (fact SuffrageDisjoinFact) MarshalJSON() ([]byte, error) {
	return util.MarshalJSON(suffrageDisjoinFactJSONMarshaler{
		BaseFactJSONMarshaler: fact.BaseFact.JSONMarshaler(),
		Node:                  fact.node,
		Start:                 fact.start,
	})
}

type suffrageDisjoinFactJSONUnmarshaler struct {
	Node string `json:"node"`
	base.BaseFactJSONUnmarshaler
	Start base.Height `json:"start"`
}

func (fact *SuffrageDisjoinFact) DecodeJSON(b []byte, enc *jsonenc.Encoder) error {
	e := util.StringErrorFunc("failed to decode SuffrageDisjoinFact")

	var u suffrageDisjoinFactJSONUnmarshaler
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

	fact.start = u.Start

	return nil
}
