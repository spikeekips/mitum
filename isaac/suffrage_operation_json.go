package isaac

import (
	"github.com/spikeekips/mitum/base"
	"github.com/spikeekips/mitum/util"
	jsonenc "github.com/spikeekips/mitum/util/encoder/json"
)

type suffrageWithdrawFactJSONMarshaler struct {
	Node   base.Address `json:"node"`
	Reason string       `json:"reason"`
	base.BaseFactJSONMarshaler
	Start base.Height `json:"start"`
	End   base.Height `json:"end"`
}

func (fact SuffrageWithdrawFact) MarshalJSON() ([]byte, error) {
	return util.MarshalJSON(suffrageWithdrawFactJSONMarshaler{
		BaseFactJSONMarshaler: fact.BaseFact.JSONMarshaler(),
		Node:                  fact.node,
		Start:                 fact.start,
		End:                   fact.end,
		Reason:                fact.reason,
	})
}

type suffrageWithdrawFactJSONUnmarshaler struct {
	Node   string `json:"node"`
	Reason string `json:"reason"`
	base.BaseFactJSONUnmarshaler
	Start base.Height `json:"start"`
	End   base.Height `json:"end"`
}

func (fact *SuffrageWithdrawFact) DecodeJSON(b []byte, enc *jsonenc.Encoder) error {
	e := util.StringErrorFunc("decode SuffrageWithdrawFact")

	var u suffrageWithdrawFactJSONUnmarshaler
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
	fact.end = u.End
	fact.reason = u.Reason

	return nil
}
