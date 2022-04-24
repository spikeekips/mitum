package isaacoperation

import (
	"encoding/json"

	"github.com/spikeekips/mitum/base"
	"github.com/spikeekips/mitum/util"
	jsonenc "github.com/spikeekips/mitum/util/encoder/json"
)

type GenesisNetworkPolicyFactJSONMarshaler struct {
	base.BaseFactJSONMarshaler
	Policy base.NetworkPolicy `json:"policy"`
}

func (fact GenesisNetworkPolicyFact) MarshalJSON() ([]byte, error) {
	return util.MarshalJSON(GenesisNetworkPolicyFactJSONMarshaler{
		BaseFactJSONMarshaler: fact.BaseFact.JSONMarshaler(),
		Policy:                fact.policy,
	})
}

type GenesisNetworkPolicyFactJSONUnmarshaler struct {
	base.BaseFactJSONUnmarshaler
	Policy json.RawMessage `json:"policy"`
}

func (fact *GenesisNetworkPolicyFact) DecodeJSON(b []byte, enc *jsonenc.Encoder) error {
	e := util.StringErrorFunc("failed to decode GenesisNetworkPolicyFact")

	var u GenesisNetworkPolicyFactJSONUnmarshaler
	if err := enc.Unmarshal(b, &u); err != nil {
		return e(err, "")
	}

	fact.BaseFact.SetJSONUnmarshaler(u.BaseFactJSONUnmarshaler)

	switch hinter, err := enc.Decode(u.Policy); {
	case err != nil:
		return e(err, "")
	default:
		i, ok := hinter.(base.NetworkPolicy)
		if !ok {
			return e(nil, "expected NetworkPolicy, but %T", hinter)
		}

		fact.policy = i
	}

	return nil
}
