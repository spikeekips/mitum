package isaac

import (
	"encoding/json"

	"github.com/spikeekips/mitum/base"
	"github.com/spikeekips/mitum/util"
	"github.com/spikeekips/mitum/util/encoder"
	jsonenc "github.com/spikeekips/mitum/util/encoder/json"
	"github.com/spikeekips/mitum/util/hint"
)

type NetworkPolicyStateValueJSONMarshaler struct {
	Policy base.NetworkPolicy `json:"policy"`
	hint.BaseHinter
}

func (s NetworkPolicyStateValue) MarshalJSON() ([]byte, error) {
	return util.MarshalJSON(NetworkPolicyStateValueJSONMarshaler{
		BaseHinter: s.BaseHinter,
		Policy:     s.policy,
	})
}

type NetworkPolicyStateValueJSONUnmarshaler struct {
	Policy json.RawMessage `json:"policy"`
}

func (s *NetworkPolicyStateValue) DecodeJSON(b []byte, enc *jsonenc.Encoder) error {
	e := util.StringErrorFunc("failed to decode NetworkPolicyStateValue")

	var u NetworkPolicyStateValueJSONUnmarshaler
	if err := enc.Unmarshal(b, &u); err != nil {
		return e(err, "")
	}

	if err := encoder.Decode(enc, u.Policy, &s.policy); err != nil {
		return e(err, "")
	}

	return nil
}
