package tree

import (
	"encoding/json"

	"github.com/spikeekips/mitum/util"
	jsonenc "github.com/spikeekips/mitum/util/encoder/json"
	"github.com/spikeekips/mitum/util/hint"
)

type BaseFixedtreeNodeJSONMarshaler struct {
	hint.BaseHinter
	IN uint64 `json:"index"`
	KY string `json:"key"`
	HS string `json:"hash"`
}

func (no BaseFixedtreeNode) JSONMarshaler() BaseFixedtreeNodeJSONMarshaler {
	return BaseFixedtreeNodeJSONMarshaler{
		BaseHinter: no.BaseHinter,
		IN:         no.index,
		KY:         no.key,
		HS:         util.EncodeHash(no.hash),
	}
}

func (no BaseFixedtreeNode) MarshalJSON() ([]byte, error) {
	if len(no.key) < 1 {
		return util.MarshalJSON(nil)
	}

	return util.MarshalJSON(no.JSONMarshaler())
}

type BaseFixedtreeNodeJSONUnmarshaler struct {
	IN uint64 `json:"index"`
	KY string `json:"key"`
	HS string `json:"hash"`
}

func (no *BaseFixedtreeNode) UnmarshalJSON(b []byte) error {
	e := util.StringErrorFunc("failed to unmarshal BaseFixedtreeNode")

	var u BaseFixedtreeNodeJSONUnmarshaler
	if err := util.UnmarshalJSON(b, &u); err != nil {
		return e(err, "")
	}

	no.index = u.IN
	no.key = u.KY

	no.hash = util.DecodeHash(u.HS)

	return nil
}

type FixedtreeJSONMarshaler struct {
	hint.BaseHinter
	NS []FixedtreeNode `json:"nodes"`
}

func (tr Fixedtree) MarshalJSON() ([]byte, error) {
	return util.MarshalJSON(FixedtreeJSONMarshaler{
		BaseHinter: tr.BaseHinter,
		NS:         tr.nodes,
	})
}

type FixedtreeJSONUnmarshaler struct {
	NS []json.RawMessage `json:"nodes"`
}

func (tr *Fixedtree) DecodeJSON(b []byte, enc *jsonenc.Encoder) error {
	e := util.StringErrorFunc("failed to decode Fixedtree")

	var u FixedtreeJSONUnmarshaler
	if err := enc.Unmarshal(b, &u); err != nil {
		return e(err, "")
	}

	tr.nodes = make([]FixedtreeNode, len(u.NS))

	for i := range u.NS {
		j, err := enc.Decode(u.NS[i])
		if err != nil {
			return e(err, "")
		}

		k, ok := j.(FixedtreeNode)
		if !ok {
			return e(nil, "not FixedtreeNode, %T", j)
		}

		tr.nodes[i] = k
	}

	return nil
}
