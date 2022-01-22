package tree

import (
	"encoding/json"

	"github.com/btcsuite/btcutil/base58"
	"github.com/spikeekips/mitum/util"
	jsonenc "github.com/spikeekips/mitum/util/encoder/json"
	"github.com/spikeekips/mitum/util/hint"
)

type BaseFixedTreeNodeJSONPacker struct {
	hint.BaseHinter
	IN uint64 `json:"index"`
	KY string `json:"key"`
	HS string `json:"hash"`
}

func (no BaseFixedTreeNode) MarshalJSON() ([]byte, error) {
	if len(no.key) < 1 {
		return util.MarshalJSON(nil)
	}

	return util.MarshalJSON(BaseFixedTreeNodeJSONPacker{
		BaseHinter: no.BaseHinter,
		IN:         no.index,
		KY:         base58.Encode(no.key),
		HS:         base58.Encode(no.hash),
	})
}

type BaseFixedTreeNodeJSONUnpacker struct {
	IN uint64 `json:"index"`
	KY string `json:"key"`
	HS string `json:"hash"`
}

func (no *BaseFixedTreeNode) UnmarshalJSON(b []byte) error {
	var u BaseFixedTreeNodeJSONUnpacker
	if err := util.UnmarshalJSON(b, &u); err != nil {
		return err
	}

	no.index = u.IN
	no.key = base58.Decode(u.KY)
	no.hash = base58.Decode(u.HS)

	return nil
}

type FixedTreeJSONPacker struct {
	hint.BaseHinter
	NS []FixedTreeNode `json:"nodes"`
}

func (tr FixedTree) MarshalJSON() ([]byte, error) {
	return util.MarshalJSON(FixedTreeJSONPacker{
		BaseHinter: tr.BaseHinter,
		NS:         tr.nodes,
	})
}

type FixedTreeJSONUnpacker struct {
	NS []json.RawMessage `json:"nodes"`
}

func (tr *FixedTree) DecodeJSON(b []byte, enc *jsonenc.Encoder) error {
	e := util.StringErrorFunc("failed to decode FixedTree")

	var u FixedTreeJSONUnpacker
	if err := enc.Unmarshal(b, &u); err != nil {
		return e(err, "")
	}

	tr.nodes = make([]FixedTreeNode, len(u.NS))

	for i := range u.NS {
		j, err := enc.Decode(u.NS[i])
		if err != nil {
			return e(err, "")
		}

		k, ok := j.(FixedTreeNode)
		if !ok {
			return e(nil, "not FixedTreeNode, %T", j)
		}

		tr.nodes[i] = k
	}

	return nil
}
