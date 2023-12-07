package isaacblock

import (
	"encoding/json"

	"github.com/spikeekips/mitum/base"
	"github.com/spikeekips/mitum/util"
	"github.com/spikeekips/mitum/util/encoder"
	"github.com/spikeekips/mitum/util/hint"
)

type blockMapJSONMarshaler struct {
	Manifest base.Manifest                            `json:"manifest"`
	Items    map[base.BlockItemType]base.BlockMapItem `json:"items"`
	base.BaseNodeSignJSONMarshaler
	hint.BaseHinter
}

func (m BlockMap) MarshalJSON() ([]byte, error) {
	items := map[base.BlockItemType]base.BlockMapItem{}

	m.items.Traverse(func(_ base.BlockItemType, v base.BlockMapItem) bool {
		if v != nil {
			items[v.Type()] = v
		}

		return true
	})

	return util.MarshalJSON(blockMapJSONMarshaler{
		BaseHinter:                m.BaseHinter,
		BaseNodeSignJSONMarshaler: m.BaseNodeSign.JSONMarshaler(),
		Manifest:                  m.manifest,
		Items:                     items,
	})
}

type blockMapJSONUnmarshaler struct {
	Items    map[base.BlockItemType]json.RawMessage `json:"items"`
	Manifest json.RawMessage                        `json:"manifest"`
}

func (m *BlockMap) DecodeJSON(b []byte, enc encoder.Encoder) error {
	e := util.StringError("decode blockmap")

	var u blockMapJSONUnmarshaler
	if err := util.UnmarshalJSON(b, &u); err != nil {
		return e.Wrap(err)
	}

	if err := m.BaseNodeSign.DecodeJSON(b, enc); err != nil {
		return e.Wrap(err)
	}

	if err := encoder.Decode(enc, u.Manifest, &m.manifest); err != nil {
		return e.WithMessage(err, "decode manifest")
	}

	items := util.NewSingleLockedMap[base.BlockItemType, base.BlockMapItem]()

	for k := range u.Items {
		var ui BlockMapItem
		if err := enc.Unmarshal(u.Items[k], &ui); err != nil {
			return e.WithMessage(err, "unmarshal block item, %q", k)
		}

		_ = items.SetValue(ui.Type(), ui)
	}

	m.items = items

	return nil
}

type blockMapItemJSONMarshaler struct {
	Type     base.BlockItemType `json:"type"`
	Checksum string             `json:"checksum"`
}

func (item BlockMapItem) MarshalJSON() ([]byte, error) {
	return util.MarshalJSON(blockMapItemJSONMarshaler{
		Type:     item.t,
		Checksum: item.checksum,
	})
}

type blockMapItemJSONUnmarshaler struct {
	Type     base.BlockItemType `json:"type"`
	Checksum string             `json:"checksum"`
}

func (item *BlockMapItem) UnmarshalJSON(b []byte) error {
	e := util.StringError("unmarshal blockMapItem")
	var u blockMapItemJSONUnmarshaler

	if err := util.UnmarshalJSON(b, &u); err != nil {
		return e.Wrap(err)
	}

	item.t = u.Type
	item.checksum = u.Checksum

	return nil
}
