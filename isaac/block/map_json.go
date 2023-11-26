package isaacblock

import (
	"encoding/json"
	"net/url"

	"github.com/spikeekips/mitum/base"
	"github.com/spikeekips/mitum/util"
	"github.com/spikeekips/mitum/util/encoder"
	"github.com/spikeekips/mitum/util/hint"
)

type blockMapJSONMarshaler struct {
	Manifest base.Manifest                               `json:"manifest"`
	Items    map[base.BlockMapItemType]base.BlockMapItem `json:"items"`
	base.BaseNodeSignJSONMarshaler
	hint.BaseHinter
	Writer  hint.Hint `json:"writer"`
	Encoder hint.Hint `json:"encoder"`
}

func (m BlockMap) MarshalJSON() ([]byte, error) {
	items := map[base.BlockMapItemType]base.BlockMapItem{}

	m.items.Traverse(func(_ base.BlockMapItemType, v base.BlockMapItem) bool {
		if v != nil {
			items[v.Type()] = v
		}

		return true
	})

	return util.MarshalJSON(blockMapJSONMarshaler{
		BaseHinter:                m.BaseHinter,
		BaseNodeSignJSONMarshaler: m.BaseNodeSign.JSONMarshaler(),
		Writer:                    m.writer,
		Encoder:                   m.encoder,
		Manifest:                  m.manifest,
		Items:                     items,
	})
}

type blockMapJSONUnmarshaler struct {
	Items    map[base.BlockMapItemType]json.RawMessage `json:"items"`
	Manifest json.RawMessage                           `json:"manifest"`
	Writer   hint.Hint                                 `json:"writer"`
	Encoder  hint.Hint                                 `json:"encoder"`
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

	items := util.NewSingleLockedMap[base.BlockMapItemType, base.BlockMapItem]()

	for k := range u.Items {
		var ui BlockMapItem
		if err := enc.Unmarshal(u.Items[k], &ui); err != nil {
			return e.WithMessage(err, "unmarshal blockmap item, %q", k)
		}

		_ = items.SetValue(ui.Type(), ui)
	}

	m.writer = u.Writer
	m.encoder = u.Encoder
	m.items = items

	return nil
}

type blockMapItemJSONMarshaler struct {
	Type     base.BlockMapItemType `json:"type"`
	URL      string                `json:"url"`
	Checksum string                `json:"checksum"`
	Num      uint64                `json:"num"`
}

func (item BlockMapItem) MarshalJSON() ([]byte, error) {
	return util.MarshalJSON(blockMapItemJSONMarshaler{
		Type:     item.t,
		URL:      item.url.String(),
		Checksum: item.checksum,
		Num:      item.num,
	})
}

type blockMapItemJSONUnmarshaler struct {
	Type     base.BlockMapItemType `json:"type"`
	URL      string                `json:"url"`
	Checksum string                `json:"checksum"`
	Num      uint64                `json:"num"`
}

func (item *BlockMapItem) UnmarshalJSON(b []byte) error {
	e := util.StringError("unmarshal blockMapItem")
	var u blockMapItemJSONUnmarshaler

	if err := util.UnmarshalJSON(b, &u); err != nil {
		return e.Wrap(err)
	}

	switch i, err := url.Parse(u.URL); {
	case err != nil:
		return e.WithMessage(err, "parse url")
	default:
		item.url = *i
	}

	item.t = u.Type
	item.checksum = u.Checksum
	item.num = u.Num

	return nil
}
