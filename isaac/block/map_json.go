package isaacblock

import (
	"encoding/json"
	"net/url"

	"github.com/spikeekips/mitum/base"
	"github.com/spikeekips/mitum/util"
	jsonenc "github.com/spikeekips/mitum/util/encoder/json"
	"github.com/spikeekips/mitum/util/hint"
)

type blockMapJSONMarshaler struct {
	hint.BaseHinter
	base.BaseNodeSignedJSONMarshaler
	W        hint.Hint                                   `json:"writer"`
	E        hint.Hint                                   `json:"encoder"`
	Manifest base.Manifest                               `json:"manifest"`
	M        map[base.BlockMapItemType]base.BlockMapItem `json:"items"`
}

func (m BlockMap) MarshalJSON() ([]byte, error) {
	items := map[base.BlockMapItemType]base.BlockMapItem{}
	m.m.Traverse(func(_, v interface{}) bool {
		if util.IsNilLockedValue(v) {
			return true
		}

		i := v.(BlockMapItem)
		items[i.Type()] = i

		return true
	})

	return util.MarshalJSON(blockMapJSONMarshaler{
		BaseHinter:                  m.BaseHinter,
		BaseNodeSignedJSONMarshaler: m.BaseNodeSigned.JSONMarshaler(),
		W:                           m.writer,
		E:                           m.encoder,
		Manifest:                    m.manifest,
		M:                           items,
	})
}

type blockMapJSONUnmarshaler struct {
	W        hint.Hint                                 `json:"writer"`
	E        hint.Hint                                 `json:"encoder"`
	Manifest json.RawMessage                           `json:"manifest"`
	M        map[base.BlockMapItemType]json.RawMessage `json:"items"`
}

func (m *BlockMap) DecodeJSON(b []byte, enc *jsonenc.Encoder) error {
	e := util.StringErrorFunc("failed to decode blockmap")

	var u blockMapJSONUnmarshaler
	if err := util.UnmarshalJSON(b, &u); err != nil {
		return e(err, "")
	}

	if err := m.BaseNodeSigned.DecodeJSON(b, enc); err != nil {
		return e(err, "")
	}

	switch hinter, err := enc.Decode(u.Manifest); {
	case err != nil:
		return e(err, "failed to decode manifest")
	default:
		i, ok := hinter.(base.Manifest)
		if !ok {
			return e(err, "decoded not Manifest, %T", hinter)
		}

		m.manifest = i
	}

	items := util.NewLockedMap()
	for k := range u.M {
		var ui BlockMapItem
		if err := enc.Unmarshal(u.M[k], &ui); err != nil {
			return e(err, "failed to unmarshal blockmap item, %q", k)
		}

		_ = items.SetValue(ui.Type(), ui)
	}

	m.writer = u.W
	m.encoder = u.E
	m.m = items

	return nil
}

type blockMapItemJSONMarshaler struct {
	T        base.BlockMapItemType `json:"type"`
	URL      string                `json:"url"`
	Checksum string                `json:"checksum"`
	Num      uint64                `json:"num"`
}

func (item BlockMapItem) MarshalJSON() ([]byte, error) {
	return util.MarshalJSON(blockMapItemJSONMarshaler{
		T:        item.t,
		URL:      item.url.String(),
		Checksum: item.checksum,
		Num:      item.num,
	})
}

type blockMapItemJSONUnmarshaler struct {
	T        base.BlockMapItemType `json:"type"`
	URL      string                `json:"url"`
	Checksum string                `json:"checksum"`
	Num      uint64                `json:"num"`
}

func (item *BlockMapItem) UnmarshalJSON(b []byte) error {
	e := util.StringErrorFunc("failed to unmarshal blockMapItem")
	var u blockMapItemJSONUnmarshaler
	if err := util.UnmarshalJSON(b, &u); err != nil {
		return e(err, "")
	}

	switch i, err := url.Parse(u.URL); {
	case err != nil:
		return e(err, "failed to parse url")
	default:
		item.url = *i
	}

	item.t = u.T
	item.checksum = u.Checksum
	item.num = u.Num

	return nil
}
