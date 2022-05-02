package isaacblockdata

import (
	"encoding/json"
	"net/url"

	"github.com/spikeekips/mitum/base"
	"github.com/spikeekips/mitum/util"
	jsonenc "github.com/spikeekips/mitum/util/encoder/json"
	"github.com/spikeekips/mitum/util/hint"
)

type blockDataMapJSONMarshaler struct {
	hint.BaseHinter
	base.BaseNodeSignedJSONMarshaler
	W        hint.Hint                                    `json:"writer"`
	E        hint.Hint                                    `json:"encoder"`
	Manifest base.Manifest                                `json:"manifest"`
	M        map[base.BlockDataType]base.BlockDataMapItem `json:"items"`
}

func (m BlockDataMap) MarshalJSON() ([]byte, error) {
	items := map[base.BlockDataType]base.BlockDataMapItem{}
	m.m.Traverse(func(_, v interface{}) bool {
		if util.IsNilLockedValue(v) {
			return true
		}

		i := v.(BlockDataMapItem)
		items[i.Type()] = i

		return true
	})

	return util.MarshalJSON(blockDataMapJSONMarshaler{
		BaseHinter:                  m.BaseHinter,
		BaseNodeSignedJSONMarshaler: m.BaseNodeSigned.JSONMarshaler(),
		W:                           m.writer,
		E:                           m.encoder,
		Manifest:                    m.manifest,
		M:                           items,
	})
}

type blockDataMapJSONUnmarshaler struct {
	W        hint.Hint                              `json:"writer"`
	E        hint.Hint                              `json:"encoder"`
	Manifest json.RawMessage                        `json:"manifest"`
	M        map[base.BlockDataType]json.RawMessage `json:"items"`
}

func (m *BlockDataMap) DecodeJSON(b []byte, enc *jsonenc.Encoder) error {
	e := util.StringErrorFunc("failed to decode blockdatamap")

	var u blockDataMapJSONUnmarshaler
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
		var ui BlockDataMapItem
		if err := enc.Unmarshal(u.M[k], &ui); err != nil {
			return e(err, "failed to unmarshal blockdatamap item, %q", k)
		}

		_ = items.SetValue(ui.Type(), ui)
	}

	m.writer = u.W
	m.encoder = u.E
	m.m = items

	return nil
}

type blockDataMapItemJSONMarshaler struct {
	T        base.BlockDataType `json:"type"`
	URL      string             `json:"url"`
	Checksum string             `json:"checksum"`
	Num      int64              `json:"num"`
}

func (item BlockDataMapItem) MarshalJSON() ([]byte, error) {
	return util.MarshalJSON(blockDataMapItemJSONMarshaler{
		T:        item.t,
		URL:      item.url.String(),
		Checksum: item.checksum,
		Num:      item.num,
	})
}

type blockDataMapItemJSONUnmarshaler struct {
	T        base.BlockDataType `json:"type"`
	URL      string             `json:"url"`
	Checksum string             `json:"checksum"`
	Num      int64              `json:"num"`
}

func (item *BlockDataMapItem) UnmarshalJSON(b []byte) error {
	e := util.StringErrorFunc("failed to unmarshal blockDataMapItem")
	var u blockDataMapItemJSONUnmarshaler
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
