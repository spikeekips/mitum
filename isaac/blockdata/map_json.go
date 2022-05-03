package isaacblockdata

import (
	"encoding/json"
	"net/url"

	"github.com/spikeekips/mitum/base"
	"github.com/spikeekips/mitum/util"
	jsonenc "github.com/spikeekips/mitum/util/encoder/json"
	"github.com/spikeekips/mitum/util/hint"
)

type blockdataMapJSONMarshaler struct {
	hint.BaseHinter
	base.BaseNodeSignedJSONMarshaler
	W        hint.Hint                                    `json:"writer"`
	E        hint.Hint                                    `json:"encoder"`
	Manifest base.Manifest                                `json:"manifest"`
	M        map[base.BlockdataType]base.BlockdataMapItem `json:"items"`
}

func (m BlockdataMap) MarshalJSON() ([]byte, error) {
	items := map[base.BlockdataType]base.BlockdataMapItem{}
	m.m.Traverse(func(_, v interface{}) bool {
		if util.IsNilLockedValue(v) {
			return true
		}

		i := v.(BlockdataMapItem)
		items[i.Type()] = i

		return true
	})

	return util.MarshalJSON(blockdataMapJSONMarshaler{
		BaseHinter:                  m.BaseHinter,
		BaseNodeSignedJSONMarshaler: m.BaseNodeSigned.JSONMarshaler(),
		W:                           m.writer,
		E:                           m.encoder,
		Manifest:                    m.manifest,
		M:                           items,
	})
}

type blockdataMapJSONUnmarshaler struct {
	W        hint.Hint                              `json:"writer"`
	E        hint.Hint                              `json:"encoder"`
	Manifest json.RawMessage                        `json:"manifest"`
	M        map[base.BlockdataType]json.RawMessage `json:"items"`
}

func (m *BlockdataMap) DecodeJSON(b []byte, enc *jsonenc.Encoder) error {
	e := util.StringErrorFunc("failed to decode blockdatamap")

	var u blockdataMapJSONUnmarshaler
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
		var ui BlockdataMapItem
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

type blockdataMapItemJSONMarshaler struct {
	T        base.BlockdataType `json:"type"`
	URL      string             `json:"url"`
	Checksum string             `json:"checksum"`
	Num      uint64             `json:"num"`
}

func (item BlockdataMapItem) MarshalJSON() ([]byte, error) {
	return util.MarshalJSON(blockdataMapItemJSONMarshaler{
		T:        item.t,
		URL:      item.url.String(),
		Checksum: item.checksum,
		Num:      item.num,
	})
}

type blockdataMapItemJSONUnmarshaler struct {
	T        base.BlockdataType `json:"type"`
	URL      string             `json:"url"`
	Checksum string             `json:"checksum"`
	Num      uint64             `json:"num"`
}

func (item *BlockdataMapItem) UnmarshalJSON(b []byte) error {
	e := util.StringErrorFunc("failed to unmarshal blockdataMapItem")
	var u blockdataMapItemJSONUnmarshaler
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
