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
			return e.WithMessage(err, "unmarshal blockmap item, %q", k)
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

type BlockItemFileJSONMarshaler struct {
	URI            string `json:"uri"`
	CompressFormat string `json:"compress_format,omitempty"`
	hint.BaseHinter
}

func (f BlockItemFile) MarshalJSON() ([]byte, error) {
	return util.MarshalJSON(BlockItemFileJSONMarshaler{
		BaseHinter:     f.BaseHinter,
		URI:            f.uri.String(),
		CompressFormat: f.compressFormat,
	})
}

func (f *BlockItemFile) UnmarshalJSON(b []byte) error {
	var u BlockItemFileJSONMarshaler
	if err := util.UnmarshalJSON(b, &u); err != nil {
		return err
	}

	switch i, err := url.Parse(u.URI); {
	case err != nil:
		return util.ErrInvalid.Wrap(err)
	default:
		f.uri = *i
	}

	f.compressFormat = u.CompressFormat

	return nil
}

type BlockItemFilesJSONMarshaler struct {
	Items map[base.BlockItemType]base.BlockItemFile `json:"items"`
	hint.BaseHinter
}

func (f BlockItemFiles) MarshalJSON() ([]byte, error) {
	return util.MarshalJSON(BlockItemFilesJSONMarshaler{
		BaseHinter: f.BaseHinter,
		Items:      f.items,
	})
}

type BlockItemFilesJSONUnmarshaler struct {
	Items map[base.BlockItemType]json.RawMessage `json:"items"`
	hint.BaseHinter
}

func (f *BlockItemFiles) DecodeJSON(b []byte, enc encoder.Encoder) error {
	var u BlockItemFilesJSONUnmarshaler
	if err := util.UnmarshalJSON(b, &u); err != nil {
		return err
	}

	f.items = map[base.BlockItemType]base.BlockItemFile{}

	for i := range u.Items {
		var j base.BlockItemFile

		if err := encoder.Decode(enc, u.Items[i], &j); err != nil {
			return err
		}

		f.items[i] = j
	}

	return nil
}
