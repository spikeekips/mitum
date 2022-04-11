package isaac

import (
	"encoding/json"
	"net/url"
	"strings"

	"github.com/pkg/errors"
	"github.com/spikeekips/mitum/base"
	"github.com/spikeekips/mitum/util"
	jsonenc "github.com/spikeekips/mitum/util/encoder/json"
	"github.com/spikeekips/mitum/util/hint"
)

var (
	BlockDataMapHint     = hint.MustNewHint("blockdatamap-v0.0.1")
	BlockDataMapItemHint = hint.MustNewHint("blockdatamap-item-v0.0.1")
)

var (
	supportedBlockDataMapItemURLSchemes = []string{"file+blockdata", "file", "http", "https"}
	fileBlockDataURL                    url.URL
)

func init() {
	u, err := url.Parse("file+blockdata://")
	if err != nil {
		panic(errors.Wrap(err, "failed to initialize fileBlockDataURL"))
	}
	fileBlockDataURL = *u
}

type BlockDataMap struct {
	hint.BaseHinter
	manifest base.Manifest
	m        map[base.BlockDataType]base.BlockDataMapItem
}

func NewBlockDataMap() BlockDataMap {
	return BlockDataMap{
		BaseHinter: hint.NewBaseHinter(BlockDataMapHint),
		m:          map[base.BlockDataType]base.BlockDataMapItem{},
	}
}

func (m BlockDataMap) IsValid([]byte) error {
	e := util.StringErrorFunc("invalid BlockDataMap")
	if err := m.BaseHinter.IsValid(BlockDataMapHint.Type().Bytes()); err != nil {
		return e(err, "")
	}

	if err := util.CheckIsValid(nil, false, m.manifest); err != nil {
		return e(err, "")
	}

	vs := make([]util.IsValider, len(m.m))

	var notemptyfound bool
	var i int
	for k := range m.m {
		if !notemptyfound && m.m[k] != nil {
			notemptyfound = true
		}

		vs[i] = m.m[k]
		i++
	}

	if !notemptyfound {
		return e(util.InvalidError.Errorf("empty items"), "")
	}

	if err := util.CheckIsValid(nil, true, vs...); err != nil {
		return e(err, "invalid item found")
	}

	return nil
}

func (m BlockDataMap) Manifest() base.Manifest {
	return m.manifest
}

func (m *BlockDataMap) SetManifest(manifest base.Manifest) {
	m.manifest = manifest
}

func (m BlockDataMap) Item(t base.BlockDataType) (base.BlockDataMapItem, bool) {
	item, found := m.m[t]

	return item, found
}

func (m *BlockDataMap) SetItem(item base.BlockDataMapItem) error {
	e := util.StringErrorFunc("failed to set blockdatamap item")
	if err := item.IsValid(nil); err != nil {
		return e(err, "")
	}

	m.m[item.Type()] = item

	return nil
}

func (m BlockDataMap) All() map[base.BlockDataType]base.BlockDataMapItem {
	return m.m
}

type blockDataMapJSONMarshaler struct {
	hint.BaseHinter
	Manifest base.Manifest                                `json:"manifest"`
	M        map[base.BlockDataType]base.BlockDataMapItem `json:"items"`
}

func (m BlockDataMap) MarshalJSON() ([]byte, error) {
	return util.MarshalJSON(blockDataMapJSONMarshaler{
		BaseHinter: m.BaseHinter,
		Manifest:   m.manifest,
		M:          m.m,
	})
}

type blockDataMapJSONUnmarshaler struct {
	Manifest json.RawMessage                        `json:"manifest"`
	M        map[base.BlockDataType]json.RawMessage `json:"items"`
}

func (m *BlockDataMap) DecodeJSON(b []byte, enc *jsonenc.Encoder) error {
	e := util.StringErrorFunc("failed to decode BlockDataMap")

	var u blockDataMapJSONUnmarshaler
	if err := util.UnmarshalJSON(b, &u); err != nil {
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

	um := map[base.BlockDataType]base.BlockDataMapItem{}
	for k := range u.M {
		switch hinter, err := enc.Decode(u.M[k]); {
		case err != nil:
			return e(err, "failed to decode BlockDataMapItem")
		default:
			i, ok := hinter.(base.BlockDataMapItem)
			if !ok {
				return e(err, "decoded not BlockDataMapItem, %T", hinter)
			}

			um[k] = i
		}
	}

	m.m = um

	return nil
}

type BlockDataMapItem struct {
	hint.BaseHinter
	t        base.BlockDataType
	url      url.URL
	checksum string
}

func NewBlockDataMapItem(t base.BlockDataType, u url.URL, checksum string) BlockDataMapItem {
	return BlockDataMapItem{
		BaseHinter: hint.NewBaseHinter(BlockDataMapItemHint),
		t:          t, url: u, checksum: checksum,
	}
}

func NewLocalBlockDataMapItem(t base.BlockDataType, checksum string) BlockDataMapItem {
	return NewBlockDataMapItem(t, fileBlockDataURL, checksum)
}

func (m BlockDataMapItem) IsValid([]byte) error {
	e := util.StringErrorFunc("invalid BlockDataMapItem")
	if err := m.BaseHinter.IsValid(BlockDataMapItemHint.Type().Bytes()); err != nil {
		return e(err, "")
	}

	if err := m.t.IsValid(nil); err != nil {
		return e(err, "")
	}

	if n := len(m.checksum); n < 1 {
		return e(util.InvalidError.Errorf("empty checksum"), "")
	}

	switch {
	case len(m.url.String()) < 1:
		return e(util.InvalidError.Errorf("empty url"), "")
	case len(m.url.Scheme) < 1:
		return e(util.InvalidError.Errorf("empty url scheme"), "")
	default:
		scheme := strings.ToLower(m.url.Scheme)

		if !util.InStringSlice(strings.ToLower(m.url.Scheme), supportedBlockDataMapItemURLSchemes) {
			return e(util.InvalidError.Errorf("unsupported url scheme found, %q", scheme), "")
		}
	}

	return nil
}

func (item BlockDataMapItem) Type() base.BlockDataType {
	return item.t
}

func (item BlockDataMapItem) URL() *url.URL {
	return &item.url
}

func (item BlockDataMapItem) Checksum() string {
	return item.checksum
}

type blockDataMapItemJSONMarshaler struct {
	hint.BaseHinter
	T        base.BlockDataType `json:"type"`
	URL      string             `json:"url"`
	Checksum string             `json:"checksum"`
}

func (item BlockDataMapItem) MarshalJSON() ([]byte, error) {
	return util.MarshalJSON(blockDataMapItemJSONMarshaler{
		BaseHinter: item.BaseHinter,
		T:          item.t,
		URL:        item.url.String(),
		Checksum:   item.checksum,
	})
}

type blockDataMapItemJSONUnmarshaler struct {
	T        base.BlockDataType `json:"type"`
	URL      string             `json:"url"`
	Checksum string             `json:"checksum"`
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

	return nil
}
