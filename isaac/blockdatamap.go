package isaac

import (
	"bytes"
	"fmt"
	"net/url"
	"sort"
	"strings"

	"github.com/pkg/errors"
	"github.com/spikeekips/mitum/base"
	"github.com/spikeekips/mitum/util"
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

var BlockDirectoryHeightFormat = "%021s"

func init() {
	u, err := url.Parse("file+blockdata://")
	if err != nil {
		panic(errors.Wrap(err, "failed to initialize fileBlockDataURL"))
	}
	fileBlockDataURL = *u
}

type BlockDataMap struct {
	hint.BaseHinter
	base.BaseNodeSigned
	writer   hint.Hint
	encoder  hint.Hint
	manifest base.Manifest
	m        map[base.BlockDataType]base.BlockDataMapItem
}

func NewBlockDataMap(writer, encoder hint.Hint) BlockDataMap {
	return BlockDataMap{
		BaseHinter: hint.NewBaseHinter(BlockDataMapHint),
		writer:     writer,
		encoder:    encoder,
		m:          map[base.BlockDataType]base.BlockDataMapItem{},
	}
}

func (m BlockDataMap) IsValid(b []byte) error {
	e := util.StringErrorFunc("invalid BlockDataMap")
	if err := m.BaseHinter.IsValid(BlockDataMapHint.Type().Bytes()); err != nil {
		return e(err, "")
	}

	if err := util.CheckIsValid(nil, false, m.writer, m.encoder, m.manifest, m.BaseNodeSigned); err != nil {
		return e(err, "")
	}

	// NOTE proposal and voteproofs must exist
	if i, found := m.m[base.BlockDataTypeProposal]; !found || i == nil {
		return e(util.InvalidError.Errorf("empty proposal"), "")
	}
	if i, found := m.m[base.BlockDataTypeVoteproofs]; !found || i == nil {
		return e(util.InvalidError.Errorf("empty voteproofs"), "")
	}

	vs := make([]util.IsValider, len(m.m))

	var i int
	for k := range m.m {
		vs[i] = m.m[k]
		i++
	}

	if err := util.CheckIsValid(nil, true, vs...); err != nil {
		return e(err, "invalid item found")
	}

	if err := m.BaseNodeSigned.Verify(b, m.signedBytes()); err != nil {
		return e(util.InvalidError.Wrap(err), "")
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

func (m *BlockDataMap) Sign(node base.Address, priv base.Privatekey, networkID base.NetworkID) error {
	sign, err := base.BaseNodeSignedFromBytes(node, priv, networkID, m.signedBytes())
	if err != nil {
		return errors.Wrap(err, "failed to sign BlockDataMap")
	}

	m.BaseNodeSigned = sign

	return nil
}

func (m BlockDataMap) Bytes() []byte {
	return nil
}

func (m *BlockDataMap) signedBytes() []byte {
	var ts [][]byte
	for k := range m.m {
		i := m.m[k]
		if i == nil {
			continue
		}

		ts = append(ts, []byte(i.Checksum()))
	}

	sort.Slice(ts, func(i, j int) bool {
		return bytes.Compare(ts[i], ts[j]) < 0
	})

	return util.ConcatByters(
		m.manifest.Hash(),
		util.BytesToByter(util.ConcatBytesSlice(ts...)),
	)
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

func NewLocalBlockDataMapItem(t base.BlockDataType, path string, checksum string) BlockDataMapItem {
	u := fileBlockDataURL
	u.Path = path

	return NewBlockDataMapItem(t, u, checksum)
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

func HeightDirectory(height base.Height) string {
	h := height.String()
	if height < 0 {
		h = strings.ReplaceAll(h, "-", "_")
	}

	p := fmt.Sprintf(BlockDirectoryHeightFormat, h)

	sl := make([]string, 7)
	var i int
	for {
		e := (i * 3) + 3
		if e > len(p) {
			e = len(p)
		}

		s := p[i*3 : e]
		if len(s) < 1 {
			break
		}

		sl[i] = s

		if len(s) < 3 {
			break
		}

		i++
	}

	return "/" + strings.Join(sl, "/")
}
