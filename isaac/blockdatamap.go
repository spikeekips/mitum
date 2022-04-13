package isaac

import (
	"bytes"
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

func (BlockDataMap) Bytes() []byte {
	return nil
}

func (m *BlockDataMap) signedBytes() []byte {
	ts := make([][]byte, len(m.m))

	i := -1
	for k := range m.m {
		i++
		j := m.m[k]
		if j == nil {
			continue
		}

		ts[i] = []byte(j.Checksum())
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
	num      int64
}

func NewBlockDataMapItem(t base.BlockDataType, u url.URL, checksum string, num int64) BlockDataMapItem {
	return BlockDataMapItem{
		BaseHinter: hint.NewBaseHinter(BlockDataMapItemHint),
		t:          t,
		url:        u,
		checksum:   checksum,
		num:        num,
	}
}

func NewLocalBlockDataMapItem(t base.BlockDataType, path string, checksum string, num int64) BlockDataMapItem {
	u := fileBlockDataURL
	u.Path = path

	return NewBlockDataMapItem(t, u, checksum, num)
}

func (item BlockDataMapItem) IsValid([]byte) error {
	e := util.StringErrorFunc("invalid BlockDataMapItem")
	if err := item.BaseHinter.IsValid(BlockDataMapItemHint.Type().Bytes()); err != nil {
		return e(err, "")
	}

	if err := item.t.IsValid(nil); err != nil {
		return e(err, "")
	}

	if n := len(item.checksum); n < 1 {
		return e(util.InvalidError.Errorf("empty checksum"), "")
	}

	switch {
	case len(item.url.String()) < 1:
		return e(util.InvalidError.Errorf("empty url"), "")
	case len(item.url.Scheme) < 1:
		return e(util.InvalidError.Errorf("empty url scheme"), "")
	default:
		scheme := strings.ToLower(item.url.Scheme)

		if !util.InStringSlice(strings.ToLower(item.url.Scheme), supportedBlockDataMapItemURLSchemes) {
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

func (item BlockDataMapItem) Num() int64 {
	return item.num
}