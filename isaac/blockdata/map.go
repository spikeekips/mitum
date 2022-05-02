package isaacblockdata

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

var BlockDataMapHint = hint.MustNewHint("blockdatamap-v0.0.1")

var (
	LocalBlockDataMapScheme             = "file+blockdata"
	supportedBlockDataMapItemURLSchemes = map[string]struct{}{
		LocalBlockDataMapScheme: {},
		"file":                  {},
		"http":                  {},
		"https":                 {},
	}
	fileBlockDataURL = url.URL{Scheme: LocalBlockDataMapScheme}
)

var BlockDirectoryHeightFormat = "%021s"

type BlockDataMap struct {
	hint.BaseHinter
	base.BaseNodeSigned
	writer   hint.Hint
	encoder  hint.Hint
	manifest base.Manifest
	m        *util.LockedMap
}

func NewBlockDataMap(writer, encoder hint.Hint) BlockDataMap {
	return BlockDataMap{
		BaseHinter: hint.NewBaseHinter(BlockDataMapHint),
		writer:     writer,
		encoder:    encoder,
		m:          util.NewLockedMap(),
	}
}

func (m BlockDataMap) IsValid(b []byte) error {
	e := util.StringErrorFunc("invalid blockdatamap")
	if err := m.BaseHinter.IsValid(BlockDataMapHint.Type().Bytes()); err != nil {
		return e(err, "")
	}

	if err := util.CheckIsValid(nil, false, m.writer, m.encoder, m.manifest, m.BaseNodeSigned); err != nil {
		return e(err, "")
	}

	if err := m.checkItems(); err != nil {
		return e(err, "")
	}

	vs := make([]util.IsValider, m.m.Len())
	var i int
	m.m.Traverse(func(_, v interface{}) bool {
		switch {
		case v == nil:
			return true
		case util.IsNilLockedValue(v):
			return true
		}

		vs[i] = v.(BlockDataMapItem)
		i++

		return true
	})

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
	switch i, found := m.m.Value(t); {
	case !found:
		return nil, false
	case i == nil:
		return nil, false
	case util.IsNilLockedValue(i):
		return nil, false
	default:
		return i.(BlockDataMapItem), true
	}
}

func (m *BlockDataMap) SetItem(item base.BlockDataMapItem) error {
	e := util.StringErrorFunc("failed to set blockdatamap item")

	if err := item.IsValid(nil); err != nil {
		return e(err, "")
	}

	_ = m.m.SetValue(item.Type(), item)

	return nil
}

func (m BlockDataMap) Items(f func(base.BlockDataMapItem) bool) {
	m.m.Traverse(func(_, v interface{}) bool {
		switch {
		case v == nil:
			return true
		case util.IsNilLockedValue(v):
			return true
		}

		return f(v.(base.BlockDataMapItem))
	})
}

func (m *BlockDataMap) Sign(node base.Address, priv base.Privatekey, networkID base.NetworkID) error {
	sign, err := base.BaseNodeSignedFromBytes(node, priv, networkID, m.signedBytes())
	if err != nil {
		return errors.Wrap(err, "failed to sign blockdatamap")
	}

	m.BaseNodeSigned = sign

	return nil
}

func (m BlockDataMap) checkItems() error {
	check := func(t base.BlockDataType) bool {
		switch i, found := m.m.Value(t); {
		case !found:
			return false
		case i == nil:
			return false
		case util.IsNilLockedValue(i):
			return false
		default:
			return true
		}
	}

	if !check(base.BlockDataTypeProposal) {
		return util.InvalidError.Errorf("empty proposal")
	}

	if !check(base.BlockDataTypeVoteproofs) {
		return util.InvalidError.Errorf("empty voteproofs")
	}

	if m.manifest.OperationsTree() != nil {
		if !check(base.BlockDataTypeOperationsTree) {
			return util.InvalidError.Errorf("empty operations tree")
		}
	}

	if m.manifest.StatesTree() != nil {
		if !check(base.BlockDataTypeStatesTree) {
			return util.InvalidError.Errorf("empty states tree")
		}
	}

	return nil
}

func (BlockDataMap) Bytes() []byte {
	return nil
}

func (m BlockDataMap) signedBytes() []byte {
	ts := make([][]byte, m.m.Len())

	var i int
	m.m.Traverse(func(_, v interface{}) bool {
		switch {
		case v == nil:
			return true
		case util.IsNilLockedValue(v):
			return true
		}

		ts[i] = []byte(v.(BlockDataMapItem).Checksum())
		i++

		return true
	})

	sort.Slice(ts, func(i, j int) bool {
		return bytes.Compare(ts[i], ts[j]) < 0
	})

	return util.ConcatByters(
		m.manifest.Hash(),
		util.BytesToByter(util.ConcatBytesSlice(ts...)),
	)
}

type BlockDataMapItem struct {
	t        base.BlockDataType
	url      url.URL
	checksum string
	num      int64
}

func NewBlockDataMapItem(t base.BlockDataType, u url.URL, checksum string, num int64) BlockDataMapItem {
	return BlockDataMapItem{
		t:        t,
		url:      u,
		checksum: checksum,
		num:      num,
	}
}

func NewLocalBlockDataMapItem(t base.BlockDataType, checksum string, num int64) BlockDataMapItem {
	return NewBlockDataMapItem(t, fileBlockDataURL, checksum, num)
}

func (item BlockDataMapItem) IsValid([]byte) error {
	e := util.StringErrorFunc("invalid blockdatamapItem")

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
		if _, found := supportedBlockDataMapItemURLSchemes[strings.ToLower(item.url.Scheme)]; !found {
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
