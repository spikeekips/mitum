package isaacblock

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

var BlockMapHint = hint.MustNewHint("blockmap-v0.0.1")

var (
	LocalBlockMapScheme             = "file+block"
	supportedBlockMapItemURLSchemes = map[string]struct{}{
		LocalBlockMapScheme: {},
		"file":              {},
		"http":              {},
		"https":             {},
	}
	fileBlockURL = url.URL{Scheme: LocalBlockMapScheme}
)

var BlockDirectoryHeightFormat = "%021s"

type BlockMap struct {
	manifest base.Manifest
	m        *util.LockedMap
	base.BaseNodeSigned
	hint.BaseHinter
	writer  hint.Hint
	encoder hint.Hint
}

func NewBlockMap(writer, encoder hint.Hint) BlockMap {
	return BlockMap{
		BaseHinter: hint.NewBaseHinter(BlockMapHint),
		writer:     writer,
		encoder:    encoder,
		m:          util.NewLockedMap(),
	}
}

func (m BlockMap) IsValid(b []byte) error {
	e := util.StringErrorFunc("invalid blockmap")
	if err := m.BaseHinter.IsValid(BlockMapHint.Type().Bytes()); err != nil {
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

		vs[i] = v.(BlockMapItem)
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

func (m BlockMap) Manifest() base.Manifest {
	return m.manifest
}

func (m *BlockMap) SetManifest(manifest base.Manifest) {
	m.manifest = manifest
}

func (m BlockMap) Item(t base.BlockMapItemType) (base.BlockMapItem, bool) {
	switch i, found := m.m.Value(t); {
	case !found:
		return nil, false
	case i == nil:
		return nil, false
	case util.IsNilLockedValue(i):
		return nil, false
	default:
		return i.(BlockMapItem), true
	}
}

func (m *BlockMap) SetItem(item base.BlockMapItem) error {
	e := util.StringErrorFunc("failed to set blockmap item")

	if err := item.IsValid(nil); err != nil {
		return e(err, "")
	}

	_ = m.m.SetValue(item.Type(), item)

	return nil
}

func (m BlockMap) Items(f func(base.BlockMapItem) bool) {
	m.m.Traverse(func(_, v interface{}) bool {
		switch {
		case v == nil:
			return true
		case util.IsNilLockedValue(v):
			return true
		}

		return f(v.(base.BlockMapItem))
	})
}

func (m *BlockMap) Sign(node base.Address, priv base.Privatekey, networkID base.NetworkID) error {
	sign, err := base.BaseNodeSignedFromBytes(node, priv, networkID, m.signedBytes())
	if err != nil {
		return errors.Wrap(err, "failed to sign blockmap")
	}

	m.BaseNodeSigned = sign

	return nil
}

func (m BlockMap) checkItems() error {
	check := func(t base.BlockMapItemType) bool {
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

	if !check(base.BlockMapItemTypeProposal) {
		return util.InvalidError.Errorf("empty proposal")
	}

	if !check(base.BlockMapItemTypeVoteproofs) {
		return util.InvalidError.Errorf("empty voteproofs")
	}

	if m.manifest.OperationsTree() != nil {
		if !check(base.BlockMapItemTypeOperationsTree) {
			return util.InvalidError.Errorf("empty operations tree")
		}
	}

	if m.manifest.StatesTree() != nil {
		if !check(base.BlockMapItemTypeStatesTree) {
			return util.InvalidError.Errorf("empty states tree")
		}
	}

	return nil
}

func (BlockMap) Bytes() []byte {
	return nil
}

func (m BlockMap) signedBytes() []byte {
	ts := make([][]byte, m.m.Len())

	var i int
	m.m.Traverse(func(_, v interface{}) bool {
		switch {
		case v == nil:
			return true
		case util.IsNilLockedValue(v):
			return true
		}

		// NOTE only checksum and num will be included in signature
		item := v.(BlockMapItem)
		ts[i] = util.ConcatBytesSlice([]byte(item.Checksum()), util.Uint64ToBytes(item.Num()))
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

type BlockMapItem struct {
	t        base.BlockMapItemType
	url      url.URL
	checksum string
	num      uint64
}

func NewBlockMapItem(t base.BlockMapItemType, u url.URL, checksum string, num uint64) BlockMapItem {
	return BlockMapItem{
		t:        t,
		url:      u,
		checksum: checksum,
		num:      num,
	}
}

func NewLocalBlockMapItem(t base.BlockMapItemType, checksum string, num uint64) BlockMapItem {
	return NewBlockMapItem(t, fileBlockURL, checksum, num)
}

func (item BlockMapItem) IsValid([]byte) error {
	e := util.StringErrorFunc("invalid BlockMapItem")

	if err := item.t.IsValid(nil); err != nil {
		return e(err, "")
	}

	if item.num < 1 {
		return e(util.InvalidError.Errorf("zero num"), "")
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
		if _, found := supportedBlockMapItemURLSchemes[strings.ToLower(item.url.Scheme)]; !found {
			return e(util.InvalidError.Errorf("unsupported url scheme found, %q", scheme), "")
		}
	}

	return nil
}

func (item BlockMapItem) Type() base.BlockMapItemType {
	return item.t
}

func (item BlockMapItem) URL() *url.URL {
	return &item.url
}

func (item BlockMapItem) Checksum() string {
	return item.checksum
}

func (item BlockMapItem) Num() uint64 {
	return item.num
}
