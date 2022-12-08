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
	items    *util.SingleLockedMap[base.BlockMapItemType, base.BlockMapItem]
	base.BaseNodeSign
	hint.BaseHinter
	writer  hint.Hint
	encoder hint.Hint
}

func NewBlockMap(writer, encoder hint.Hint) BlockMap {
	return BlockMap{
		BaseHinter: hint.NewBaseHinter(BlockMapHint),
		writer:     writer,
		encoder:    encoder,
		items:      util.NewSingleLockedMap(base.BlockMapItemType(""), (base.BlockMapItem)(nil)),
	}
}

func (m BlockMap) IsValid(b []byte) error {
	e := util.ErrInvalid.Errorf("invalid blockmap")
	if err := m.BaseHinter.IsValid(BlockMapHint.Type().Bytes()); err != nil {
		return e.Wrap(err)
	}

	if err := util.CheckIsValiders(nil, false, m.writer, m.encoder, m.manifest, m.BaseNodeSign); err != nil {
		return e.Wrap(err)
	}

	if err := m.checkItems(); err != nil {
		return e.Wrap(err)
	}

	vs := make([]util.IsValider, m.items.Len())
	var i int
	m.items.Traverse(func(_ base.BlockMapItemType, v base.BlockMapItem) bool {
		if v != nil {
			vs[i] = v

			i++
		}

		return true
	})

	if err := util.CheckIsValiderSlice(nil, true, vs); err != nil {
		return e.Wrapf(err, "invalid item found")
	}

	if err := m.BaseNodeSign.Verify(b, m.signedBytes()); err != nil {
		return e.Wrap(err)
	}

	return nil
}

func (m BlockMap) Writer() hint.Hint {
	return m.writer
}

func (m BlockMap) Encoder() hint.Hint {
	return m.encoder
}

func (m BlockMap) Manifest() base.Manifest {
	return m.manifest
}

func (m *BlockMap) SetManifest(manifest base.Manifest) {
	m.manifest = manifest
}

func (m BlockMap) Item(t base.BlockMapItemType) (base.BlockMapItem, bool) {
	switch i, found := m.items.Value(t); {
	case !found, i == nil:
		return nil, false
	default:
		return i, true
	}
}

func (m *BlockMap) SetItem(item base.BlockMapItem) error {
	e := util.StringErrorFunc("failed to set blockmap item")

	if err := item.IsValid(nil); err != nil {
		return e(err, "")
	}

	_ = m.items.SetValue(item.Type(), item)

	return nil
}

func (m BlockMap) Items(f func(base.BlockMapItem) bool) {
	m.items.Traverse(func(_ base.BlockMapItemType, v base.BlockMapItem) bool {
		if v == nil {
			return true
		}

		return f(v)
	})
}

func (m *BlockMap) Sign(node base.Address, priv base.Privatekey, networkID base.NetworkID) error {
	sign, err := base.NewBaseNodeSignFromBytes(node, priv, networkID, m.signedBytes())
	if err != nil {
		return errors.Wrap(err, "failed to sign blockmap")
	}

	m.BaseNodeSign = sign

	return nil
}

func (m BlockMap) checkItems() error {
	check := func(t base.BlockMapItemType) bool {
		switch i, found := m.items.Value(t); {
		case !found, i == nil:
			return false
		default:
			return true
		}
	}

	if !check(base.BlockMapItemTypeProposal) {
		return util.ErrInvalid.Errorf("empty proposal")
	}

	if !check(base.BlockMapItemTypeVoteproofs) {
		return util.ErrInvalid.Errorf("empty voteproofs")
	}

	if m.manifest.OperationsTree() != nil {
		if !check(base.BlockMapItemTypeOperationsTree) {
			return util.ErrInvalid.Errorf("empty operations tree")
		}
	}

	if m.manifest.StatesTree() != nil {
		if !check(base.BlockMapItemTypeStatesTree) {
			return util.ErrInvalid.Errorf("empty states tree")
		}
	}

	return nil
}

func (BlockMap) Bytes() []byte {
	return nil
}

func (m BlockMap) signedBytes() []byte {
	ts := make([][]byte, m.items.Len())

	var i int
	m.items.Traverse(func(_ base.BlockMapItemType, v base.BlockMapItem) bool {
		if v != nil {
			// NOTE only checksum and num will be included in signature
			ts[i] = util.ConcatBytesSlice([]byte(v.Checksum()), util.Uint64ToBytes(v.Num()))

			i++
		}

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
	e := util.ErrInvalid.Errorf("invalid BlockMapItem")

	if err := item.t.IsValid(nil); err != nil {
		return e.Wrap(err)
	}

	if item.num < 1 {
		return e.Errorf("zero num")
	}

	if n := len(item.checksum); n < 1 {
		return e.Errorf("empty checksum")
	}

	switch {
	case len(item.url.String()) < 1:
		return e.Errorf("empty url")
	case len(item.url.Scheme) < 1:
		return e.Errorf("empty url scheme")
	default:
		scheme := strings.ToLower(item.url.Scheme)
		if _, found := supportedBlockMapItemURLSchemes[strings.ToLower(item.url.Scheme)]; !found {
			return e.Errorf("unsupported url scheme found, %q", scheme)
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
