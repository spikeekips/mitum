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

var BlockdataMapHint = hint.MustNewHint("blockdatamap-v0.0.1")

var (
	LocalBlockdataMapScheme             = "file+blockdata"
	supportedBlockdataMapItemURLSchemes = map[string]struct{}{
		LocalBlockdataMapScheme: {},
		"file":                  {},
		"http":                  {},
		"https":                 {},
	}
	fileBlockdataURL = url.URL{Scheme: LocalBlockdataMapScheme}
)

var BlockDirectoryHeightFormat = "%021s"

type BlockdataMap struct {
	hint.BaseHinter
	base.BaseNodeSigned
	writer   hint.Hint
	encoder  hint.Hint
	manifest base.Manifest
	m        *util.LockedMap
}

func NewBlockdataMap(writer, encoder hint.Hint) BlockdataMap {
	return BlockdataMap{
		BaseHinter: hint.NewBaseHinter(BlockdataMapHint),
		writer:     writer,
		encoder:    encoder,
		m:          util.NewLockedMap(),
	}
}

func (m BlockdataMap) IsValid(b []byte) error {
	e := util.StringErrorFunc("invalid blockdatamap")
	if err := m.BaseHinter.IsValid(BlockdataMapHint.Type().Bytes()); err != nil {
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

		vs[i] = v.(BlockdataMapItem)
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

func (m BlockdataMap) Manifest() base.Manifest {
	return m.manifest
}

func (m *BlockdataMap) SetManifest(manifest base.Manifest) {
	m.manifest = manifest
}

func (m BlockdataMap) Item(t base.BlockdataType) (base.BlockdataMapItem, bool) {
	switch i, found := m.m.Value(t); {
	case !found:
		return nil, false
	case i == nil:
		return nil, false
	case util.IsNilLockedValue(i):
		return nil, false
	default:
		return i.(BlockdataMapItem), true
	}
}

func (m *BlockdataMap) SetItem(item base.BlockdataMapItem) error {
	e := util.StringErrorFunc("failed to set blockdatamap item")

	if err := item.IsValid(nil); err != nil {
		return e(err, "")
	}

	_ = m.m.SetValue(item.Type(), item)

	return nil
}

func (m BlockdataMap) Items(f func(base.BlockdataMapItem) bool) {
	m.m.Traverse(func(_, v interface{}) bool {
		switch {
		case v == nil:
			return true
		case util.IsNilLockedValue(v):
			return true
		}

		return f(v.(base.BlockdataMapItem))
	})
}

func (m *BlockdataMap) Sign(node base.Address, priv base.Privatekey, networkID base.NetworkID) error {
	sign, err := base.BaseNodeSignedFromBytes(node, priv, networkID, m.signedBytes())
	if err != nil {
		return errors.Wrap(err, "failed to sign blockdatamap")
	}

	m.BaseNodeSigned = sign

	return nil
}

func (m BlockdataMap) checkItems() error {
	check := func(t base.BlockdataType) bool {
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

	if !check(base.BlockdataTypeProposal) {
		return util.InvalidError.Errorf("empty proposal")
	}

	if !check(base.BlockdataTypeVoteproofs) {
		return util.InvalidError.Errorf("empty voteproofs")
	}

	if m.manifest.OperationsTree() != nil {
		if !check(base.BlockdataTypeOperationsTree) {
			return util.InvalidError.Errorf("empty operations tree")
		}
	}

	if m.manifest.StatesTree() != nil {
		if !check(base.BlockdataTypeStatesTree) {
			return util.InvalidError.Errorf("empty states tree")
		}
	}

	return nil
}

func (BlockdataMap) Bytes() []byte {
	return nil
}

func (m BlockdataMap) signedBytes() []byte {
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
		item := v.(BlockdataMapItem)
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

type BlockdataMapItem struct {
	t        base.BlockdataType
	url      url.URL
	checksum string
	num      uint64
}

func NewBlockdataMapItem(t base.BlockdataType, u url.URL, checksum string, num uint64) BlockdataMapItem {
	return BlockdataMapItem{
		t:        t,
		url:      u,
		checksum: checksum,
		num:      num,
	}
}

func NewLocalBlockdataMapItem(t base.BlockdataType, checksum string, num uint64) BlockdataMapItem {
	return NewBlockdataMapItem(t, fileBlockdataURL, checksum, num)
}

func (item BlockdataMapItem) IsValid([]byte) error {
	e := util.StringErrorFunc("invalid blockdatamapItem")

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
		if _, found := supportedBlockdataMapItemURLSchemes[strings.ToLower(item.url.Scheme)]; !found {
			return e(util.InvalidError.Errorf("unsupported url scheme found, %q", scheme), "")
		}
	}

	return nil
}

func (item BlockdataMapItem) Type() base.BlockdataType {
	return item.t
}

func (item BlockdataMapItem) URL() *url.URL {
	return &item.url
}

func (item BlockdataMapItem) Checksum() string {
	return item.checksum
}

func (item BlockdataMapItem) Num() uint64 {
	return item.num
}
