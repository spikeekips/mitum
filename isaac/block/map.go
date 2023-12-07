package isaacblock

import (
	"bytes"
	"sort"

	"github.com/pkg/errors"
	"github.com/spikeekips/mitum/base"
	"github.com/spikeekips/mitum/util"
	"github.com/spikeekips/mitum/util/hint"
)

var (
	BlockMapHint       = hint.MustNewHint("blockmap-v0.0.1")
	BlockItemFileHint  = hint.MustNewHint("block-item-file-v0.0.1")
	BlockItemFilesHint = hint.MustNewHint("block-item-files-v0.0.1")
)

type BlockMap struct {
	manifest base.Manifest
	items    *util.SingleLockedMap[base.BlockItemType, base.BlockMapItem]
	base.BaseNodeSign
	hint.BaseHinter
}

func NewBlockMap() BlockMap {
	return BlockMap{
		BaseHinter: hint.NewBaseHinter(BlockMapHint),
		items:      util.NewSingleLockedMap[base.BlockItemType, base.BlockMapItem](),
	}
}

func (m BlockMap) IsValid(b []byte) error {
	e := util.ErrInvalid.Errorf("invalid blockmap")
	if err := m.BaseHinter.IsValid(BlockMapHint.Type().Bytes()); err != nil {
		return e.Wrap(err)
	}

	if err := util.CheckIsValiders(nil, false, m.manifest, m.BaseNodeSign); err != nil {
		return e.Wrap(err)
	}

	if err := m.checkItems(); err != nil {
		return e.Wrap(err)
	}

	var vs []util.IsValider

	m.items.Traverse(func(_ base.BlockItemType, v base.BlockMapItem) bool {
		if v != nil {
			vs = append(vs, v)
		}

		return true
	})

	if err := util.CheckIsValiderSlice(nil, true, vs); err != nil {
		return e.WithMessage(err, "invalid item found")
	}

	if err := m.BaseNodeSign.Verify(b, m.signedBytes()); err != nil {
		return e.Wrap(err)
	}

	return nil
}

func (m BlockMap) Manifest() base.Manifest {
	return m.manifest
}

func (m *BlockMap) SetManifest(manifest base.Manifest) {
	m.manifest = manifest
}

func (m BlockMap) Item(t base.BlockItemType) (base.BlockMapItem, bool) {
	switch i, found := m.items.Value(t); {
	case !found, i == nil:
		return nil, false
	default:
		return i, true
	}
}

func (m *BlockMap) SetItem(item base.BlockMapItem) error {
	e := util.StringError("set block item")

	if err := item.IsValid(nil); err != nil {
		return e.Wrap(err)
	}

	_ = m.items.SetValue(item.Type(), item)

	return nil
}

func (m BlockMap) Items(f func(base.BlockMapItem) bool) {
	m.items.Traverse(func(_ base.BlockItemType, v base.BlockMapItem) bool {
		if v == nil {
			return true
		}

		return f(v)
	})
}

func (m *BlockMap) Sign(node base.Address, priv base.Privatekey, networkID base.NetworkID) error {
	sign, err := base.NewBaseNodeSignFromBytes(node, priv, networkID, m.signedBytes())
	if err != nil {
		return errors.Wrap(err, "sign blockmap")
	}

	m.BaseNodeSign = sign

	return nil
}

func (m BlockMap) checkItems() error {
	check := func(t base.BlockItemType) bool {
		switch i, found := m.items.Value(t); {
		case !found, i == nil:
			return false
		default:
			return true
		}
	}

	if !check(base.BlockItemProposal) {
		return util.ErrInvalid.Errorf("empty proposal")
	}

	if !check(base.BlockItemVoteproofs) {
		return util.ErrInvalid.Errorf("empty voteproofs")
	}

	if m.manifest.OperationsTree() != nil {
		if !check(base.BlockItemOperationsTree) {
			return util.ErrInvalid.Errorf("empty operations tree")
		}
	}

	if m.manifest.StatesTree() != nil {
		if !check(base.BlockItemStatesTree) {
			return util.ErrInvalid.Errorf("empty states tree")
		}
	}

	return nil
}

func (BlockMap) Bytes() []byte {
	return nil
}

func (m BlockMap) signedBytes() []byte {
	var ts [][]byte

	m.items.Traverse(func(_ base.BlockItemType, v base.BlockMapItem) bool {
		if v != nil {
			// NOTE only checksum will be included in signature
			ts = append(ts, []byte(v.Checksum()))
		}

		return true
	})

	if len(ts) > 0 {
		sort.Slice(ts, func(i, j int) bool {
			return bytes.Compare(ts[i], ts[j]) < 0
		})
	}

	return util.ConcatByters(
		m.manifest.Hash(),
		util.BytesToByter(util.ConcatBytesSlice(ts...)),
	)
}

type BlockMapItem struct {
	t        base.BlockItemType
	checksum string
}

func NewBlockMapItem(t base.BlockItemType, checksum string) BlockMapItem {
	return BlockMapItem{
		t:        t,
		checksum: checksum,
	}
}

func (item BlockMapItem) IsValid([]byte) error {
	e := util.ErrInvalid.Errorf("invalid BlockMapItem")

	if err := item.t.IsValid(nil); err != nil {
		return e.Wrap(err)
	}

	if n := len(item.checksum); n < 1 {
		return e.Errorf("empty checksum")
	}

	return nil
}

func (item BlockMapItem) Type() base.BlockItemType {
	return item.t
}

func (item BlockMapItem) Checksum() string {
	return item.checksum
}
