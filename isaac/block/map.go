package isaacblock

import (
	"bytes"
	"net/url"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"

	"github.com/pkg/errors"
	"github.com/spikeekips/mitum/base"
	"github.com/spikeekips/mitum/util"
	"github.com/spikeekips/mitum/util/encoder"
	"github.com/spikeekips/mitum/util/hint"
)

var (
	BlockMapHint       = hint.MustNewHint("blockmap-v0.0.1")
	BlockItemFileHint  = hint.MustNewHint("block-item-file-v0.0.1")
	BlockItemFilesHint = hint.MustNewHint("block-item-files-v0.0.1")
)

var LocalFSBlockItemScheme = "localfs"

var BlockDirectoryHeightFormat = "%021s"

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
	e := util.StringError("set blockmap item")

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

func NewLocalBlockMapItem(t base.BlockItemType, checksum string) BlockMapItem {
	return NewBlockMapItem(t, checksum)
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

type BlockItemFile struct {
	uri            url.URL
	compressFormat string
	hint.BaseHinter
}

func NewBlockItemFile(uri url.URL, compressFormat string) BlockItemFile {
	return BlockItemFile{
		BaseHinter:     hint.NewBaseHinter(BlockItemFileHint),
		uri:            uri,
		compressFormat: compressFormat,
	}
}

func NewLocalFSBlockItemFile(f string, compressFormat string) BlockItemFile {
	uri := url.URL{Scheme: LocalFSBlockItemScheme}

	if len(f) > 0 && !strings.HasPrefix(f, "/") {
		uri.Path = "/" + f
	}

	return NewBlockItemFile(uri, compressFormat)
}

func NewFileBlockItemFile(f string, compressFormat string) BlockItemFile {
	return NewBlockItemFile(url.URL{Scheme: "file", Path: f}, compressFormat)
}

func (f BlockItemFile) IsValid([]byte) error {
	if err := f.BaseHinter.IsValid(BlockItemFileHint.Type().Bytes()); err != nil {
		return err
	}

	switch {
	case len(f.uri.Scheme) < 1:
		return util.ErrInvalid.Errorf("empty uri scheme")
	case f.uri.Scheme == LocalFSBlockItemScheme, f.uri.Scheme == "file":
		if len(f.uri.Path) < 1 {
			return util.ErrInvalid.Errorf("empty filename in file uri")
		}
	}

	return nil
}

func (f BlockItemFile) URI() url.URL {
	return f.uri
}

func (f BlockItemFile) CompressFormat() string {
	if len(f.compressFormat) > 0 {
		return f.compressFormat
	}

	if s := f.uri.Scheme; s != LocalFSBlockItemScheme && s != "file" {
		return ""
	}

	// NOTE compress extension should be with original file extension
	switch lext := filepath.Ext(f.uri.Path); {
	case len(lext) < 1:
		return ""
	default:
		if len(filepath.Ext(f.uri.Path[0:len(f.uri.Path)-len(lext)])) < 1 {
			return ""
		}

		return lext[1:]
	}
}

type BlockItemFiles struct {
	items map[base.BlockItemType]base.BlockItemFile
	hint.BaseHinter
}

func NewBlockItemFiles(items map[base.BlockItemType]base.BlockItemFile) BlockItemFiles {
	if items == nil {
		items = map[base.BlockItemType]base.BlockItemFile{} //revive:disable-line:modifies-parameter
	}

	return BlockItemFiles{
		BaseHinter: hint.NewBaseHinter(BlockItemFilesHint),
		items:      items,
	}
}

func (f BlockItemFiles) IsValid([]byte) error {
	if len(f.items) < 1 {
		return util.ErrInvalid.Errorf("empty items")
	}

	if err := f.BaseHinter.IsValid(BlockItemFilesHint.Type().Bytes()); err != nil {
		return err
	}

	for i := range f.items {
		if err := i.IsValid(nil); err != nil {
			return err
		}

		if err := f.items[i].IsValid(nil); err != nil {
			return util.ErrInvalid.WithMessage(err, "item, %s", i)
		}
	}

	mustbe := []base.BlockItemType{
		base.BlockItemMap,
		base.BlockItemProposal,
		base.BlockItemVoteproofs,
	}

	for i := range mustbe {
		t := mustbe[i]

		if _, found := f.items[t]; !found {
			return util.ErrInvalid.Errorf("important item file, %v missing", t)
		}
	}

	return nil
}

func (f BlockItemFiles) Item(t base.BlockItemType) (base.BlockItemFile, bool) {
	i, found := f.items[t]

	return i, found
}

func (f BlockItemFiles) Items() map[base.BlockItemType]base.BlockItemFile {
	return f.items
}

type BlockItemFilesMaker struct {
	enc   encoder.Encoder
	files BlockItemFiles
	sync.RWMutex
}

func NewBlockItemFilesMaker(jsonenc encoder.Encoder) *BlockItemFilesMaker {
	return &BlockItemFilesMaker{
		files: NewBlockItemFiles(nil),
		enc:   jsonenc,
	}
}

func (f *BlockItemFilesMaker) Files() BlockItemFiles {
	f.RLock()
	defer f.RUnlock()

	return f.files
}

func (f *BlockItemFilesMaker) SetItem(t base.BlockItemType, i base.BlockItemFile) bool {
	f.Lock()
	defer f.Unlock()

	_, found := f.files.items[t]

	f.files.items[t] = i

	return !found
}

func BlockItemFilesPath(root string, height base.Height) string {
	return filepath.Join(
		filepath.Dir(filepath.Join(root, HeightDirectory(height))),
		base.BlockItemFilesName(height),
	)
}

func (f *BlockItemFilesMaker) Save(s string) error {
	switch i, err := os.OpenFile(filepath.Clean(s), os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0o600); {
	case err != nil:
		return errors.WithStack(err)
	default:
		defer func() {
			_ = i.Close()
		}()

		return f.enc.StreamEncoder(i).Encode(f.files)
	}
}
