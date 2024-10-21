package isaac

import (
	"bytes"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/pkg/errors"
	"github.com/spikeekips/mitum/base"
	"github.com/spikeekips/mitum/util"
	"github.com/spikeekips/mitum/util/encoder"
	"github.com/spikeekips/mitum/util/hint"
	"github.com/spikeekips/mitum/util/localtime"
	"github.com/spikeekips/mitum/util/valuehash"
)

var (
	ManifestHint       = hint.MustNewHint("manifest-v0.0.1")
	BlockItemFileHint  = hint.MustNewHint("block-item-file-v0.0.1")
	BlockItemFilesHint = hint.MustNewHint("block-item-files-v0.0.1")
)

type Manifest struct {
	proposedAt     time.Time
	statesTree     util.Hash
	h              util.Hash
	previous       util.Hash
	proposal       util.Hash
	operationsTree util.Hash
	suffrage       util.Hash
	hint.BaseHinter
	height base.Height
}

func NewManifest(
	height base.Height,
	previous,
	proposal,
	operationsTree,
	statesTree,
	suffrage util.Hash,
	proposedAt time.Time,
) Manifest {
	m := Manifest{
		BaseHinter:     hint.NewBaseHinter(ManifestHint),
		height:         height,
		previous:       previous,
		proposal:       proposal,
		operationsTree: operationsTree,
		statesTree:     statesTree,
		suffrage:       suffrage,
		proposedAt:     proposedAt,
	}

	m.h = m.generateHash()

	return m
}

func (m Manifest) IsValid([]byte) error {
	e := util.ErrInvalid.Errorf("invalid manifest")

	if err := m.BaseHinter.IsValid(ManifestHint.Type().Bytes()); err != nil {
		return e.Wrap(err)
	}

	if err := util.CheckIsValiders(nil, false,
		m.height,
		m.proposal,
		util.DummyIsValider(func([]byte) error {
			if m.proposedAt.IsZero() {
				return util.ErrInvalid.Errorf("empty proposedAt")
			}

			return nil
		}),
	); err != nil {
		return e.Wrap(err)
	}

	if m.height != base.GenesisHeight {
		if err := util.CheckIsValiders(nil, false, m.previous); err != nil {
			return e.Wrap(err)
		}
	}

	if err := util.CheckIsValiders(nil, true,
		m.operationsTree,
		m.statesTree,
		m.suffrage,
	); err != nil {
		return e.Wrap(err)
	}

	return nil
}

func (m Manifest) Hash() util.Hash {
	return m.h
}

func (m Manifest) Height() base.Height {
	return m.height
}

func (m Manifest) Previous() util.Hash {
	return m.previous
}

func (m Manifest) Proposal() util.Hash {
	return m.proposal
}

func (m Manifest) OperationsTree() util.Hash {
	return m.operationsTree
}

func (m Manifest) StatesTree() util.Hash {
	return m.statesTree
}

func (m Manifest) Suffrage() util.Hash {
	return m.suffrage
}

func (m Manifest) ProposedAt() time.Time {
	return m.proposedAt
}

func (m Manifest) generateHash() util.Hash {
	return valuehash.NewSHA256(util.ConcatByters(
		m.height,
		m.previous,
		m.proposal,
		m.operationsTree,
		m.statesTree,
		m.suffrage,
		localtime.New(m.proposedAt),
	))
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

func NewLocalFSBlockItemFile(f, compressFormat string) BlockItemFile {
	uri := url.URL{Scheme: LocalFSBlockItemScheme}

	if f != "" && !strings.HasPrefix(f, "/") {
		uri.Path = "/" + f
	}

	return NewBlockItemFile(uri, compressFormat)
}

func NewFileBlockItemFile(f, compressFormat string) BlockItemFile {
	uri := url.URL{Scheme: "file", Path: f}

	if f != "" && !strings.HasPrefix(f, "/") {
		uri.Path = "/" + f
	}

	return NewBlockItemFile(uri, compressFormat)
}

func (f BlockItemFile) IsValid([]byte) error {
	if err := f.BaseHinter.IsValid(BlockItemFileHint.Type().Bytes()); err != nil {
		return err
	}

	switch {
	case len(f.uri.Scheme) < 1:
		return util.ErrInvalid.Errorf("empty uri scheme")
	case IsInLocalBlockItemFile(f.uri):
		if len(f.uri.Path) < 1 {
			return util.ErrInvalid.Errorf("empty filename in file uri")
		}
	case len(f.uri.Host) < 1:
		return util.ErrInvalid.Errorf("empty hostname in remote uri")
	}

	return nil
}

func (f BlockItemFile) URI() url.URL {
	return f.uri
}

func (f BlockItemFile) CompressFormat() string {
	if f.compressFormat != "" {
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
	l     sync.RWMutex
}

func NewBlockItemFilesMaker(jsonenc encoder.Encoder) *BlockItemFilesMaker {
	return &BlockItemFilesMaker{
		files: NewBlockItemFiles(nil),
		enc:   jsonenc,
	}
}

func (f *BlockItemFilesMaker) Files() BlockItemFiles {
	f.l.RLock()
	defer f.l.RUnlock()

	return f.files
}

func (f *BlockItemFilesMaker) SetItem(t base.BlockItemType, i base.BlockItemFile) (bool, error) {
	f.l.Lock()
	defer f.l.Unlock()

	if err := i.IsValid(nil); err != nil {
		return false, err
	}

	_, found := f.files.items[t]

	f.files.items[t] = i

	return !found, nil
}

func (f *BlockItemFilesMaker) Bytes() ([]byte, error) {
	buf := bytes.NewBuffer(nil)

	if err := f.enc.StreamEncoder(buf).Encode(f.files); err != nil {
		return nil, err
	}

	return buf.Bytes(), nil
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
