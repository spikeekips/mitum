package main

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"io"
	"slices"
	"sort"
	"sync"
	"time"

	"github.com/pkg/errors"
	"github.com/spikeekips/mitum/base"
	isaacblock "github.com/spikeekips/mitum/isaac/block"
	"github.com/spikeekips/mitum/util"
	"github.com/spikeekips/mitum/util/encoder"
	"github.com/spikeekips/mitum/util/hint"
	"github.com/spikeekips/mitum/util/localtime"
)

var BlockMapHint = hint.MustNewHint("chunked-blockmap-v0.0.1")

type blockFileChunk interface {
	URI() string
	Checksum() string
	CompressFormat() string
}

type baseBlockFileChunk struct {
	uri            string
	checksum       string
	compressFormat string
}

func newBaseBlockFileChunk(uri, checksum, compressFormat string) baseBlockFileChunk {
	return baseBlockFileChunk{
		uri:            uri,
		checksum:       checksum,
		compressFormat: compressFormat,
	}
}

func (c baseBlockFileChunk) URI() string            { return c.uri }
func (c baseBlockFileChunk) Checksum() string       { return c.checksum }
func (c baseBlockFileChunk) CompressFormat() string { return c.compressFormat }

func baseBlockMapItemChecksum(input []string) string {
	switch {
	case len(input) < 1:
		return ""
	case len(input) < 2:
		return input[0]
	}

	slices.Sort(input)

	w, d := defaultChecksumWrite()

	for i := range input {
		_, _ = w([]byte(input[i]))
	}

	return d()
}

type BlockMapItem interface {
	base.BlockMapItem
	IsChunked() bool
}

type BlockMap interface {
	base.NodeSign
	Manifest() base.Manifest
	Item(base.BlockItemType) (BlockMapItem, bool)
	Items(func(BlockMapItem) bool)
	ExpiredAt() time.Time
}

type BaseBlockMap struct {
	hint.BaseHinter
	base.BaseNodeSign
	manifest  base.Manifest
	items     map[base.BlockItemType]BlockMapItem
	expiredAt time.Time
}

func newBaseBlockMap(
	manifest base.Manifest,
	items map[base.BlockItemType]BlockMapItem,
	expired time.Duration,
) BaseBlockMap {
	return BaseBlockMap{
		BaseHinter: hint.NewBaseHinter(BlockMapHint),
		manifest:   manifest,
		items:      items,
		expiredAt:  localtime.Now().Add(expired),
	}
}

func (m BaseBlockMap) Manifest() base.Manifest {
	return m.manifest
}

func (m BaseBlockMap) Item(it base.BlockItemType) (BlockMapItem, bool) {
	i, found := m.items[it]

	return i, found
}

func (m BaseBlockMap) Items(f func(BlockMapItem) bool) {
	_ = TraverseMap(m.items, func(_ base.BlockItemType, v BlockMapItem) (bool, error) {
		if v == nil {
			return true, nil
		}

		return f(v), nil
	})
}

func (m BaseBlockMap) ExpiredAt() time.Time {
	return m.expiredAt
}

func (m BaseBlockMap) IsValid(b []byte) error {
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

	if err := TraverseMap(m.items, func(_ base.BlockItemType, v BlockMapItem) (bool, error) {
		if v == nil {
			return true, nil
		}

		return true, v.IsValid(nil)
	}); err != nil {
		return e.Wrap(err)
	}

	if err := m.BaseNodeSign.Verify(b, m.signedBytes()); err != nil {
		return e.Wrap(err)
	}

	if m.ExpiredAt().IsZero() {
		return e.Errorf("zero expired at")
	}

	return nil
}

func (m *BaseBlockMap) Sign(node base.Address, priv base.Privatekey, networkID base.NetworkID) error {
	sign, err := base.NewBaseNodeSignFromBytes(node, priv, networkID, m.signedBytes())
	if err != nil {
		return errors.Wrap(err, "sign blockmap")
	}

	m.BaseNodeSign = sign

	return nil
}

func (BaseBlockMap) Bytes() []byte {
	return nil
}

func (m BaseBlockMap) checkItems() error {
	check := func(it base.BlockItemType) bool {
		switch i, found := m.items[it]; {
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

func (m BaseBlockMap) signedBytes() []byte {
	var ts [][]byte

	_ = TraverseMap(m.items, func(_ base.BlockItemType, v BlockMapItem) (bool, error) {
		if v != nil {
			// NOTE only checksum will be included in signature
			ts = append(ts, []byte(v.Checksum()))
		}

		return true, nil
	})

	sort.Slice(ts, func(i, j int) bool {
		return bytes.Compare(ts[i], ts[j]) < 0
	})

	return util.ConcatByters(
		m.manifest.Hash(),
		util.BytesToByter(util.ConcatBytesSlice(ts...)),
	)
}

type blockMapJSONMarshaler struct {
	hint.BaseHinter
	base.BaseNodeSignJSONMarshaler
	Manifest  base.Manifest                       `json:"manifest"`
	Items     map[base.BlockItemType]BlockMapItem `json:"items"`
	ExpiredAt localtime.Time                      `json:"expired_at"`
}

func (m BaseBlockMap) MarshalJSON() ([]byte, error) {
	return util.MarshalJSON(blockMapJSONMarshaler{
		BaseHinter:                m.BaseHinter,
		BaseNodeSignJSONMarshaler: m.BaseNodeSign.JSONMarshaler(),
		Manifest:                  m.manifest,
		Items:                     m.items,
		ExpiredAt:                 localtime.New(m.expiredAt),
	})
}

type blockMapJSONUnmarshaler struct {
	Manifest  json.RawMessage                        `json:"manifest"`
	Items     map[base.BlockItemType]json.RawMessage `json:"items"`
	ExpiredAt localtime.Time                         `json:"expired_at"`
}

func (m *BaseBlockMap) DecodeJSON(b []byte, enc encoder.Encoder) error {
	e := util.StringError("decode blockmap")

	var u blockMapJSONUnmarshaler
	if err := enc.Unmarshal(b, &u); err != nil {
		return e.Wrap(err)
	}

	if err := m.BaseNodeSign.DecodeJSON(b, enc); err != nil {
		return e.WithMessage(err, "node sign")
	}

	if err := encoder.Decode(enc, u.Manifest, &m.manifest); err != nil {
		return e.WithMessage(err, "decode manifest")
	}

	m.items = map[base.BlockItemType]BlockMapItem{}

	if err := TraverseMap(u.Items, func(it base.BlockItemType, r json.RawMessage) (bool, error) {
		var ui BaseBlockMapItem
		if err := enc.Unmarshal(r, &ui); err != nil {
			return false, errors.WithMessagef(err, "unmarshal block item, %q", it)
		}

		m.items[it] = ui

		return true, nil
	}); err != nil {
		return e.Wrap(err)
	}

	m.expiredAt = u.ExpiredAt.Time

	return nil
}

type BaseBlockMapItem struct {
	isaacblock.BlockMapItem
	isChunked bool
}

func newBaseBlockMapItem(it base.BlockItemType, checksum string, isChunked bool) BaseBlockMapItem {
	return BaseBlockMapItem{
		BlockMapItem: isaacblock.NewBlockMapItem(it, checksum),
		isChunked:    isChunked,
	}
}

func (it BaseBlockMapItem) IsChunked() bool {
	return it.isChunked
}

type baseBlockMapItemJSONMarshaler struct {
	Type      base.BlockItemType `json:"type"`
	Checksum  string             `json:"checksum"`
	IsChunked bool               `json:"is_chunked"`
}

func (it BaseBlockMapItem) MarshalJSON() ([]byte, error) {
	return util.MarshalJSON(baseBlockMapItemJSONMarshaler{
		Type:      it.Type(),
		Checksum:  it.Checksum(),
		IsChunked: it.isChunked,
	})
}

func (it *BaseBlockMapItem) UnmarshalJSON(b []byte) error {
	var u baseBlockMapItemJSONMarshaler
	if err := util.UnmarshalJSON(b, &u); err != nil {
		return err
	}

	it.BlockMapItem = isaacblock.NewBlockMapItem(u.Type, u.Checksum)
	it.isChunked = u.IsChunked

	return nil
}

func defaultChecksumWrite() (ioWriteFunc, func() string) {
	h := sha256.New()

	return h.Write, func() string {
		defer h.Reset()

		switch b := h.Sum(nil); {
		case len(b) < 1:
			return ""
		default:
			return hex.EncodeToString(b)
		}
	}
}

func newChecksumWrite(write ioWriteFunc) (
	ioWriteFunc,
	func() string,
) {
	var l sync.RWMutex

	h, d := defaultChecksumWrite()

	w := io.MultiWriter(writeFunc(write), writeFunc(h))

	return func(b []byte) (int, error) {
			l.Lock()
			defer l.Unlock()

			return w.Write(b)
		},
		func() string {
			l.Lock()
			defer l.Unlock()

			return d()
		}
}
