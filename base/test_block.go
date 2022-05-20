//go:build test
// +build test

package base

import (
	"encoding/json"
	"time"

	"github.com/pkg/errors"
	"github.com/spikeekips/mitum/util"
	jsonenc "github.com/spikeekips/mitum/util/encoder/json"
	"github.com/spikeekips/mitum/util/hint"
	"github.com/spikeekips/mitum/util/localtime"
	"github.com/spikeekips/mitum/util/valuehash"
	"github.com/stretchr/testify/assert"
)

var DummyManifestHint = hint.MustNewHint("dummy-manifest-v0.0.1")

type DummyManifest struct {
	h          util.Hash
	height     Height
	prev       util.Hash
	proposal   util.Hash
	opstree    util.Hash
	statestree util.Hash
	suf        util.Hash
	proposedAt time.Time
	Invalidf   func([]byte) error
}

func NewDummyManifest(height Height, h util.Hash) DummyManifest {
	return DummyManifest{
		h:          h,
		height:     height,
		proposedAt: localtime.UTCNow(),
	}
}

func (m DummyManifest) Hint() hint.Hint {
	return DummyManifestHint
}

func (m DummyManifest) Hash() util.Hash {
	return m.h
}

func (m DummyManifest) Height() Height {
	return m.height
}

func (m DummyManifest) Previous() util.Hash {
	return m.prev
}

func (m DummyManifest) Proposal() util.Hash {
	return m.proposal
}

func (m DummyManifest) OperationsTree() util.Hash {
	return m.opstree
}

func (m DummyManifest) StatesTree() util.Hash {
	return m.statestree
}

func (m DummyManifest) Suffrage() util.Hash {
	return m.suf
}

func (m DummyManifest) ProposedAt() time.Time {
	return m.proposedAt
}

func (m DummyManifest) IsValid(b []byte) error {
	if m.Invalidf != nil {
		return m.Invalidf(b)
	}

	return nil
}

func (m *DummyManifest) SetHash(i util.Hash) *DummyManifest {
	m.h = i
	return m
}

func (m *DummyManifest) SetHeight(i Height) *DummyManifest {
	m.height = i
	return m
}

func (m *DummyManifest) SetPrevious(i util.Hash) *DummyManifest {
	m.prev = i
	return m
}

func (m *DummyManifest) SetProposal(i util.Hash) *DummyManifest {
	m.proposal = i
	return m
}

func (m *DummyManifest) SetOperationsTree(i util.Hash) *DummyManifest {
	m.opstree = i
	return m
}

func (m *DummyManifest) SetStatesTree(i util.Hash) *DummyManifest {
	m.statestree = i
	return m
}

func (m *DummyManifest) SetSuffrage(i util.Hash) *DummyManifest {
	m.suf = i
	return m
}

func (m *DummyManifest) SetProposedAt(i time.Time) *DummyManifest {
	m.proposedAt = i
	return m
}

type DummyManifestJSONMarshaler struct {
	hint.BaseHinter
	Hash           util.Hash      `json:"hash"`
	Height         Height         `json:"height"`
	PreviousBlock  util.Hash      `json:"previous_block"`
	Proposal       util.Hash      `json:"proposal"`
	OperationsTree util.Hash      `json:"operations_tree"`
	StatesTree     util.Hash      `json:"states_tree"`
	Suffrage       util.Hash      `json:"suffrage"`
	ProposedAt     localtime.Time `json:"proposed_at"`
}

func (m DummyManifest) MarshalJSON() ([]byte, error) {
	return util.MarshalJSON(DummyManifestJSONMarshaler{
		BaseHinter:     hint.NewBaseHinter(DummyManifestHint),
		Hash:           m.h,
		Height:         m.height,
		PreviousBlock:  m.prev,
		Proposal:       m.proposal,
		OperationsTree: m.opstree,
		StatesTree:     m.statestree,
		Suffrage:       m.suf,
		ProposedAt:     localtime.New(m.proposedAt),
	})
}

type DummyManifestJSONUnmarshaler struct {
	Hash           valuehash.HashDecoder `json:"hash"`
	Height         Height                `json:"height"`
	PreviousBlock  valuehash.HashDecoder `json:"previous_block"`
	Proposal       valuehash.HashDecoder `json:"proposal"`
	OperationsTree valuehash.HashDecoder `json:"operations_tree"`
	StatesTree     valuehash.HashDecoder `json:"states_tree"`
	Suffrage       valuehash.HashDecoder `json:"suffrage"`
	ProposedAt     localtime.Time        `json:"proposed_at"`
}

func (m *DummyManifest) UnmarshalJSON(b []byte) error {
	var u DummyManifestJSONUnmarshaler
	if err := util.UnmarshalJSON(b, &u); err != nil {
		return errors.Wrap(err, "failed to unmarshal DummyManifest")
	}

	m.h = u.Hash.Hash()
	m.height = u.Height
	m.prev = u.PreviousBlock.Hash()
	m.proposal = u.Proposal.Hash()
	m.opstree = u.OperationsTree.Hash()
	m.statestree = u.StatesTree.Hash()
	m.suf = u.Suffrage.Hash()
	m.proposedAt = u.ProposedAt.Time

	return nil
}

func EqualManifest(t *assert.Assertions, a, b Manifest) {
	isnil := func(name string, a, b interface{}) bool {
		if a != nil && b != nil {
			return false
		}

		if a != nil || b != nil {
			t.True(false, "%s; a=%v, b=%v", name, a == nil, b == nil)
		}

		return true
	}

	if isnil("manifest", a, b) {
		return
	}

	aht := a.(hint.Hinter).Hint()
	bht := b.(hint.Hinter).Hint()
	t.True(aht.Equal(bht), "Hint does not match")

	t.True(a.Hash().Equal(b.Hash()), "Hash does not match")
	t.Equal(a.Height(), b.Height(), "Height does not match")
	if !isnil("previous", a.Previous(), b.Previous()) {
		t.True(a.Previous().Equal(b.Previous()), "Previous does not match")
	}
	if !isnil("proposal", a.Proposal(), b.Proposal()) {
		t.True(a.Proposal().Equal(b.Proposal()), "proposal does not match")
	}
	if !isnil("OperationsTree", a.OperationsTree(), b.OperationsTree()) {
		t.True(a.OperationsTree().Equal(b.OperationsTree()), "OperationsTree does not match")
	}
	if !isnil("StatesTree", a.StatesTree(), b.StatesTree()) {
		t.True(a.StatesTree().Equal(b.StatesTree()), "StatesTree does not match")
	}
	if !isnil("Suffrage", a.Suffrage(), b.Suffrage()) {
		t.True(a.Suffrage().Equal(b.Suffrage()), "Suffrage does not match")
	}
	t.True(localtime.Equal(a.ProposedAt(), b.ProposedAt()), "ProposedAt does not match")
}

var (
	DummyBlockMapHint    = hint.MustNewHint("dummy-blockmap-v0.0.1")
	DummyBlockWriterHint = hint.MustNewHint("dummy-block-writer-v0.0.1")
)

type DummyBlockMap struct {
	BaseNodeSigned
	M Manifest
}

func NewDummyBlockMap(manifest Manifest) DummyBlockMap {
	signed, _ := BaseNodeSignedFromBytes(
		RandomAddress(""),
		NewMPrivatekey(),
		util.UUID().Bytes(),
		nil,
	)
	return DummyBlockMap{
		BaseNodeSigned: signed,
		M:              manifest,
	}
}

func (m DummyBlockMap) Hint() hint.Hint {
	return DummyBlockMapHint
}

func (m DummyBlockMap) Writer() hint.Hint {
	return DummyBlockWriterHint
}

func (m DummyBlockMap) Encoder() hint.Hint {
	return jsonenc.JSONEncoderHint
}

func (m DummyBlockMap) Manifest() Manifest {
	return m.M
}

func (m DummyBlockMap) Item(BlockMapItemType) (BlockMapItem, bool) {
	return nil, false
}

func (m DummyBlockMap) Items(func(BlockMapItem) bool) {
}

func (m DummyBlockMap) Bytes() []byte {
	return nil
}

func (m DummyBlockMap) IsValid([]byte) error {
	return nil
}

func (m DummyBlockMap) MarshalJSON() ([]byte, error) {
	return util.MarshalJSON(struct {
		hint.HintedJSONHead
		B BaseNodeSigned
		M Manifest
	}{
		HintedJSONHead: hint.NewHintedJSONHead(m.Hint()),
		B:              m.BaseNodeSigned,
		M:              m.M,
	})
}

func (m *DummyBlockMap) DecodeJSON(b []byte, enc *jsonenc.Encoder) error {
	var u struct {
		B json.RawMessage
		M json.RawMessage
	}

	if err := util.UnmarshalJSON(b, &u); err != nil {
		return err
	}

	if err := m.BaseSigned.DecodeJSON(u.B, enc); err != nil {
		return err
	}

	switch hinter, err := enc.Decode(u.M); {
	case err != nil:
		return err
	default:
		i, ok := hinter.(Manifest)
		if !ok {
			return errors.Errorf("not Manifest, %T", hinter)
		}

		m.M = i
	}

	return nil
}

func EqualBlockMap(t *assert.Assertions, a, b BlockMap) {
	aht := a.(hint.Hinter).Hint()
	bht := b.(hint.Hinter).Hint()
	t.True(aht.Equal(bht), "Hint does not match")

	EqualManifest(t, a.Manifest(), b.Manifest())

	a.Items(func(ai BlockMapItem) bool {
		bi, found := b.Item(ai.Type())
		t.True(found)

		EqualBlockMapItem(t, ai, bi)

		return true
	})
}

func EqualBlockMapItem(t *assert.Assertions, a, b BlockMapItem) {
	t.Equal(a.Type(), b.Type())
	t.Equal(a.URL().String(), b.URL().String())
	t.Equal(a.Checksum(), b.Checksum())
	t.Equal(a.Num(), b.Num())
}
