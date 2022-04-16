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
	h             util.Hash
	height        Height
	prev          util.Hash
	proposal      util.Hash
	opstree       util.Hash
	statestree    util.Hash
	suf           util.Hash
	proposedAt    time.Time
	nodeCreatedAt time.Time
	Invalidf      func([]byte) error
}

func NewDummyManifest(height Height, h util.Hash) DummyManifest {
	return DummyManifest{
		h:             h,
		height:        height,
		proposedAt:    localtime.UTCNow(),
		nodeCreatedAt: localtime.UTCNow(),
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

func (m DummyManifest) NodeCreatedAt() time.Time {
	return m.nodeCreatedAt
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

func (m *DummyManifest) SetNodeCreatedAt(i time.Time) *DummyManifest {
	m.nodeCreatedAt = i
	return m
}

type DummyManifestJSONMarshaler struct {
	hint.BaseHinter
	H             util.Hash      `json:"hash"`
	Height        Height         `json:"height"`
	Prev          util.Hash      `json:"previous_block"`
	Proposal      util.Hash      `json:"proposal"`
	Opstree       util.Hash      `json:"operations_tree"`
	Statestree    util.Hash      `json:"states_tree"`
	Suf           util.Hash      `json:"suffrage"`
	ProposedAt    localtime.Time `json:"proposed_at"`
	NodeCreatedAt localtime.Time `json:"node_created_at"`
}

func (m DummyManifest) MarshalJSON() ([]byte, error) {
	return util.MarshalJSON(DummyManifestJSONMarshaler{
		BaseHinter:    hint.NewBaseHinter(DummyManifestHint),
		H:             m.h,
		Height:        m.height,
		Prev:          m.prev,
		Proposal:      m.proposal,
		Opstree:       m.opstree,
		Statestree:    m.statestree,
		Suf:           m.suf,
		ProposedAt:    localtime.New(m.proposedAt),
		NodeCreatedAt: localtime.New(m.nodeCreatedAt),
	})
}

type DummyManifestJSONUnmarshaler struct {
	H             valuehash.HashDecoder `json:"hash"`
	Height        Height                `json:"height"`
	Prev          valuehash.HashDecoder `json:"previous_block"`
	Proposal      valuehash.HashDecoder `json:"proposal"`
	Opstree       valuehash.HashDecoder `json:"operations_tree"`
	Statestree    valuehash.HashDecoder `json:"states_tree"`
	Suf           valuehash.HashDecoder `json:"suffrage"`
	ProposedAt    localtime.Time        `json:"proposed_at"`
	NodeCreatedAt localtime.Time        `json:"node_created_at"`
}

func (m *DummyManifest) UnmarshalJSON(b []byte) error {
	var u DummyManifestJSONUnmarshaler
	if err := util.UnmarshalJSON(b, &u); err != nil {
		return errors.Wrap(err, "failed to unmarshal DummyManifest")
	}

	m.h = u.H.Hash()
	m.height = u.Height
	m.prev = u.Prev.Hash()
	m.proposal = u.Proposal.Hash()
	m.opstree = u.Opstree.Hash()
	m.statestree = u.Statestree.Hash()
	m.suf = u.Suf.Hash()
	m.proposedAtt = u.ProposedAt.Time
	m.nodeCreatedAt = u.NodeCreatedAt.Time

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

	t.True(a.Hint().Equal(b.Hint()), "Hint does not match")
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
	t.True(localtime.Equal(a.NodeCreatedAt(), b.NodeCreatedAt()), "NodeCreatedAt does not match")
}

var DummyBlockDataMapHint = hint.MustNewHint("dummy-blockdatamap-v0.0.1")

type DummyBlockDataMap struct {
	BaseNodeSigned
	M Manifest
}

func NewDummyBlockDataMap(manifest Manifest) DummyBlockDataMap {
	signed, _ := BaseNodeSignedFromBytes(
		RandomAddress(""),
		NewMPrivatekey(),
		util.UUID().Bytes(),
		nil,
	)
	return DummyBlockDataMap{
		BaseNodeSigned: signed,
		M:              manifest,
	}
}

func (m DummyBlockDataMap) Hint() hint.Hint {
	return DummyBlockDataMapHint
}

func (m DummyBlockDataMap) Manifest() Manifest {
	return m.M
}

func (m DummyBlockDataMap) Item(BlockDataType) (BlockDataMapItem, bool) {
	return nil, false
}

func (m DummyBlockDataMap) All() map[BlockDataType]BlockDataMapItem {
	return nil
}

func (m DummyBlockDataMap) Bytes() []byte {
	return nil
}

func (m DummyBlockDataMap) IsValid([]byte) error {
	return nil
}

func (m DummyBlockDataMap) MarshalJSON() ([]byte, error) {
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

func (m *DummyBlockDataMap) DecodeJSON(b []byte, enc *jsonenc.Encoder) error {
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

func EqualBlockDataMap(t *assert.Assertions, a, b BlockDataMap) {
	t.True(a.Hint().Equal(b.Hint()))
	EqualManifest(t, a.Manifest(), b.Manifest())

	am := a.All()
	bm := b.All()

	t.Equal(len(am), len(bm))

	for k := range am {
		ai := am[k]
		bi := bm[k]

		EqualBlockDataMapItem(t, ai, bi)
	}
}

func EqualBlockDataMapItem(t *assert.Assertions, a, b BlockDataMapItem) {
	t.Equal(a.Type(), b.Type())
	t.Equal(a.URL().String(), b.URL().String())
	t.Equal(a.Checksum(), b.Checksum())
	t.Equal(a.Num(), b.Num())
}
