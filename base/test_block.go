//go:build test
// +build test

package base

import (
	"time"

	"github.com/spikeekips/mitum/util"
	"github.com/spikeekips/mitum/util/hint"
	"github.com/spikeekips/mitum/util/localtime"
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
	createdAt     time.Time
	nodeCreatedAt time.Time
}

func NewDummyManifest(height Height, h util.Hash) DummyManifest {
	return DummyManifest{
		h:             h,
		height:        height,
		createdAt:     localtime.Now(),
		nodeCreatedAt: localtime.Now(),
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

func (m DummyManifest) CreatedAt() time.Time {
	return m.createdAt
}

func (m DummyManifest) NodeCreatedAt() time.Time {
	return m.nodeCreatedAt
}

func (m DummyManifest) IsValid([]byte) error {
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

func (m *DummyManifest) SetCreatedAt(i time.Time) *DummyManifest {
	m.createdAt = i
	return m
}

func (m *DummyManifest) SetNodeCreatedAt(i time.Time) *DummyManifest {
	m.nodeCreatedAt = i
	return m
}

func CompareManifest(t *assert.Assertions, a, b Manifest) {
	isnil := func(name string, a, b interface{}) bool {
		if a != nil && b != nil {
			return false
		}

		if a != nil || b != nil {
			t.True(false, name)
		}

		return true
	}

	if isnil("manifest", a, b) {
		return
	}

	t.True(a.Hint().Equal(b.Hint()))
	t.True(a.Hash().Equal(b.Hash()))
	t.Equal(a.Height(), b.Height())
	if !isnil("previous", a.Previous(), b.Previous()) {
		t.True(a.Previous().Equal(b.Previous()))
	}
	if !isnil("proposal", a.Proposal(), b.Proposal()) {
		t.True(a.Proposal().Equal(b.Proposal()))
	}
	if !isnil("OperationsTree", a.OperationsTree(), b.OperationsTree()) {
		t.True(a.OperationsTree().Equal(b.OperationsTree()))
	}
	if !isnil("StatesTree", a.StatesTree(), b.StatesTree()) {
		t.True(a.StatesTree().Equal(b.StatesTree()))
	}
	if !isnil("Suffrage", a.Suffrage(), b.Suffrage()) {
		t.True(a.Suffrage().Equal(b.Suffrage()))
	}
	t.True(localtime.Equal(a.CreatedAt(), b.CreatedAt()))
	t.True(localtime.Equal(a.NodeCreatedAt(), b.NodeCreatedAt()))
}
