//go:build test
// +build test

package base

import (
	"time"

	"github.com/spikeekips/mitum/util"
	"github.com/spikeekips/mitum/util/hint"
	"github.com/spikeekips/mitum/util/localtime"
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

func (m DummyManifest) SuffrageBlock() util.Hash {
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

func (m *DummyManifest) SetSuffrageBlock(i util.Hash) *DummyManifest {
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
