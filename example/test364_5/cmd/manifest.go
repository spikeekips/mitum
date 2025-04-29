//go:generate go run -v -race ../tool/set_base_hinter.go - $GOFILE

package main

import (
	"time"

	"github.com/spikeekips/mitum/base"
	jsonenc "github.com/spikeekips/mitum/example/test364_5/encoder/json"
	dhash "github.com/spikeekips/mitum/example/test364_5/hash"
	dutil "github.com/spikeekips/mitum/example/test364_5/util"
	"github.com/spikeekips/mitum/util"
	"github.com/spikeekips/mitum/util/hint"
	"github.com/spikeekips/mitum/util/localtime"
)

var manifestHint = hint.MustNewHint("dop5-manifest-v0.0.1")

type manifest struct {
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

func newManifest(
	height base.Height,
	previous,
	proposal,
	operationsTree,
	statesTree,
	suffrage util.Hash,
	proposedAt time.Time,
) *manifest {
	m := &manifest{
		BaseHinter:     hint.NewBaseHinter(manifestHint),
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

func (m *manifest) IsValid([]byte) error {
	e := util.ErrInvalid.Errorf("invalid manifest")

	if err := m.BaseHinter.IsValid(manifestHint.Type().Bytes()); err != nil {
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

func (m *manifest) Hash() util.Hash {
	return m.h
}

func (m *manifest) Height() base.Height {
	return m.height
}

func (m *manifest) Previous() util.Hash {
	return m.previous
}

func (m *manifest) Proposal() util.Hash {
	return m.proposal
}

func (m *manifest) OperationsTree() util.Hash {
	return m.operationsTree
}

func (m *manifest) StatesTree() util.Hash {
	return m.statesTree
}

func (m *manifest) Suffrage() util.Hash {
	return m.suffrage
}

func (m *manifest) ProposedAt() time.Time {
	return m.proposedAt
}

func (m *manifest) generateHash() util.Hash {
	w, s := dhash.NewHex64Writer()

	w(m.height.Bytes())

	if m.previous != nil {
		w(m.previous.Bytes())
	}

	if m.previous != nil {
		w(m.proposal.Bytes())
	}

	if m.previous != nil {
		w(m.operationsTree.Bytes())
	}

	if m.previous != nil {
		w(m.statesTree.Bytes())
	}

	if m.previous != nil {
		w(m.suffrage.Bytes())
	}

	w(localtime.New(m.proposedAt).Bytes())

	return s()
}

type ManifestJSONMarshaler struct {
	ProposedAt     time.Time `json:"proposed_at"`
	StatesTree     util.Hash `json:"states_tree"`
	Hash           util.Hash `json:"hash"`
	Previous       util.Hash `json:"previous"`
	Proposal       util.Hash `json:"proposal"`
	OperationsTree util.Hash `json:"operations_tree"`
	Suffrage       util.Hash `json:"suffrage"`
	hint.BaseHinter
	Height base.Height `json:"height"`
}

func (m *manifest) MarshalJSON() ([]byte, error) {
	return util.MarshalJSON(ManifestJSONMarshaler{
		BaseHinter:     m.BaseHinter,
		Hash:           m.h,
		Height:         m.height,
		Previous:       m.previous,
		Proposal:       m.proposal,
		OperationsTree: m.operationsTree,
		StatesTree:     m.statesTree,
		Suffrage:       m.suffrage,
		ProposedAt:     m.proposedAt,
	})
}

var manifestHeightAssingJSON = dutil.NewFuncAssignJSONInts[int64]()

func decodeManifest(_ *jsonenc.Encoder, m map[string]any) (any, error) {
	mf := new(manifest)

	var height int64

	if err := dutil.FuncChain(
		dutil.AssignJSONHash(m["hash"], &mf.h),
		dutil.AssignJSONHash(m["previous"], &mf.previous),
		dutil.AssignJSONHash(m["proposal"], &mf.proposal),
		dutil.AssignJSONHash(m["operations_tree"], &mf.operationsTree),
		dutil.AssignJSONHash(m["states_tree"], &mf.statesTree),
		dutil.AssignJSONHash(m["suffrage"], &mf.suffrage),
		manifestHeightAssingJSON(m["height"], &height),
		func() error {
			var s string

			if err := dutil.AssignJSON(m["proposed_at"], &s)(); err != nil {
				return err
			}

			var t localtime.Time
			if err := t.UnmarshalText([]byte(s)); err != nil {
				return err
			}

			mf.proposedAt = t.Time

			return nil
		},
	); err != nil {
		return nil, err
	}

	mf.height = base.Height(height)

	return mf, nil
}
