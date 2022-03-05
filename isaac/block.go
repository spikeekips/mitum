package isaac

import (
	"time"

	"github.com/spikeekips/mitum/base"
	"github.com/spikeekips/mitum/util"
	"github.com/spikeekips/mitum/util/hint"
	"github.com/spikeekips/mitum/util/localtime"
	"github.com/spikeekips/mitum/util/valuehash"
)

type Manifest struct {
	hint.BaseHinter
	h              util.Hash
	height         base.Height
	previous       util.Hash
	proposal       util.Hash
	operationsTree util.Hash
	statesTree     util.Hash
	suffrage       util.Hash
	createdAt      time.Time
	nodeCreatedAt  time.Time
}

func NewManifest(
	height base.Height,
	previous,
	proposal,
	operationsTree,
	statesTree,
	suffrage util.Hash,
	createdAt time.Time,
) Manifest {
	m := Manifest{
		height:         height,
		previous:       previous,
		proposal:       proposal,
		operationsTree: operationsTree,
		statesTree:     statesTree,
		suffrage:       suffrage,
		createdAt:      createdAt,
		nodeCreatedAt:  localtime.UTCNow(),
	}

	m.h = m.hash()

	return m
}

func (m Manifest) IsValid(networkID []byte) error {
	e := util.StringErrorFunc("invalid manifest")

	if err := util.CheckIsValid(networkID, false,
		m.BaseHinter,
		m.height,
		m.previous,
		m.proposal,
		util.DummyIsValider(func([]byte) error {
			if m.createdAt.IsZero() {
				return util.InvalidError.Errorf("empty createdAt")
			}

			return nil
		}),
		util.DummyIsValider(func([]byte) error {
			if m.nodeCreatedAt.IsZero() {
				return util.InvalidError.Errorf("empty nodeCreatedAt")
			}

			return nil
		}),
	); err != nil {
		return e(err, "")
	}

	if err := util.CheckIsValid(networkID, true,
		m.operationsTree,
		m.statesTree,
		m.suffrage,
	); err != nil {
		return e(err, "")
	}

	return nil
}

func (m Manifest) Hash() util.Hash {
	return m.h
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

func (m Manifest) CreatedAt() time.Time {
	return m.createdAt
}

func (m Manifest) NodeCreatedAt() time.Time {
	return m.nodeCreatedAt
}

func (m Manifest) hash() util.Hash {
	return valuehash.NewSHA256(util.ConcatByters(
		m.height,
		m.previous,
		m.proposal,
		m.operationsTree,
		m.statesTree,
		m.suffrage,
		localtime.New(m.createdAt),
		localtime.New(m.nodeCreatedAt),
	))
}
