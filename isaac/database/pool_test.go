package database

import (
	"testing"

	"github.com/spikeekips/mitum/base"
	"github.com/spikeekips/mitum/isaac"
	"github.com/spikeekips/mitum/util"
	"github.com/spikeekips/mitum/util/valuehash"
	"github.com/stretchr/testify/suite"
)

type testPool struct {
	isaac.BaseTestBallots
	BaseTestDatabase
}

func (t *testPool) SetupTest() {
	t.BaseTestBallots.SetupTest()
	t.BaseTestDatabase.SetupTest()
}

func (t *testPool) TestProposal() {
	pst := t.NewPool()
	defer pst.Close()

	prfact := t.NewProposalFact(base.RawPoint(33, 44), t.Local, []util.Hash{valuehash.RandomSHA256(), valuehash.RandomSHA256()})
	pr := t.NewProposal(t.Local, prfact)

	issaved, err := pst.SetProposal(pr)
	t.NoError(err)
	t.True(issaved)

	t.Run("by hash", func() {
		upr, found, err := pst.Proposal(pr.Fact().Hash())
		t.NoError(err)
		t.True(found)

		base.EqualProposalSignedFact(t.Assert(), pr, upr)
	})

	t.Run("unknown hash", func() {
		upr, found, err := pst.Proposal(valuehash.RandomSHA256())
		t.NoError(err)
		t.False(found)
		t.Nil(upr)
	})

	t.Run("by point", func() {
		upr, found, err := pst.ProposalByPoint(prfact.Point(), prfact.Proposer())
		t.NoError(err)
		t.True(found)

		base.EqualProposalSignedFact(t.Assert(), pr, upr)
	})

	t.Run("unknown point", func() {
		upr, found, err := pst.ProposalByPoint(prfact.Point().Next(), prfact.Proposer())
		t.NoError(err)
		t.False(found)
		t.Nil(upr)
	})

	t.Run("unknown proposer", func() {
		upr, found, err := pst.ProposalByPoint(prfact.Point(), base.RandomAddress(""))
		t.NoError(err)
		t.False(found)
		t.Nil(upr)
	})
}

func (t *testPool) TestCleanOldProposals() {
	pst := t.NewPool()
	defer pst.Close()

	point := base.RawPoint(33, 44)

	oldpr := t.NewProposal(t.Local, t.NewProposalFact(point.Decrease(), t.Local, []util.Hash{valuehash.RandomSHA256(), valuehash.RandomSHA256()}))

	issaved, err := pst.SetProposal(oldpr)
	t.NoError(err)
	t.True(issaved)

	t.Run("old pr saved", func() {
		upr, found, err := pst.Proposal(oldpr.Fact().Hash())
		t.NoError(err)
		t.True(found)

		base.EqualProposalSignedFact(t.Assert(), oldpr, upr)
	})

	sameheightpr := t.NewProposal(t.Local, t.NewProposalFact(point.Prev(), t.Local, []util.Hash{valuehash.RandomSHA256(), valuehash.RandomSHA256()}))

	issaved, err = pst.SetProposal(sameheightpr)
	t.NoError(err)
	t.True(issaved)

	newpr := t.NewProposal(t.Local, t.NewProposalFact(point, t.Local, []util.Hash{valuehash.RandomSHA256(), valuehash.RandomSHA256()}))

	issaved, err = pst.SetProposal(newpr)
	t.NoError(err)
	t.True(issaved)

	t.Run("new pr saved", func() {
		upr, found, err := pst.Proposal(newpr.Fact().Hash())
		t.NoError(err)
		t.True(found)

		base.EqualProposalSignedFact(t.Assert(), newpr, upr)
	})

	t.Run("same height pr not cleaned", func() {
		upr, found, err := pst.Proposal(sameheightpr.Fact().Hash())
		t.NoError(err)
		t.True(found)

		base.EqualProposalSignedFact(t.Assert(), sameheightpr, upr)
	})

	t.Run("old pr cleaned", func() {
		upr, found, err := pst.Proposal(oldpr.Fact().Hash())
		t.NoError(err)
		t.False(found)
		t.Nil(upr)
	})
}

func TestPool(t *testing.T) {
	suite.Run(t, new(testPool))
}
