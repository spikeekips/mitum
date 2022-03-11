package isaac

import (
	"testing"

	"github.com/spikeekips/mitum/base"
	"github.com/spikeekips/mitum/util"
	"github.com/spikeekips/mitum/util/valuehash"
	"github.com/stretchr/testify/suite"
)

type testPoolDatabase struct {
	baseTestHandler
	baseTestDatabase
}

func (t *testPoolDatabase) SetupTest() {
	t.baseTestHandler.SetupTest()
	t.baseTestDatabase.SetupTest()
}

func (t *testPoolDatabase) TestProposal() {
	pst := t.newPool()
	defer pst.Close()

	prfact := t.newProposalFact(base.RawPoint(33, 44), t.local, []util.Hash{valuehash.RandomSHA256(), valuehash.RandomSHA256()})
	pr := t.newProposal(t.local, prfact)

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

func (t *testPoolDatabase) TestCleanOldProposals() {
	pst := t.newPool()
	defer pst.Close()

	point := base.RawPoint(33, 44)

	oldpr := t.newProposal(t.local, t.newProposalFact(point.Decrease(), t.local, []util.Hash{valuehash.RandomSHA256(), valuehash.RandomSHA256()}))

	issaved, err := pst.SetProposal(oldpr)
	t.NoError(err)
	t.True(issaved)

	t.Run("old pr saved", func() {
		upr, found, err := pst.Proposal(oldpr.Fact().Hash())
		t.NoError(err)
		t.True(found)

		base.EqualProposalSignedFact(t.Assert(), oldpr, upr)
	})

	sameheightpr := t.newProposal(t.local, t.newProposalFact(point.Prev(), t.local, []util.Hash{valuehash.RandomSHA256(), valuehash.RandomSHA256()}))

	issaved, err = pst.SetProposal(sameheightpr)
	t.NoError(err)
	t.True(issaved)

	newpr := t.newProposal(t.local, t.newProposalFact(point, t.local, []util.Hash{valuehash.RandomSHA256(), valuehash.RandomSHA256()}))

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

func TestPoolDatabase(t *testing.T) {
	suite.Run(t, new(testPoolDatabase))
}
