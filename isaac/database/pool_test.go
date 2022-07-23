package isaacdatabase

import (
	"bytes"
	"context"
	"testing"

	"github.com/pkg/errors"
	"github.com/spikeekips/mitum/base"
	"github.com/spikeekips/mitum/isaac"
	"github.com/spikeekips/mitum/util"
	"github.com/spikeekips/mitum/util/encoder"
	"github.com/spikeekips/mitum/util/hint"
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

func (t *testPool) TestNew() {
	pst := t.NewPool()
	defer pst.Close()

	_ = (interface{})(pst).(isaac.ProposalPool)
	_ = (interface{})(pst).(isaac.NewOperationPool)
	_ = (interface{})(pst).(isaac.VoteproofsPool)
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
		upr, found, err := pst.ProposalByPoint(prfact.Point().NextHeight(), prfact.Proposer())
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

	oldpr := t.NewProposal(t.Local, t.NewProposalFact(point.PrevHeight().PrevHeight().PrevHeight(), t.Local, []util.Hash{valuehash.RandomSHA256(), valuehash.RandomSHA256()}))

	issaved, err := pst.SetProposal(oldpr)
	t.NoError(err)
	t.True(issaved)

	t.Run("old pr saved", func() {
		upr, found, err := pst.Proposal(oldpr.Fact().Hash())
		t.NoError(err)
		t.True(found)

		base.EqualProposalSignedFact(t.Assert(), oldpr, upr)
	})

	sameheightpr := t.NewProposal(t.Local, t.NewProposalFact(point.PrevRound(), t.Local, []util.Hash{valuehash.RandomSHA256(), valuehash.RandomSHA256()}))

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

type testNewOperationPool struct {
	isaac.BaseTestBallots
	BaseTestDatabase
	local     isaac.LocalNode
	networkID base.NetworkID
}

func (t *testNewOperationPool) SetupSuite() {
	t.BaseTestDatabase.SetupSuite()

	t.noerror(t.Enc.Add(encoder.DecodeDetail{Hint: isaac.DummyOperationFactHint, Instance: isaac.DummyOperationFact{}}))
	t.noerror(t.Enc.Add(encoder.DecodeDetail{Hint: isaac.DummyOperationHint, Instance: isaac.DummyOperation{}}))

	t.local = isaac.RandomLocalNode()
	t.networkID = util.UUID().Bytes()
}

func (t *testNewOperationPool) SetupTest() {
	t.BaseTestBallots.SetupTest()
	t.BaseTestDatabase.SetupTest()
}

func (t *testNewOperationPool) TestNewOperation() {
	pst := t.NewPool()
	defer pst.Close()

	_ = (interface{})(pst).(isaac.NewOperationPool)

	ops := make([]base.Operation, 33)
	for i := range ops {
		fact := isaac.NewDummyOperationFact(util.UUID().Bytes(), valuehash.RandomSHA256())
		op, _ := isaac.NewDummyOperation(fact, t.local.Privatekey(), t.networkID)

		ops[i] = op

		added, err := pst.SetNewOperation(context.Background(), op)
		t.NoError(err)
		t.True(added)
	}

	t.Run("check", func() {
		for i := range ops {
			op := ops[i]

			rop, found, err := pst.NewOperation(context.Background(), op.Hash())
			t.NoError(err)
			t.True(found)
			base.EqualOperation(t.Assert(), op, rop)
		}
	})

	t.Run("unknown", func() {
		op, found, err := pst.NewOperation(context.Background(), valuehash.RandomSHA256())
		t.NoError(err)
		t.False(found)
		t.Nil(op)
	})
}

func (t *testNewOperationPool) TestNewOperationHashes() {
	pst := t.NewPool()
	defer pst.Close()

	anotherDummyOperationFactHint := hint.MustNewHint("another-dummy-fact-v0.0.1")
	t.noerror(t.Enc.Add(encoder.DecodeDetail{Hint: anotherDummyOperationFactHint, Instance: isaac.DummyOperationFact{}}))

	ops := make([]base.Operation, 33)
	var anothers []base.Operation
	for i := range ops {
		fact := isaac.NewDummyOperationFact(util.UUID().Bytes(), valuehash.RandomSHA256())

		if i%10 == 0 {
			fact.UpdateHint(anotherDummyOperationFactHint)
		}

		op, _ := isaac.NewDummyOperation(fact, t.local.Privatekey(), t.networkID)

		ops[i] = op
		if i%10 == 0 {
			anothers = append(anothers, op)
		}

		added, err := pst.SetNewOperation(context.Background(), op)
		t.NoError(err)
		t.True(added)
	}

	t.Run("limit 10", func() {
		rops, err := pst.NewOperationHashes(context.Background(), 10, nil)
		t.NoError(err)
		t.Equal(10, len(rops))

		for i := range rops {
			op := ops[i].Hash()
			rop := rops[i]

			t.True(op.Equal(rop), "op=%q rop=%q", op, rop)
		}
	})

	t.Run("over 33", func() {
		rops, err := pst.NewOperationHashes(context.Background(), 100, nil)
		t.NoError(err)
		t.Equal(len(ops), len(rops))

		for i := range rops {
			op := ops[i].Hash()
			rop := rops[i]

			t.True(op.Equal(rop), "op=%q rop=%q", op, rop)
		}
	})

	t.Run("filter", func() {
		filter := func(_, facthash util.Hash, header isaac.PoolOperationHeader) (bool, error) {
			if facthash.Equal(ops[32].Fact().Hash()) {
				return false, nil
			}

			return true, nil
		}

		rops, err := pst.NewOperationHashes(context.Background(), 100, filter)
		t.NoError(err)
		t.Equal(len(ops)-1, len(rops))

		for i := range rops {
			op := ops[i].Hash()
			rop := rops[i]

			t.True(op.Equal(rop), "op=%q rop=%q", op, rop)
		}

		// NOTE ops[32] was removed
		op, found, err := pst.NewOperation(context.Background(), ops[32].Fact().Hash())
		t.NoError(err)
		t.False(found)
		t.Nil(op)
	})

	t.Run("filter error", func() {
		filter := func(_, facthash util.Hash, _ isaac.PoolOperationHeader) (bool, error) {
			if facthash.Equal(ops[31].Fact().Hash()) {
				return false, errors.Errorf("findme")
			}

			return true, nil
		}

		_, err := pst.NewOperationHashes(context.Background(), 100, filter)
		t.Error(err)
		t.ErrorContains(err, "findme")
	})

	t.Run("filter by header", func() {
		filter := func(_, _ util.Hash, header isaac.PoolOperationHeader) (bool, error) {
			// NOTE filter non-anotherDummyOperationFactHint
			return bytes.HasPrefix(
				header.HintBytes(),
				anotherDummyOperationFactHint.Bytes(),
			), nil
		}

		rops, err := pst.NewOperationHashes(context.Background(), 100, filter)
		t.NoError(err)
		t.Equal(len(anothers), len(rops))

		for i := range rops {
			op := anothers[i].Hash()
			rop := rops[i]

			t.True(op.Equal(rop), "op=%q rop=%q", op, rop)
		}
	})
}

func (t *testNewOperationPool) TestRemoveNewOperations() {
	pst := t.NewPool()
	defer pst.Close()

	ops := make([]base.Operation, 33)
	var removes []util.Hash
	for i := range ops {
		fact := isaac.NewDummyOperationFact(util.UUID().Bytes(), valuehash.RandomSHA256())
		op, _ := isaac.NewDummyOperation(fact, t.local.Privatekey(), t.networkID)

		ops[i] = op
		if i%3 == 0 {
			removes = append(removes, fact.Hash())
		}

		added, err := pst.SetNewOperation(context.Background(), op)
		t.NoError(err)
		t.True(added)
	}

	t.NoError(pst.RemoveNewOperations(context.Background(), removes))

	t.Run("all", func() {
		rops, err := pst.NewOperationHashes(context.Background(), 100, nil)
		t.NoError(err)
		t.Equal(len(ops)-len(removes), len(rops))

		var j int
		for i := range ops {
			if i%3 == 0 {
				continue
			}
			if i >= len(rops)-1 {
				break
			}

			op := ops[i].Hash()
			rop := rops[j]

			t.True(op.Equal(rop), "%d: op=%q rop=%q", i, op, rop)

			j++
		}
	})

	for i := range removes {
		h := removes[i]

		op, found, err := pst.NewOperation(context.Background(), h)
		t.NoError(err)
		t.False(found)
		t.Nil(op)
	}
}

func (t *testNewOperationPool) TestLastVoteproofs() {
	pst := t.NewPool()
	defer pst.Close()

	point := base.RawPoint(33, 0)

	pr := valuehash.RandomSHA256()
	_, ivp := t.VoteproofsPair(
		point.PrevRound(), point,
		valuehash.RandomSHA256(), valuehash.RandomSHA256(), pr,
		[]isaac.LocalNode{t.Local},
	)
	avp, _ := t.VoteproofsPair(
		point, point.NextHeight(),
		valuehash.RandomSHA256(), pr, valuehash.RandomSHA256(),
		[]isaac.LocalNode{t.Local},
	)

	t.T().Log("marshaled ivp:", string(util.MustMarshalJSON(ivp)))
	t.T().Log("marshaled avp:", string(util.MustMarshalJSON(avp)))

	t.Run("before set", func() {
		ivp, avp, found, err := pst.LastVoteproofs()
		t.NoError(err)
		t.False(found)
		t.Nil(ivp)
		t.Nil(avp)
	})

	t.NoError(pst.SetLastVoteproofs(ivp, avp))

	t.Run("after set", func() {
		rivp, ravp, found, err := pst.LastVoteproofs()
		t.NoError(err)
		t.True(found)
		t.NotNil(rivp)
		t.NotNil(ravp)

		base.EqualVoteproof(t.Assert(), ivp, rivp)
		base.EqualVoteproof(t.Assert(), avp, ravp)
	})
}

func TestNewOperationPool(t *testing.T) {
	suite.Run(t, new(testNewOperationPool))
}
