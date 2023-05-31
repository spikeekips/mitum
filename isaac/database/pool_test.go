package isaacdatabase

import (
	"bytes"
	"context"
	"sort"
	"testing"
	"time"

	"github.com/pkg/errors"
	"github.com/spikeekips/mitum/base"
	"github.com/spikeekips/mitum/isaac"
	"github.com/spikeekips/mitum/util"
	"github.com/spikeekips/mitum/util/encoder"
	"github.com/spikeekips/mitum/util/hint"
	"github.com/spikeekips/mitum/util/valuehash"
	"github.com/stretchr/testify/suite"
	leveldbutil "github.com/syndtr/goleveldb/leveldb/util"
)

type testPool struct {
	isaac.BaseTestBallots
	BaseTestDatabase
}

func (t *testPool) SetupSuite() {
	t.BaseTestDatabase.SetupSuite()

	t.noerror(t.Enc.Add(encoder.DecodeDetail{Hint: isaac.INITBallotSignFactHint, Instance: isaac.INITBallotSignFact{}}))
	t.noerror(t.Enc.Add(encoder.DecodeDetail{Hint: isaac.INITBallotFactHint, Instance: isaac.INITBallotFact{}}))
	t.noerror(t.Enc.Add(encoder.DecodeDetail{Hint: isaac.INITBallotHint, Instance: isaac.INITBallot{}}))
	t.noerror(t.Enc.Add(encoder.DecodeDetail{Hint: isaac.ACCEPTBallotSignFactHint, Instance: isaac.ACCEPTBallotSignFact{}}))
	t.noerror(t.Enc.Add(encoder.DecodeDetail{Hint: isaac.ACCEPTBallotFactHint, Instance: isaac.ACCEPTBallotFact{}}))
	t.noerror(t.Enc.Add(encoder.DecodeDetail{Hint: isaac.ACCEPTBallotHint, Instance: isaac.ACCEPTBallot{}}))
}

func (t *testPool) SetupTest() {
	t.BaseTestBallots.SetupTest()
}

func (t *testPool) TestNew() {
	pst := t.NewPool()
	defer pst.Close()

	_ = (interface{})(pst).(isaac.ProposalPool)
	_ = (interface{})(pst).(isaac.NewOperationPool)
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

		base.EqualProposalSignFact(t.Assert(), pr, upr)
	})

	t.Run("unknown hash", func() {
		upr, found, err := pst.Proposal(valuehash.RandomSHA256())
		t.NoError(err)
		t.False(found)
		t.Nil(upr)
	})

	t.Run("by point", func() {
		upr, found, err := pst.ProposalByPoint(prfact.Point(), prfact.Proposer(), prfact.PreviousBlock())
		t.NoError(err)
		t.True(found)

		base.EqualProposalSignFact(t.Assert(), pr, upr)
	})

	t.Run("unknown point", func() {
		upr, found, err := pst.ProposalByPoint(prfact.Point().NextHeight(), prfact.Proposer(), prfact.PreviousBlock())
		t.NoError(err)
		t.False(found)
		t.Nil(upr)
	})

	t.Run("unknown proposer", func() {
		upr, found, err := pst.ProposalByPoint(prfact.Point(), base.RandomAddress(""), prfact.PreviousBlock())
		t.NoError(err)
		t.False(found)
		t.Nil(upr)
	})

	t.Run("unknown previous block", func() {
		upr, found, err := pst.ProposalByPoint(prfact.Point(), prfact.Proposer(), valuehash.RandomSHA256())
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

		base.EqualProposalSignFact(t.Assert(), oldpr, upr)
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

		base.EqualProposalSignFact(t.Assert(), newpr, upr)
	})

	t.Run("same height pr not cleaned", func() {
		upr, found, err := pst.Proposal(sameheightpr.Fact().Hash())
		t.NoError(err)
		t.True(found)

		base.EqualProposalSignFact(t.Assert(), sameheightpr, upr)
	})

	removed, err := pst.cleanProposals()
	t.NoError(err)
	t.True(removed > 0)

	t.Run("old pr cleaned", func() {
		upr, found, err := pst.Proposal(oldpr.Fact().Hash())
		t.NoError(err)
		t.False(found)
		t.Nil(upr)
	})
}

func (t *testPool) TestBallot() {
	pst := t.NewPool()
	defer pst.Close()

	point := base.RawPoint(33, 44)

	var bl base.Ballot

	t.Run("set", func() {
		fact := t.NewINITBallotFact(point, valuehash.RandomSHA256(), valuehash.RandomSHA256())
		signfact := isaac.NewINITBallotSignFact(fact)
		t.NoError(signfact.NodeSign(t.Local.Privatekey(), t.LocalParams.NetworkID(), base.RandomAddress("")))

		bl = isaac.NewINITBallot(nil, signfact, nil)

		added, err := pst.SetBallot(bl)
		t.NoError(err)
		t.True(added)

		rbl, found, err := pst.Ballot(point, base.StageINIT, false)
		t.NoError(err)
		t.True(found)

		base.EqualBallot(t.Assert(), bl, rbl)

		rbl, found, err = pst.Ballot(point, base.StageINIT, true)
		t.NoError(err)
		t.False(found)
		t.Nil(rbl)
	})

	t.Run("set same point", func() {
		fact := t.NewINITBallotFact(point, valuehash.RandomSHA256(), valuehash.RandomSHA256())
		signfact := isaac.NewINITBallotSignFact(fact)
		t.NoError(signfact.NodeSign(t.Local.Privatekey(), t.LocalParams.NetworkID(), base.RandomAddress("")))

		samebl := isaac.NewINITBallot(nil, signfact, nil)

		added, err := pst.SetBallot(samebl)
		t.NoError(err)
		t.False(added)

		rbl, found, err := pst.Ballot(point, base.StageINIT, false)
		t.NoError(err)
		t.True(found)

		base.EqualBallot(t.Assert(), bl, rbl)
	})

	t.Run("set same point, higher stage", func() {
		fact := t.NewACCEPTBallotFact(point, valuehash.RandomSHA256(), valuehash.RandomSHA256())
		signfact := isaac.NewACCEPTBallotSignFact(fact)
		t.NoError(signfact.NodeSign(t.Local.Privatekey(), t.LocalParams.NetworkID(), base.RandomAddress("")))

		abl := isaac.NewACCEPTBallot(nil, signfact, nil)

		added, err := pst.SetBallot(abl)
		t.NoError(err)
		t.True(added)

		rbl, found, err := pst.Ballot(point, base.StageINIT, false)
		t.NoError(err)
		t.True(found)

		base.EqualBallot(t.Assert(), bl, rbl)

		rbl, found, err = pst.Ballot(point, base.StageACCEPT, false)
		t.NoError(err)
		t.True(found)

		base.EqualBallot(t.Assert(), abl, rbl)
	})

	t.Run("clean", func() {
		old := point.PrevHeight().PrevHeight().PrevHeight()

		fact := t.NewINITBallotFact(old, valuehash.RandomSHA256(), valuehash.RandomSHA256())
		signfact := isaac.NewINITBallotSignFact(fact)
		t.NoError(signfact.NodeSign(t.Local.Privatekey(), t.LocalParams.NetworkID(), base.RandomAddress("")))

		obl := isaac.NewINITBallot(nil, signfact, nil)

		added, err := pst.SetBallot(obl)
		t.NoError(err)
		t.True(added)

		rbl, found, err := pst.Ballot(old, base.StageINIT, false)
		t.NoError(err)
		t.True(found)

		base.EqualBallot(t.Assert(), obl, rbl)

		removed, err := pst.cleanBallots()
		t.NoError(err)
		t.True(removed > 0)

		rbl, found, err = pst.Ballot(old, base.StageINIT, false)
		t.NoError(err)
		t.False(found)
		t.Nil(rbl)
	})
}

func TestPool(t *testing.T) {
	suite.Run(t, new(testPool))
}

type testNewOperationPool struct {
	isaac.BaseTestBallots
	BaseTestDatabase
	local     base.LocalNode
	networkID base.NetworkID
}

func (t *testNewOperationPool) SetupSuite() {
	t.BaseTestDatabase.SetupSuite()

	t.noerror(t.Enc.Add(encoder.DecodeDetail{Hint: isaac.DummyOperationFactHint, Instance: isaac.DummyOperationFact{}}))
	t.noerror(t.Enc.Add(encoder.DecodeDetail{Hint: isaac.DummyOperationHint, Instance: isaac.DummyOperation{}}))

	t.local = base.RandomLocalNode()
	t.networkID = util.UUID().Bytes()
}

func (t *testNewOperationPool) SetupTest() {
	t.BaseTestBallots.SetupTest()
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

		added, err := pst.SetOperation(context.Background(), op)
		t.NoError(err)
		t.True(added)
	}

	t.Run("check", func() {
		for i := range ops {
			op := ops[i]

			rop, found, err := pst.Operation(context.Background(), op.Hash())
			t.NoError(err)
			t.True(found)
			base.EqualOperation(t.Assert(), op, rop)
		}
	})

	t.Run("unknown", func() {
		op, found, err := pst.Operation(context.Background(), valuehash.RandomSHA256())
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

		added, err := pst.SetOperation(context.Background(), op)
		t.NoError(err)
		t.True(added)
	}

	t.Run("limit 10", func() {
		rops, err := pst.OperationHashes(context.Background(), base.Height(33), 10, nil)
		t.NoError(err)
		t.Equal(10, len(rops))

		for i := range rops {
			op := ops[i].Hash()
			rop := rops[i]

			t.True(op.Equal(rop), "op=%q rop=%q", op, rop)
		}
	})

	t.Run("over 33", func() {
		rops, err := pst.OperationHashes(context.Background(), base.Height(33), 100, nil)
		t.NoError(err)
		t.Equal(len(ops), len(rops))

		for i := range rops {
			op := ops[i].Hash()
			rop := rops[i]

			t.True(op.Equal(rop), "op=%q rop=%q", op, rop)
		}
	})

	t.Run("filter", func() {
		filter := func(header isaac.PoolOperationRecordMeta) (bool, error) {
			if header.Fact().Equal(ops[32].Fact().Hash()) {
				return false, nil
			}

			return true, nil
		}

		rops, err := pst.OperationHashes(context.Background(), base.Height(33), 100, filter)
		t.NoError(err)
		t.Equal(len(ops)-1, len(rops))

		for i := range rops {
			op := ops[i].Hash()
			rop := rops[i]

			t.True(op.Equal(rop), "op=%q rop=%q", op, rop)
		}
	})

	t.Run("filter error", func() {
		filter := func(header isaac.PoolOperationRecordMeta) (bool, error) {
			if header.Fact().Equal(ops[31].Fact().Hash()) {
				return false, errors.Errorf("findme")
			}

			return true, nil
		}

		_, err := pst.OperationHashes(context.Background(), base.Height(33), 100, filter)
		t.Error(err)
		t.ErrorContains(err, "findme")
	})

	t.Run("filter by header", func() {
		filter := func(header isaac.PoolOperationRecordMeta) (bool, error) {
			// NOTE filter non-anotherDummyOperationFactHint
			return header.Hint().Type() == anotherDummyOperationFactHint.Type(), nil
		}

		rops, err := pst.OperationHashes(context.Background(), base.Height(33), 100, filter)
		t.NoError(err)
		t.Equal(len(anothers), len(rops))

		for i := range rops {
			op := anothers[i].Hash()
			rop := rops[i]

			t.True(op.Equal(rop), "op=%q rop=%q", op, rop)
		}
	})
}

func (t *testNewOperationPool) TestTraverseOperationsBytes() {
	pst := t.NewPool()
	defer pst.Close()

	ops := make([]base.Operation, 33)
	for i := range ops {
		fact := isaac.NewDummyOperationFact(util.UUID().Bytes(), valuehash.RandomSHA256())
		op, _ := isaac.NewDummyOperation(fact, t.local.Privatekey(), t.networkID)

		ops[i] = op

		added, err := pst.SetOperation(context.Background(), op)
		t.NoError(err)
		t.True(added)
	}

	var lastoffset []byte

	t.Run("traverse all", func() {
		var i int

		t.NoError(pst.TraverseOperationsBytes(context.Background(), nil, func(enchint hint.Hint, meta PoolOperationRecordMeta, body, offset []byte) (bool, error) {
			op := ops[i]

			t.True(enchint.Equal(t.Enc.Hint()))
			t.True(op.Hash().Equal(meta.Operation()))
			t.True(op.Fact().Hash().Equal(meta.Fact()))

			var rop base.Operation
			t.NoError(encoder.Decode(t.Enc, body, &rop))

			base.EqualOperation(t.Assert(), op, rop)

			lastoffset = offset

			i++

			return true, nil
		}))

		t.Equal(len(ops), i)
	})

	t.Run("offset", func() {
		var i int
		var half []byte

		t.NoError(pst.TraverseOperationsBytes(context.Background(), nil, func(_ hint.Hint, _ PoolOperationRecordMeta, _, offset []byte) (bool, error) {
			defer func() {
				i++
			}()

			if i >= len(ops)/2 {
				half = offset

				return false, nil
			}

			return true, nil
		}))

		t.T().Log("with offset")

		t.NoError(pst.TraverseOperationsBytes(context.Background(), half, func(_ hint.Hint, meta PoolOperationRecordMeta, body, offset []byte) (bool, error) {
			op := ops[i]

			t.True(op.Hash().Equal(meta.Operation()))
			t.True(op.Fact().Hash().Equal(meta.Fact()))

			var rop base.Operation
			t.NoError(encoder.Decode(t.Enc, body, &rop))

			base.EqualOperation(t.Assert(), op, rop)

			i++

			return true, nil
		}))

		t.Equal(len(ops), i)
	})

	t.Run("invalid offset", func() {
		var i int

		invalid := leveldbutil.BytesPrefix(lastoffset).Limit

		t.NoError(pst.TraverseOperationsBytes(context.Background(), invalid, func(_ hint.Hint, _ PoolOperationRecordMeta, _, offset []byte) (bool, error) {
			i++

			return true, nil
		}))

		t.Equal(0, i)
	})

	t.Run("callback error", func() {
		var i int

		err := pst.TraverseOperationsBytes(context.Background(), nil, func(_ hint.Hint, meta PoolOperationRecordMeta, body, offset []byte) (bool, error) {
			if i >= len(ops)/2 {
				return false, errors.Errorf("hihihi")
			}

			i++

			return true, nil
		})
		t.Error(err)
		t.ErrorContains(err, "hihihi")
		t.Equal(i, len(ops)/2)
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
			removes = append(removes, op.Hash())
		}

		added, err := pst.SetOperation(context.Background(), op)
		t.NoError(err)
		t.True(added)
	}

	t.NoError(pst.setRemoveNewOperations(context.Background(), base.Height(33), removes))

	t.Run("all", func() {
		rops, err := pst.OperationHashes(context.Background(), base.Height(33), 100, nil)
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

	t.Run("set removed, but still exists", func() {
		for i := range removes {
			h := removes[i]

			op, found, err := pst.Operation(context.Background(), h)
			t.NoError(err)
			t.True(found)
			t.NotNil(op)
		}
	})
}

func (t *testNewOperationPool) TestCleanNewOperations() {
	pst := t.NewPool()
	defer pst.Close()

	ops := make([]base.Operation, 33)
	for i := range ops {
		fact := isaac.NewDummyOperationFact(util.UUID().Bytes(), valuehash.RandomSHA256())

		op, _ := isaac.NewDummyOperation(fact, t.local.Privatekey(), t.networkID)

		ops[i] = op

		added, err := pst.SetOperation(context.Background(), op)
		t.NoError(err)
		t.True(added)
	}

	startheight := base.Height(3)

	filtered369 := []util.Hash{
		ops[3].Hash(),
		ops[6].Hash(),
		ops[9].Hash(),
	}

	filterf := func(hs []util.Hash) func(isaac.PoolOperationRecordMeta) (bool, error) {
		return func(header isaac.PoolOperationRecordMeta) (bool, error) {
			for i := range hs {
				if header.Operation().Equal(hs[i]) {
					return false, nil
				}
			}

			return true, nil
		}
	}

	sort.Slice(filtered369, func(i, j int) bool {
		return bytes.Compare(filtered369[i].Bytes(), filtered369[j].Bytes()) < 0
	})

	t.Run("filter 3", func() {
		rops, err := pst.OperationHashes(context.Background(), startheight, uint64(len(ops)), filterf(filtered369))
		t.NoError(err)
		t.Equal(len(ops)-len(filtered369), len(rops))

		var ropsindex int

		for i := range ops {
			op := ops[i].Hash()

			var found bool
			for i := range filtered369 {
				if op.Equal(filtered369[i]) {
					found = true
					break
				}
			}

			if found {
				continue
			}

			rop := rops[ropsindex]
			ropsindex++

			t.True(op.Equal(rop), "op=%q rop=%q", op, rop)
		}

		t.T().Log("exists in NewOperation")

		for i := range ops {
			h := ops[i].Hash()

			op, found, err := pst.Operation(context.Background(), h)
			t.NoError(err)
			t.True(found)
			t.True(h.Equal(op.Hash()))
		}
	})

	t.Run("filtered in removed", func() {
		var removed []util.Hash

		pst.st.Iter(
			leveldbutil.BytesPrefix(leveldbKeyPrefixRemovedNewOperation),
			func(_, b []byte) (bool, error) {
				removed = append(removed, valuehash.NewBytes(b))

				return true, nil
			},
			true,
		)

		sort.Slice(removed, func(i, j int) bool {
			return bytes.Compare(removed[i].Bytes(), removed[j].Bytes()) < 0
		})

		t.Equal(len(filtered369), len(removed))

		for i := range removed {
			a := filtered369[i]
			b := removed[i]

			t.True(a.Equal(b))
		}
	})

	t.Run("clean filtered; deep=1; not removed", func() {
		pst.cleanRemovedNewOperationsDeep = 1

		removed, err := pst.cleanRemovedNewOperations()
		t.NoError(err)
		t.Equal(0, removed)
	})

	filtered12 := []util.Hash{
		ops[12].Hash(),
	}

	t.Run("clean filtered; deep=1; fetch once with higher height", func() {
		rops, err := pst.OperationHashes(context.Background(), startheight+1, uint64(len(ops)), filterf(filtered12))
		t.NoError(err)
		t.Equal(len(ops)-len(filtered369)-len(filtered12), len(rops))

		pst.cleanRemovedNewOperationsDeep = 1

		removed, err := pst.cleanRemovedNewOperations()
		t.NoError(err)
		t.Equal(len(filtered369), removed)
	})

	t.Run("check left in new operations", func() {
		var left int
		pst.st.Iter(
			leveldbutil.BytesPrefix(leveldbKeyPrefixNewOperation),
			func(key, _ []byte) (bool, error) {
				left++

				return true, nil
			},
			true,
		)

		t.Equal(len(ops)-len(filtered369), left)
	})

	t.Run("check left in new operations ordered", func() {
		var left int
		pst.st.Iter(
			leveldbutil.BytesPrefix(leveldbKeyPrefixNewOperationOrdered),
			func(key, _ []byte) (bool, error) {
				left++

				return true, nil
			},
			true,
		)

		t.Equal(len(ops)-len(filtered369)-len(filtered12), left)
	})

	t.Run("check left in new operations keys key", func() {
		var left int
		pst.st.Iter(
			leveldbutil.BytesPrefix(leveldbKeyPrefixNewOperationOrderedKeys),
			func(key, _ []byte) (bool, error) {
				left++

				return true, nil
			},
			true,
		)

		t.Equal(len(ops)-len(filtered369)-len(filtered12), left)
	})

	t.Run("check left in new operations removed", func() {
		var left int
		pst.st.Iter(
			leveldbutil.BytesPrefix(leveldbKeyPrefixRemovedNewOperation),
			func(key, _ []byte) (bool, error) {
				left++

				return true, nil
			},
			true,
		)

		t.Equal(len(filtered12), left)
	})
}

func (t *testNewOperationPool) TestPeriodicCleanNewOperations() {
	pst := t.NewPool()
	defer pst.Close()

	ops := make([]base.Operation, 33)
	for i := range ops {
		fact := isaac.NewDummyOperationFact(util.UUID().Bytes(), valuehash.RandomSHA256())

		op, _ := isaac.NewDummyOperation(fact, t.local.Privatekey(), t.networkID)

		ops[i] = op

		added, err := pst.SetOperation(context.Background(), op)
		t.NoError(err)
		t.True(added)
	}

	filtered3 := []util.Hash{
		ops[3].Hash(),
		ops[6].Hash(),
		ops[9].Hash(),
	}

	filtered4 := []util.Hash{
		ops[4].Hash(),
		ops[7].Hash(),
		ops[10].Hash(),
	}

	filtered5 := []util.Hash{
		ops[5].Hash(),
		ops[8].Hash(),
		ops[11].Hash(),
	}

	filterf := func(hs []util.Hash) func(isaac.PoolOperationRecordMeta) (bool, error) {
		return func(h isaac.PoolOperationRecordMeta) (bool, error) {
			for i := range hs {
				if h.Operation().Equal(hs[i]) {
					return false, nil
				}
			}

			return true, nil
		}
	}

	t.Run("filter3", func() {
		rops, err := pst.OperationHashes(context.Background(), base.Height(3), uint64(len(ops)), filterf(filtered3))
		t.NoError(err)
		t.Equal(len(ops)-len(filtered3), len(rops))
	})

	t.Run("filter4", func() {
		rops, err := pst.OperationHashes(context.Background(), base.Height(4), uint64(len(ops)), filterf(filtered4))
		t.NoError(err)
		t.Equal(len(ops)-len(filtered3)-len(filtered4), len(rops))
	})

	pst.cleanRemovedNewOperationsInterval = time.Millisecond * 10
	pst.cleanRemovedNewOperationsDeep = 1

	removedch := make(chan int)
	pst.whenNewOperationsremoved = func(removed int, _ error) {
		removedch <- removed
	}

	t.NoError(pst.Start(context.Background()))

	select {
	case <-time.After(time.Second * 2):
		t.NoError(errors.Errorf("wait filtered3 removed, but nothing happened"))
	case removed := <-removedch:
		t.Equal(len(filtered3), removed)
	}

	t.Run("filter5", func() {
		rops, err := pst.OperationHashes(context.Background(), base.Height(5), uint64(len(ops)), filterf(filtered5))
		t.NoError(err)
		t.Equal(len(ops)-len(filtered3)-len(filtered4)-len(filtered5), len(rops))

		select {
		case <-time.After(time.Second * 2):
			t.NoError(errors.Errorf("wait filtered4 removed, but nothing happened"))
		case removed := <-removedch:
			t.Equal(len(filtered4), removed)
		}
	})
}

func TestNewOperationPool(t *testing.T) {
	suite.Run(t, new(testNewOperationPool))
}

type testSuffrageExpelPool struct {
	isaac.BaseTestBallots
	BaseTestDatabase
	local     base.LocalNode
	networkID base.NetworkID
}

func (t *testSuffrageExpelPool) SetupSuite() {
	t.BaseTestDatabase.SetupSuite()

	t.noerror(t.Enc.Add(encoder.DecodeDetail{Hint: isaac.SuffrageExpelOperationHint, Instance: isaac.SuffrageExpelOperation{}}))
	t.noerror(t.Enc.Add(encoder.DecodeDetail{Hint: isaac.SuffrageExpelFactHint, Instance: isaac.SuffrageExpelFact{}}))

	t.local = base.RandomLocalNode()
	t.networkID = util.UUID().Bytes()
}

func (t *testSuffrageExpelPool) SetupTest() {
	t.BaseTestBallots.SetupTest()
}

func (t *testSuffrageExpelPool) operation(
	start, end base.Height,
	node base.Address,
) isaac.SuffrageExpelOperation {
	fact := isaac.NewSuffrageExpelFact(node, start, end, util.UUID().String())
	op := isaac.NewSuffrageExpelOperation(fact)
	t.NoError(op.NodeSign(t.local.Privatekey(), t.networkID, t.local.Address()))

	return op
}

func (t *testSuffrageExpelPool) TestSet() {
	pst := t.NewPool()
	defer pst.Close()

	node := base.RandomAddress("")

	t.Run("set and get", func() {
		height := base.Height(33)

		rop, found, err := pst.SuffrageExpelOperation(height, node)
		t.NoError(err)
		t.False(found)
		t.Nil(rop)

		nop := t.operation(height, height+1, node)
		t.NoError(pst.SetSuffrageExpelOperation(nop))

		rop, found, err = pst.SuffrageExpelOperation(height, node)
		t.NoError(err)
		t.True(found)
		t.NotNil(rop)
		t.True(nop.Hash().Equal(rop.Hash()))
	})

	t.Run("filter", func() {
		height := base.Height(35)

		nop := t.operation(height, height, node)
		t.NoError(pst.SetSuffrageExpelOperation(nop))

		rop, found, err := pst.SuffrageExpelOperation(height, node)
		t.NoError(err)
		t.True(found)
		t.NotNil(rop)
		t.True(nop.Hash().Equal(rop.Hash()))
	})
}

func (t *testSuffrageExpelPool) TestRemoveByFact() {
	pst := t.NewPool()
	defer pst.Close()

	t.Run("set and remove", func() {
		height := base.Height(33)
		node := base.RandomAddress("")

		rop, found, err := pst.SuffrageExpelOperation(height, node)
		t.NoError(err)
		t.False(found)
		t.Nil(rop)

		nop0 := t.operation(height, height+1, node)
		t.NoError(pst.SetSuffrageExpelOperation(nop0))

		nop1 := t.operation(height, height+1, base.RandomAddress(""))
		t.NoError(pst.SetSuffrageExpelOperation(nop1))

		t.NoError(pst.RemoveSuffrageExpelOperationsByFact([]base.SuffrageExpelFact{nop0.ExpelFact()}))

		rop, found, err = pst.SuffrageExpelOperation(height, node)
		t.NoError(err)
		t.False(found)
		t.Nil(rop)

		rop, found, err = pst.SuffrageExpelOperation(height, nop1.ExpelFact().Node())
		t.NoError(err)
		t.True(found)
		t.NotNil(rop)
	})

	t.Run("remove unknowns", func() {
		height := base.Height(33)
		node := base.RandomAddress("")

		op := t.operation(height, height+1, node)
		t.NoError(pst.SetSuffrageExpelOperation(op))

		unknown := t.operation(height, height+1, node)

		t.NoError(pst.RemoveSuffrageExpelOperationsByFact([]base.SuffrageExpelFact{
			op.ExpelFact(),
			unknown.ExpelFact(),
		}))
	})
}

func (t *testSuffrageExpelPool) TestRemoveByHeight() {
	pst := t.NewPool()
	defer pst.Close()

	t.Run("remove", func() {
		baseheight := base.Height(33)

		ops := make([]base.SuffrageExpelOperation, 3)
		for i := range ops {
			op := t.operation(baseheight, baseheight+base.Height(int64(i)), base.RandomAddress(""))
			t.NoError(pst.SetSuffrageExpelOperation(op))

			ops[i] = op
		}

		height := ops[0].ExpelFact().ExpelEnd()

		var rops0 []base.SuffrageExpelOperation

		t.NoError(pst.TraverseSuffrageExpelOperations(context.Background(), height, func(op base.SuffrageExpelOperation) (bool, error) {
			rops0 = append(rops0, op)

			return true, nil
		}))
		t.Equal(len(ops), len(rops0))

		t.NoError(pst.RemoveSuffrageExpelOperationsByHeight(height))

		var rops1 []base.SuffrageExpelOperation

		t.NoError(pst.TraverseSuffrageExpelOperations(context.Background(), height, func(op base.SuffrageExpelOperation) (bool, error) {
			rops1 = append(rops1, op)

			return true, nil
		}))
		t.Equal(2, len(rops1))

		t.NoError(pst.RemoveSuffrageExpelOperationsByHeight(ops[2].ExpelFact().ExpelEnd()))

		var rops2 []base.SuffrageExpelOperation

		t.NoError(pst.TraverseSuffrageExpelOperations(context.Background(), height, func(op base.SuffrageExpelOperation) (bool, error) {
			rops2 = append(rops2, op)

			return true, nil
		}))
		t.Equal(0, len(rops2))
	})
}

func TestSuffrageExpelPool(t *testing.T) {
	suite.Run(t, new(testSuffrageExpelPool))
}
