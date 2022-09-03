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
		rops, err := pst.NewOperationHashes(context.Background(), base.Height(33), 10, nil)
		t.NoError(err)
		t.Equal(10, len(rops))

		for i := range rops {
			op := ops[i].Hash()
			rop := rops[i]

			t.True(op.Equal(rop), "op=%q rop=%q", op, rop)
		}
	})

	t.Run("over 33", func() {
		rops, err := pst.NewOperationHashes(context.Background(), base.Height(33), 100, nil)
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

		rops, err := pst.NewOperationHashes(context.Background(), base.Height(33), 100, filter)
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

		_, err := pst.NewOperationHashes(context.Background(), base.Height(33), 100, filter)
		t.Error(err)
		t.ErrorContains(err, "findme")
	})

	t.Run("filter by header", func() {
		filter := func(header isaac.PoolOperationRecordMeta) (bool, error) {
			// NOTE filter non-anotherDummyOperationFactHint
			return header.Hint().Type() == anotherDummyOperationFactHint.Type(), nil
		}

		rops, err := pst.NewOperationHashes(context.Background(), base.Height(33), 100, filter)
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
			removes = append(removes, op.Hash())
		}

		added, err := pst.SetNewOperation(context.Background(), op)
		t.NoError(err)
		t.True(added)
	}

	t.NoError(pst.setRemoveNewOperations(context.Background(), base.Height(33), removes))

	t.Run("all", func() {
		rops, err := pst.NewOperationHashes(context.Background(), base.Height(33), 100, nil)
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

			op, found, err := pst.NewOperation(context.Background(), h)
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

		added, err := pst.SetNewOperation(context.Background(), op)
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
		rops, err := pst.NewOperationHashes(context.Background(), startheight, uint64(len(ops)), filterf(filtered369))
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

			op, found, err := pst.NewOperation(context.Background(), h)
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
		rops, err := pst.NewOperationHashes(context.Background(), startheight+1, uint64(len(ops)), filterf(filtered12))
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

		added, err := pst.SetNewOperation(context.Background(), op)
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
		rops, err := pst.NewOperationHashes(context.Background(), base.Height(3), uint64(len(ops)), filterf(filtered3))
		t.NoError(err)
		t.Equal(len(ops)-len(filtered3), len(rops))
	})

	t.Run("filter4", func() {
		rops, err := pst.NewOperationHashes(context.Background(), base.Height(4), uint64(len(ops)), filterf(filtered4))
		t.NoError(err)
		t.Equal(len(ops)-len(filtered3)-len(filtered4), len(rops))
	})

	pst.cleanRemovedNewOperationsInterval = time.Millisecond * 10
	pst.cleanRemovedNewOperationsDeep = 1

	removedch := make(chan int)
	pst.whenNewOperationsremoved = func(removed int, _ error) {
		removedch <- removed
	}

	t.NoError(pst.Start())

	select {
	case <-time.After(time.Second * 2):
		t.NoError(errors.Errorf("wait filtered3 removed, but nothing happened"))
	case removed := <-removedch:
		t.Equal(len(filtered3), removed)
	}

	t.Run("filter5", func() {
		rops, err := pst.NewOperationHashes(context.Background(), base.Height(5), uint64(len(ops)), filterf(filtered5))
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
