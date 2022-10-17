package isaac

import (
	"context"
	"sort"
	"strings"
	"sync"
	"testing"

	"github.com/spikeekips/mitum/base"
	"github.com/spikeekips/mitum/util"
	"github.com/stretchr/testify/suite"
)

type dummySuffrageWithdrawPool struct {
	sync.RWMutex
	ops map[string]base.SuffrageWithdrawOperation
}

func newDummySuffrageWithdrawPool() *dummySuffrageWithdrawPool {
	return &dummySuffrageWithdrawPool{
		ops: map[string]base.SuffrageWithdrawOperation{},
	}
}

func (p *dummySuffrageWithdrawPool) SuffrageWithdrawOperation(height base.Height, node base.Address) (base.SuffrageWithdrawOperation, bool, error) {
	p.RLock()
	defer p.RUnlock()

	for i := range p.ops {
		op := p.ops[i]

		switch {
		case !op.WithdrawFact().Node().Equal(node):
			continue
		case height > op.WithdrawFact().WithdrawEnd():
			continue
		default:
			return op, true, nil
		}
	}

	return nil, false, nil
}

func (p *dummySuffrageWithdrawPool) SetSuffrageWithdrawOperation(op base.SuffrageWithdrawOperation) error {
	p.Lock()
	defer p.Unlock()

	p.ops[op.Fact().Hash().String()] = op

	return nil
}

func (p *dummySuffrageWithdrawPool) TraverseSuffrageWithdrawOperations(
	ctx context.Context,
	height base.Height,
	callback func(base.SuffrageWithdrawOperation) (ok bool, err error),
) error {
	p.RLock()
	defer p.RUnlock()

	for i := range p.ops {
		op := p.ops[i]
		fact := op.WithdrawFact()
		if height > fact.WithdrawEnd() {
			continue
		}

		switch ok, err := callback(op); {
		case err != nil:
			return err
		case !ok:
			return nil
		}
	}

	return nil
}

func (p *dummySuffrageWithdrawPool) RemoveSuffrageWithdrawOperationsByFact(facts []base.SuffrageWithdrawFact) error {
	p.RLock()
	defer p.RUnlock()

	for i := range facts {
		h := facts[i].Hash()

		if _, found := p.ops[h.String()]; !found {
			continue
		}

		delete(p.ops, h.String())
	}

	return nil
}

func (p *dummySuffrageWithdrawPool) RemoveSuffrageWithdrawOperationsByHeight(height base.Height) error {
	p.RLock()
	defer p.RUnlock()

	for i := range p.ops {
		fact := p.ops[i].WithdrawFact()

		if height < fact.WithdrawEnd() {
			delete(p.ops, fact.Hash().String())
		}
	}

	return nil
}

type testSuffrageVoting struct {
	suite.Suite
	local     base.LocalNode
	networkID base.NetworkID
}

func (t *testSuffrageVoting) SetupSuite() {
	t.local = RandomLocalNode()
	t.networkID = util.UUID().Bytes()
}

func (t *testSuffrageVoting) operation(
	local base.LocalNode,
	height base.Height,
	node base.Address,
) SuffrageWithdrawOperation {
	fact := NewSuffrageWithdrawFact(node, height, height+1, util.UUID().String())
	op := NewSuffrageWithdrawOperation(fact)
	t.NoError(op.NodeSign(local.Privatekey(), t.networkID, local.Address()))

	return op
}

func (t *testSuffrageVoting) TestVote() {
	db := newDummySuffrageWithdrawPool()

	sv := NewSuffrageVoting(t.local.Address(), db, func(base.SuffrageWithdrawOperation) error { return nil })

	t.Run("voted; no previous vote", func() {
		height := base.Height(33)

		op := t.operation(t.local, height, base.RandomAddress(""))

		voted, err := sv.Vote(op)
		t.NoError(err)
		t.True(voted)
	})

	t.Run("false voted; local is withdraw node", func() {
		height := base.Height(33)

		newnode := RandomLocalNode()

		op := t.operation(newnode, height, t.local.Address())

		voted, err := sv.Vote(op)
		t.NoError(err)
		t.False(voted)
	})

	t.Run("false voted; already voted", func() {
		height := base.Height(33)
		op := t.operation(t.local, height, base.RandomAddress(""))

		voted, err := sv.Vote(op)
		t.NoError(err)
		t.True(voted)

		voted, err = sv.Vote(op)
		t.NoError(err)
		t.False(voted)
	})

	t.Run("voted; new signs", func() {
		height := base.Height(33)

		op := t.operation(t.local, height, base.RandomAddress(""))

		voted, err := sv.Vote(op)
		t.NoError(err)
		t.True(voted)

		newnode := RandomLocalNode()
		newop := t.operation(newnode, height, op.WithdrawFact().Node())

		voted, err = sv.Vote(newop)
		t.NoError(err)
		t.True(voted)
	})
}

func (t *testSuffrageVoting) TestVoteCallback() {
	db := newDummySuffrageWithdrawPool()

	sv := NewSuffrageVoting(t.local.Address(), db, func(base.SuffrageWithdrawOperation) error { return nil })

	t.Run("voted and callback", func() {
		height := base.Height(33)

		var called bool

		sv.votedCallback = func(base.SuffrageWithdrawOperation) error {
			called = true

			return nil
		}

		op := t.operation(t.local, height, base.RandomAddress(""))

		voted, err := sv.Vote(op)
		t.NoError(err)
		t.True(voted)
		t.True(called)
	})

	t.Run("false voted and callback", func() {
		height := base.Height(33)

		var called int

		sv.votedCallback = func(base.SuffrageWithdrawOperation) error {
			called++

			return nil
		}

		op := t.operation(t.local, height, base.RandomAddress(""))

		voted, err := sv.Vote(op)
		t.NoError(err)
		t.True(voted)
		t.Equal(1, called)

		voted, err = sv.Vote(op)
		t.NoError(err)
		t.False(voted)
		t.Equal(1, called)
	})
}

func (t *testSuffrageVoting) suffrage(n int) (base.Suffrage, []base.LocalNode) {
	localnodes := make([]base.LocalNode, n)
	nodes := make([]base.Node, n)

	for i := 0; i < n; i++ {
		local := RandomLocalNode()
		localnodes[i] = local
		nodes[i] = local
	}

	suf, err := NewSuffrage(nodes)
	t.NoError(err)

	return suf, localnodes
}

func (t *testSuffrageVoting) TestFindSingleOperation() {
	height := base.Height(33)
	suf, localnodes := t.suffrage(10) // threshold=7

	local := localnodes[0]

	db := newDummySuffrageWithdrawPool()

	sv := NewSuffrageVoting(local.Address(), db, func(base.SuffrageWithdrawOperation) error { return nil })

	withdrawnode := localnodes[1].Address()

	op := t.operation(local, height, withdrawnode)

	for i := range localnodes[2:] { // all node signs
		node := localnodes[2:][i]
		t.NoError(op.NodeSign(node.Privatekey(), t.networkID, node.Address()))
	}

	voted, err := sv.Vote(op)
	t.NoError(err)
	t.True(voted)

	found, err := sv.Find(context.Background(), height, suf)
	t.NoError(err)
	t.Equal(1, len(found))
	t.True(found[0].Hash().Equal(op.Hash()))
}

func (t *testSuffrageVoting) TestFindMultipleOperations() {
	height := base.Height(33)
	suf, localnodes := t.suffrage(10) // threshold=7

	local := localnodes[0]

	db := newDummySuffrageWithdrawPool()

	sv := NewSuffrageVoting(local.Address(), db, func(base.SuffrageWithdrawOperation) error { return nil })

	expectedwithdrawnodes := localnodes[1:5] // 4 withdraw nodes
	threshold := base.DefaultThreshold.Threshold(uint(suf.Len()))
	minsigns := suf.Len() - len(expectedwithdrawnodes)

	t.T().Log("suffrage length:", suf.Len())
	t.T().Log("threshold:", threshold)
	t.T().Log("min signs:", minsigns)

	for i := range expectedwithdrawnodes {
		w := expectedwithdrawnodes[i].Address()

		op := t.operation(local, height, w)

		var withdrawnodesigns int

		for j := range localnodes[1:] { // all node signs
			node := localnodes[1:][j]

			if node.Address().Equal(w) {
				continue
			}

			if util.InSliceFunc(expectedwithdrawnodes, func(_ interface{}, x int) bool {
				return expectedwithdrawnodes[x].Address().Equal(node.Address())
			}) >= 0 {
				withdrawnodesigns++
			}

			t.NoError(op.NodeSign(node.Privatekey(), t.networkID, node.Address()))

			if len(op.NodeSigns())-withdrawnodesigns == minsigns {
				break
			}
		}

		voted, err := sv.Vote(op)
		t.NoError(err)
		t.True(voted)
	}

	found, err := sv.Find(context.Background(), height, suf)
	t.NoError(err)
	t.Equal(len(expectedwithdrawnodes), len(found))

	sort.Slice(found, func(i, j int) bool {
		return strings.Compare(
			found[i].WithdrawFact().Node().String(),
			found[j].WithdrawFact().Node().String(),
		) < 0
	})

	sort.Slice(expectedwithdrawnodes, func(i, j int) bool {
		return strings.Compare(
			expectedwithdrawnodes[i].Address().String(),
			expectedwithdrawnodes[j].Address().String(),
		) < 0
	})

	for i := range found {
		a := found[i]
		b := expectedwithdrawnodes[i]

		t.True(a.WithdrawFact().Node().Equal(b.Address()))
	}
}

func (t *testSuffrageVoting) TestFindMultipleOperationsWithInsufficientVotes() {
	height := base.Height(33)
	suf, localnodes := t.suffrage(10) // threshold=7

	local := localnodes[0]

	db := newDummySuffrageWithdrawPool()

	sv := NewSuffrageVoting(local.Address(), db, func(base.SuffrageWithdrawOperation) error { return nil })

	allwithdrawnodes := localnodes[1:5] // 4 withdraw nodes
	threshold := base.DefaultThreshold.Threshold(uint(suf.Len()))

	insufficients := allwithdrawnodes[:2]
	expectedwithdrawnodes := allwithdrawnodes[2:]

	minsigns := threshold

	t.T().Log("suffrage length:", suf.Len())
	t.T().Log("threshold:", threshold)
	t.T().Log("min signs:", minsigns)

	for i := range allwithdrawnodes {
		w := allwithdrawnodes[i].Address()

		op := t.operation(local, height, w)

		var withdrawnodesigns uint
		opminsigns := minsigns

		isininsufficients := util.InSliceFunc(insufficients, func(_ interface{}, x int) bool {
			return insufficients[x].Address().Equal(w)
		}) >= 0

		if isininsufficients {
			opminsigns = uint(suf.Len()-len(allwithdrawnodes)) - 1
		}

		for j := range localnodes[1:] { // all node signs
			node := localnodes[1:][j]

			if node.Address().Equal(w) {
				continue
			}

			if util.InSliceFunc(insufficients, func(_ interface{}, x int) bool {
				return insufficients[x].Address().Equal(node.Address())
			}) >= 0 {
				withdrawnodesigns++
			}

			t.NoError(op.NodeSign(node.Privatekey(), t.networkID, node.Address()))

			if uint(len(op.NodeSigns()))-withdrawnodesigns == opminsigns {
				break
			}
		}

		t.T().Logf("signs: signs=%d validsigns=%d minsigns=%d node=%s", len(op.NodeSigns()), func() int {
			signs := op.NodeSigns()

			withdrawnodes := expectedwithdrawnodes
			if isininsufficients {
				withdrawnodes = allwithdrawnodes
			}

			return util.CountFilteredSlice(signs, func(_ interface{}, x int) bool {
				return util.InSliceFunc(withdrawnodes, func(_ interface{}, y int) bool {
					return signs[x].Node().Equal(withdrawnodes[y].Address())
				}) < 0
			})
		}(), opminsigns, w)

		voted, err := sv.Vote(op)
		t.NoError(err)
		t.True(voted)
	}

	found, err := sv.Find(context.Background(), height, suf)
	t.NoError(err)
	t.Equal(len(allwithdrawnodes)-len(insufficients), len(found))

	sort.Slice(found, func(i, j int) bool {
		return strings.Compare(
			found[i].WithdrawFact().Node().String(),
			found[j].WithdrawFact().Node().String(),
		) < 0
	})

	sort.Slice(expectedwithdrawnodes, func(i, j int) bool {
		return strings.Compare(
			expectedwithdrawnodes[i].Address().String(),
			expectedwithdrawnodes[j].Address().String(),
		) < 0
	})

	for i := range found {
		t.T().Logf("found: %d %s", i, found[i].WithdrawFact().Node())
	}

	for i := range expectedwithdrawnodes {
		t.T().Logf("expected: %d %s", i, expectedwithdrawnodes[i].Address())
	}

	for i := range found {
		a := found[i]
		b := expectedwithdrawnodes[i]

		an := a.WithdrawFact().Node()
		bn := b.Address()

		t.True(an.Equal(bn), "a=%s b=%s", an, bn)
	}
}

func TestSuffrageVoting(t *testing.T) {
	suite.Run(t, new(testSuffrageVoting))
}
