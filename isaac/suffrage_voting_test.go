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
	"golang.org/x/exp/slices"
)

type dummySuffrageExpelPool struct {
	sync.RWMutex
	ops map[string]base.SuffrageExpelOperation
}

func newDummySuffrageExpelPool() *dummySuffrageExpelPool {
	return &dummySuffrageExpelPool{
		ops: map[string]base.SuffrageExpelOperation{},
	}
}

func (p *dummySuffrageExpelPool) SuffrageExpelOperation(height base.Height, node base.Address) (base.SuffrageExpelOperation, bool, error) {
	p.RLock()
	defer p.RUnlock()

	for i := range p.ops {
		op := p.ops[i]

		switch {
		case !op.ExpelFact().Node().Equal(node):
			continue
		case height > op.ExpelFact().ExpelEnd():
			continue
		default:
			return op, true, nil
		}
	}

	return nil, false, nil
}

func (p *dummySuffrageExpelPool) SetSuffrageExpelOperation(op base.SuffrageExpelOperation) error {
	p.Lock()
	defer p.Unlock()

	p.ops[op.Fact().Hash().String()] = op

	return nil
}

func (p *dummySuffrageExpelPool) TraverseSuffrageExpelOperations(
	ctx context.Context,
	height base.Height,
	callback SuffrageVoteFunc,
) error {
	p.RLock()
	defer p.RUnlock()

	for i := range p.ops {
		op := p.ops[i]
		fact := op.ExpelFact()
		if height > fact.ExpelEnd() {
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

func (p *dummySuffrageExpelPool) RemoveSuffrageExpelOperationsByFact(facts []base.SuffrageExpelFact) error {
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

func (p *dummySuffrageExpelPool) RemoveSuffrageExpelOperationsByHeight(height base.Height) error {
	p.RLock()
	defer p.RUnlock()

	for i := range p.ops {
		fact := p.ops[i].ExpelFact()

		if height < fact.ExpelEnd() {
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
	t.local = base.RandomLocalNode()
	t.networkID = util.UUID().Bytes()
}

func (t *testSuffrageVoting) operation(
	local base.LocalNode,
	height base.Height,
	node base.Address,
) SuffrageExpelOperation {
	fact := NewSuffrageExpelFact(node, height, height+1, util.UUID().String())
	op := NewSuffrageExpelOperation(fact)
	t.NoError(op.NodeSign(local.Privatekey(), t.networkID, local.Address()))

	return op
}

func (t *testSuffrageVoting) TestVote() {
	db := newDummySuffrageExpelPool()

	sv := NewSuffrageVoting(t.local.Address(), db,
		func(util.Hash) (bool, error) { return false, nil },
		func(base.SuffrageExpelOperation) error { return nil },
	)

	t.Run("voted; no previous vote", func() {
		height := base.Height(33)

		op := t.operation(t.local, height, base.RandomAddress(""))

		voted, err := sv.Vote(op)
		t.NoError(err)
		t.True(voted)
	})

	t.Run("false voted; local is expel node", func() {
		height := base.Height(33)

		newnode := base.RandomLocalNode()

		op := t.operation(newnode, height, t.local.Address())

		voted, err := sv.Vote(op)
		t.NoError(err)
		t.False(voted)
	})

	t.Run("false voted; already in state", func() {
		height := base.Height(33)
		op := t.operation(t.local, height, base.RandomAddress(""))

		sv.existsInState = func(h util.Hash) (bool, error) {
			return h.Equal(op.Fact().Hash()), nil
		}

		voted, err := sv.Vote(op)
		t.NoError(err)
		t.False(voted)
	})

	sv.existsInState = func(util.Hash) (bool, error) { return false, nil }

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

		newnode := base.RandomLocalNode()
		newop := t.operation(newnode, height, op.ExpelFact().Node())

		voted, err = sv.Vote(newop)
		t.NoError(err)
		t.True(voted)
	})
}

func (t *testSuffrageVoting) TestVoteCallback() {
	db := newDummySuffrageExpelPool()

	sv := NewSuffrageVoting(t.local.Address(), db,
		func(util.Hash) (bool, error) { return false, nil },
		func(base.SuffrageExpelOperation) error { return nil },
	)

	t.Run("voted and callback", func() {
		height := base.Height(33)

		var called bool

		sv.votedCallback = func(base.SuffrageExpelOperation) error {
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

		sv.votedCallback = func(base.SuffrageExpelOperation) error {
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
		local := base.RandomLocalNode()
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

	db := newDummySuffrageExpelPool()

	sv := NewSuffrageVoting(local.Address(), db,
		func(util.Hash) (bool, error) { return false, nil },
		func(base.SuffrageExpelOperation) error { return nil },
	)

	expelnode := localnodes[1].Address()

	op := t.operation(local, height, expelnode)

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

	db := newDummySuffrageExpelPool()

	sv := NewSuffrageVoting(local.Address(), db,
		func(util.Hash) (bool, error) { return false, nil },
		func(base.SuffrageExpelOperation) error { return nil },
	)

	expectedexpelnodes := localnodes[1:5] // 4 expel nodes
	threshold := base.DefaultThreshold.Threshold(uint(suf.Len()))
	minsigns := suf.Len() - len(expectedexpelnodes)

	t.T().Log("suffrage length:", suf.Len())
	t.T().Log("threshold:", threshold)
	t.T().Log("min signs:", minsigns)

	for i := range expectedexpelnodes {
		w := expectedexpelnodes[i].Address()

		op := t.operation(local, height, w)

		var expelnodesigns int

		for j := range localnodes[1:] { // all node signs
			node := localnodes[1:][j]

			if node.Address().Equal(w) {
				continue
			}

			if slices.IndexFunc(expectedexpelnodes, func(n base.LocalNode) bool {
				return n.Address().Equal(node.Address())
			}) >= 0 {
				expelnodesigns++
			}

			t.NoError(op.NodeSign(node.Privatekey(), t.networkID, node.Address()))

			if len(op.NodeSigns())-expelnodesigns == minsigns {
				break
			}
		}

		voted, err := sv.Vote(op)
		t.NoError(err)
		t.True(voted)
	}

	found, err := sv.Find(context.Background(), height, suf)
	t.NoError(err)
	t.Equal(len(expectedexpelnodes), len(found))

	sort.Slice(found, func(i, j int) bool {
		return strings.Compare(
			found[i].ExpelFact().Node().String(),
			found[j].ExpelFact().Node().String(),
		) < 0
	})

	sort.Slice(expectedexpelnodes, func(i, j int) bool {
		return strings.Compare(
			expectedexpelnodes[i].Address().String(),
			expectedexpelnodes[j].Address().String(),
		) < 0
	})

	for i := range found {
		a := found[i]
		b := expectedexpelnodes[i]

		t.True(a.ExpelFact().Node().Equal(b.Address()))
	}
}

func (t *testSuffrageVoting) TestFindMultipleOperationsWithInsufficientVotes() {
	height := base.Height(33)
	suf, localnodes := t.suffrage(10) // threshold=7

	local := localnodes[0]

	db := newDummySuffrageExpelPool()

	sv := NewSuffrageVoting(local.Address(), db,
		func(util.Hash) (bool, error) { return false, nil },
		func(base.SuffrageExpelOperation) error { return nil },
	)

	allexpelnodes := localnodes[1:5] // 4 expel nodes
	threshold := base.DefaultThreshold.Threshold(uint(suf.Len()))

	insufficients := allexpelnodes[:2]
	expectedexpelnodes := allexpelnodes[2:]

	minsigns := threshold

	t.T().Log("suffrage length:", suf.Len())
	t.T().Log("threshold:", threshold)
	t.T().Log("min signs:", minsigns)

	for i := range allexpelnodes {
		w := allexpelnodes[i].Address()

		op := t.operation(local, height, w)

		var expelnodesigns uint
		opminsigns := minsigns

		isininsufficients := slices.IndexFunc(insufficients, func(n base.LocalNode) bool {
			return n.Address().Equal(w)
		}) >= 0

		if isininsufficients {
			opminsigns = uint(suf.Len()-len(allexpelnodes)) - 1
		}

		for j := range localnodes[1:] { // all node signs
			node := localnodes[1:][j]

			if node.Address().Equal(w) {
				continue
			}

			if slices.IndexFunc(insufficients, func(n base.LocalNode) bool {
				return n.Address().Equal(node.Address())
			}) >= 0 {
				expelnodesigns++
			}

			t.NoError(op.NodeSign(node.Privatekey(), t.networkID, node.Address()))

			if uint(len(op.NodeSigns()))-expelnodesigns == opminsigns {
				break
			}
		}

		t.T().Logf("signs: signs=%d validsigns=%d minsigns=%d node=%s", len(op.NodeSigns()), func() int {
			signs := op.NodeSigns()

			expelnodes := expectedexpelnodes
			if isininsufficients {
				expelnodes = allexpelnodes
			}

			return util.CountFilteredSlice(signs, func(x base.NodeSign) bool {
				return slices.IndexFunc(expelnodes, func(n base.LocalNode) bool {
					return x.Node().Equal(n.Address())
				}) < 0
			})
		}(), opminsigns, w)

		voted, err := sv.Vote(op)
		t.NoError(err)
		t.True(voted)
	}

	found, err := sv.Find(context.Background(), height, suf)
	t.NoError(err)
	t.Equal(len(allexpelnodes)-len(insufficients), len(found))

	sort.Slice(found, func(i, j int) bool {
		return strings.Compare(
			found[i].ExpelFact().Node().String(),
			found[j].ExpelFact().Node().String(),
		) < 0
	})

	sort.Slice(expectedexpelnodes, func(i, j int) bool {
		return strings.Compare(
			expectedexpelnodes[i].Address().String(),
			expectedexpelnodes[j].Address().String(),
		) < 0
	})

	for i := range found {
		t.T().Logf("found: %d %s", i, found[i].ExpelFact().Node())
	}

	for i := range expectedexpelnodes {
		t.T().Logf("expected: %d %s", i, expectedexpelnodes[i].Address())
	}

	for i := range found {
		a := found[i]
		b := expectedexpelnodes[i]

		an := a.ExpelFact().Node()
		bn := b.Address()

		t.True(an.Equal(bn), "a=%s b=%s", an, bn)
	}
}

func TestSuffrageVoting(t *testing.T) {
	suite.Run(t, new(testSuffrageVoting))
}
