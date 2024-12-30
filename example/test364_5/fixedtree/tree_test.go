package fixedtree

import (
	"context"
	"testing"

	dutil "github.com/spikeekips/mitum/example/test364_5/util"
	"github.com/spikeekips/mitum/util"
	"github.com/spikeekips/mitum/util/hint"
	"github.com/spikeekips/mitum/util/valuehash"
	"github.com/stretchr/testify/suite"
)

var treeHint = hint.MustNewHint("test-tree-v0.0.1")

func emptyTree() *Tree {
	return &Tree{BaseHinter: hint.NewBaseHinter(treeHint)}
}

type baseTestTree struct {
	dutil.BaseSuite
}

func (t *baseTestTree) nodesWithoutHash(n int) []Node {
	nodes := make([]Node, n)
	for i := range nodes {
		node := NewBaseNode(util.UUID().String())

		nodes[i] = node
	}

	return nodes
}

func (t *baseTestTree) nodes(n int) []Node {
	nodes := t.nodesWithoutHash(n)
	t.NoError(SyncronousGenerateNodesHash(context.Background(), nodes, func(uint64, Node) error { return nil }))

	return nodes
}

type testTree struct {
	baseTestTree
}

func (t *testTree) TestNew() {
	nodes := t.nodes(33)

	tr := NewTree(treeHint, nodes)
	t.NoError(tr.IsValid(nil))
	t.Equal(len(nodes), tr.Len())

	tr.Traverse(func(index uint64, b Node) (bool, error) {
		a := nodes[index]

		t.True(a.Equal(b))

		return true, nil
	})
}

func (t *testTree) TestInvalid() {
	t.Run("invalid node", func() {
		nodes := t.nodes(3)

		n := nodes[0].(*BaseNode)
		n.h = nil
		nodes[0] = n

		tr := NewTree(treeHint, nodes)
		err := tr.IsValid(nil)
		t.ErrorIs(err, util.ErrInvalid)
		t.ErrorContains(err, "nil node hash")
	})

	t.Run("nil node", func() {
		nodes := t.nodes(3)

		nodes[0] = nil

		tr := NewTree(treeHint, nodes)
		err := tr.IsValid(nil)
		t.ErrorIs(err, util.ErrInvalid)
		t.ErrorContains(err, "nil node")
	})

	t.Run("wrong node hash", func() {
		nodes := t.nodes(3)

		n := nodes[2].(*BaseNode)
		n.h = valuehash.RandomSHA256()
		nodes[2] = n

		tr := NewTree(treeHint, nodes)
		err := tr.IsValid(nil)
		t.ErrorIs(err, util.ErrInvalid)
		t.ErrorContains(err, "hash does not match")
	})
}

func TestTree(t *testing.T) {
	suite.Run(t, new(testTree))
}
