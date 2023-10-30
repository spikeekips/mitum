package fixedtree

import (
	"context"
	"math"
	"testing"
	"time"

	"github.com/spikeekips/mitum/util"
	"github.com/stretchr/testify/suite"
)

type testGenerator struct {
	baseTestTree
}

func (t *testGenerator) TestNew() {
	for size := uint64(1); size < 1333; size++ {
		if size%3 != 0 {
			continue
		}

		nodes := t.nodesWithoutHash(int(size))

		started := time.Now()

		g, err := NewWriter(t.ht, size)
		t.NoError(err)

		worker, _ := util.NewErrgroupWorker(context.Background(), math.MaxInt32)
		defer worker.Close()

		for i := range nodes {
			i := i

			t.NoError(worker.NewJob(func(context.Context, uint64) error {
				return g.Add(uint64(i), nodes[i].(BaseNode))
			}))
		}
		worker.Done()

		t.NoError(worker.Wait())

		totalelapsed := time.Since(started)

		treestarted := time.Now()
		generated := make([]Node, len(nodes))
		t.NoError(g.Write(func(index uint64, n Node) error {
			generated[index] = n

			return nil
		}))

		t.T().Logf("size-%d: total=%v tree=%v", size, totalelapsed, time.Since(treestarted))

		newtr, err := NewTree(t.ht, generated)
		t.NoError(err)
		t.NoError(newtr.IsValid(nil))

		newtr.Traverse(func(index uint64, a Node) (bool, error) {
			t.True(a.Equal(generated[index]))

			return true, nil
		})
	}
}

func (t *testGenerator) TestZeroSize() {
	_, err := NewWriter(t.ht, 0)
	t.Error(err)
	t.ErrorContains(err, "zero size")
}

func (t *testGenerator) TestFailedToAdd() {
	g, err := NewWriter(t.ht, 3)
	t.NoError(err)

	nodes := t.nodesWithoutHash(1)

	err = g.Add(3, nodes[0].(BaseNode))
	t.Error(err)
	t.ErrorContains(err, "out of range")
}

func (t *testGenerator) TestEmptyKey() {
	size := 333
	g, err := NewWriter(t.ht, uint64(size))
	t.NoError(err)

	nodes := t.nodesWithoutHash(size)

	worker, _ := util.NewErrgroupWorker(context.Background(), math.MaxInt32)
	defer worker.Close()

	for i := range nodes {
		i := i

		t.NoError(worker.NewJob(func(context.Context, uint64) error {
			n := nodes[i].(BaseNode)
			n.key = ""

			return g.Add(uint64(i), n)
		}))
	}
	worker.Done()

	t.NoError(worker.Wait())

	err = g.Write(func(uint64, Node) error { return nil })
	t.Error(err)
	t.ErrorContains(err, "empty key")
}

func (t *testGenerator) TestWriter() {
	nodes := t.nodesWithoutHash(33)

	g, err := NewWriter(t.ht, uint64(len(nodes)))
	t.NoError(err)

	for i := range nodes {
		t.NoError(g.Add(uint64(i), nodes[i].(BaseNode)))
	}

	generated := make([]Node, len(nodes))

	var root Node
	w := func(index uint64, n Node) error {
		if index == 0 {
			root = n
		}
		generated[index] = n

		return nil
	}

	t.NoError(g.Write(w))

	nodes, err = generateNodesHash(nodes, func(uint64, Node) error { return nil })
	t.NoError(err)

	t.True(root.Hash().Equal(nodes[0].Hash()))

	for i := range nodes {
		t.True(nodes[i].Equal(generated[i]))
	}
}

func (t *testGenerator) TestShrinkEmptyNode() {
	nodes := t.nodesWithoutHash(33)

	g, err := NewWriter(t.ht, uint64(len(nodes))+3)
	t.NoError(err)

	var index uint64

	for i := range nodes {
		switch i {
		case 9, 11, 13:
			index++
		}

		t.NoError(g.Add(index, nodes[i].(BaseNode)))

		index++
	}

	t.T().Log("writer len, before write:", g.Len())

	generated := make([]Node, len(nodes))

	var root Node
	w := func(index uint64, n Node) error {
		if index == 0 {
			root = n
		}
		generated[index] = n

		return nil
	}

	t.NoError(g.Write(w))
	t.T().Log("writer len, after write:", g.Len())

	t.Equal(len(nodes), g.Len())

	nodes, err = generateNodesHash(nodes, func(uint64, Node) error { return nil })
	t.NoError(err)

	t.True(root.Hash().Equal(nodes[0].Hash()))

	for i := range nodes {
		t.True(nodes[i].Equal(generated[i]))
	}
}

func TestGenerator(t *testing.T) {
	suite.Run(t, new(testGenerator))
}
