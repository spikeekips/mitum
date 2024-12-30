package fixedtree

import (
	"context"
	"math"
	"testing"
	"time"

	"github.com/pkg/errors"
	"github.com/spikeekips/mitum/util"
	jsonenc "github.com/spikeekips/mitum/util/encoder/json"
	"github.com/stretchr/testify/suite"
)

type testWrite struct {
	baseTestTree
	enc *jsonenc.Encoder
}

func (t *testWrite) TestSuite() {
	t.enc = jsonenc.NewEncoder()
}

func (t *testWrite) TestWriteNodes() {
	size := 1 << 14
	nodes := t.nodesWithoutHash(size)

	worker, _ := util.NewBaseJobWorker(context.Background(), math.MaxInt32)
	defer worker.Close()

	written := make([]Node, len(nodes))

	started := time.Now()

	t.NoError(WriteNodes(
		context.Background(),
		1<<9,
		nodes,
		func(index uint64, n Node) error {
			return worker.NewJob(func(context.Context, uint64) error {
				written[index] = n

				return nil
			},
			)
		},
		SyncronousGenerateNodesHash,
	))

	worker.Done()
	t.NoError(worker.Wait())

	totalelapsed := time.Since(started)

	t.T().Logf("size-%d: total=%v", size, totalelapsed)

	tr := NewTree(treeHint, written)
	t.NoError(tr.IsValid(nil))

	tr.Traverse(func(index uint64, a Node) (bool, error) {
		t.True(a.Equal(nodes[index]))

		return true, nil
	})
}

func (t *testWrite) TestError() {
	t.Run("empty nodes", func() {
		t.NoError(WriteNodes(context.Background(), 1<<9, nil, nil, nil))
	})

	t.Run("root empty node", func() {
		nodes := t.nodes(3)
		nodes[0] = nil

		err := WriteNodes(context.Background(), 1<<9, nodes, nil, nil)
		t.Error(err)
		t.ErrorContains(err, "nil root")
	})

	t.Run("nil node", func() {
		nodes := t.nodes(9)
		nodes[1] = nil

		err := WriteNodes(context.Background(), 1<<9,
			nodes,
			func(uint64, Node) error { return nil },
			nil,
		)
		t.Error(err)
		t.ErrorContains(err, "nil node")
	})

	t.Run("nil root hash, nil node", func() {
		nodes := t.nodesWithoutHash(9)
		nodes[1] = nil

		err := WriteNodes(context.Background(), 1<<9,
			nodes,
			func(uint64, Node) error { return nil },
			SyncronousGenerateNodesHash,
		)
		t.Error(err)
		t.ErrorContains(err, "nil node")
	})

	t.Run("nil root hash, empty key", func() {
		nodes := t.nodesWithoutHash(9)
		nodes[1] = NewBaseNode("")

		err := WriteNodes(context.Background(), 1<<9,
			nodes,
			func(uint64, Node) error { return nil },
			SyncronousGenerateNodesHash,
		)
		t.Error(err)
		t.ErrorContains(err, "empty key")
	})
}

func (t *testWrite) TestAsyncGenerateNodesHashByFollowingParent() {
	t.RunWithGoLeak("ok", func() {
		t.NoError(AsyncGenerateNodesHashByFollowingParent(
			context.Background(),
			t.nodesWithoutHash(3),
			1<<14,
			nil,
		))
	})

	t.RunWithGoLeak("already hashed", func() {
		nodes := t.nodesWithoutHash(3)
		nodes[1] = t.nodes(1)[0]
		err := AsyncGenerateNodesHashByFollowingParent(
			context.Background(),
			nodes,
			1<<14,
			nil,
		)
		t.Error(err)
		t.ErrorContains(err, "already hashed")
	})

	t.RunWithGoLeak("cancel before", func() {
		ctx, cancel := t.Ctx()
		cancel(nil)

		err := AsyncGenerateNodesHashByFollowingParent(
			ctx,
			t.nodesWithoutHash(9),
			1<<14,
			nil,
		)
		t.ErrorIs(err, context.Canceled)
	})

	t.RunWithGoLeak("cancel during generating", func() {
		ctx, cancel := t.Ctx()

		err := AsyncGenerateNodesHashByFollowingParent(
			ctx,
			t.nodesWithoutHash(9),
			1<<14,
			func(index uint64, _ Node) error {
				if index == 3 {
					cancel(errors.Errorf("findme"))
				}

				return nil
			},
		)
		t.ErrorIs(err, context.Canceled)
		t.ErrorContains(err, "findme")
	})

	t.RunWithGoLeak("error node write", func() {
		ctx, _ := t.Ctx()

		err := AsyncGenerateNodesHashByFollowingParent(
			ctx,
			t.nodesWithoutHash(9),
			1<<14,
			func(index uint64, _ Node) error {
				if index == 3 {
					return errors.Errorf("findme")
				}

				return nil
			},
		)
		t.Error(err)
		t.ErrorContains(err, "findme")
	})
}

func (t *testWrite) TestBenchmarkGenerateHash() {
	g := func(name string, n int, f func([]Node) error) {
		nodes := t.nodesWithoutHash(n)

		// t.NoError(traverseByHeight(uint64(n), func(height uint64, is []uint64) error {
		//	fmt.Printf("%02d: %v\n", height, is)

		//	return nil
		//}))

		started := time.Now()
		t.NoError(f(nodes))

		gelapsed := time.Since(started)

		tr := NewTree(treeHint, nodes)

		started = time.Now()
		t.NoError(tr.IsValid(nil))
		telapsed := time.Since(started)

		t.T().Logf("%s; nodes=%d hash elapsed=%v isvalid elapsed=%v",
			name, n, gelapsed, telapsed)
	}

	runTests := func(name string, f func([]Node) error) {
	end:
		for i := range 40000 {
			switch {
			case i > 0 && i < 11:
			case i > 0 && i < 99 && i%10 == 0:
			case i > 0 && i%5000 == 0:
			default:
				continue end
			}

			g(name, i, f)
		}
	}

	runTests("sync", func(nodes []Node) error {
		return SyncronousGenerateNodesHash(
			context.Background(),
			nodes,
			nil,
		)
	})

	runTests("async; following parent", func(nodes []Node) error {
		return AsyncGenerateNodesHashByFollowingParent(
			context.Background(),
			nodes,
			1<<14,
			nil,
		)
	})

	runTests("async; heights", func(nodes []Node) error {
		return AsyncGenerateNodesHashByHeight(
			context.Background(),
			nodes,
			nil,
		)
	})
}

func TestWrite(t *testing.T) {
	suite.Run(t, new(testWrite))
}
