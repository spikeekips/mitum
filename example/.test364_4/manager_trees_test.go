package main

import (
	"context"
	"slices"
	"testing"

	"github.com/spikeekips/mitum/base"
	"github.com/spikeekips/mitum/util"
	"github.com/spikeekips/mitum/util/fixedtree"
	"github.com/spikeekips/mitum/util/logging"
	"github.com/stretchr/testify/suite"
	"go.uber.org/goleak"
)

type testManagerTrees struct {
	suite.Suite
	baseTestManager
	workID string
}

func (t *testManagerTrees) SetupTest() {
	t.workID = util.UUID().String()
}

func (t *testManagerTrees) newArgs() *managerWorkersTreesArgs {
	targs := newManagerWorkersTreesArgs()

	targs.NewWorkersOpsFunc = distributeNShareWorkerOps
	targs.NewOperationsTreeWriterFuncs = newFuncOperationsTreeWriter()
	targs.NewStatesTreeWriterFuncs = newFuncStatesTreeWriter(1<<9, 1<<9)

	_ = targs.SetLogging(logging.TestNilLogging)

	return targs
}

func (t *testManagerTrees) TestError() {
	targs := t.newArgs()

	f := newFuncManagerTrees(targs, t.newCheckWorkersFunc())

	t.Run("canceled before new", func() {
		ctx, cancel := context.WithCancel(context.Background())

		cancel()

		workers := t.workers(3)
		ophs := t.ophs(3)

		distribute, unmerge, states, done, err := f(ctx, workers, 1, 1, ophs)
		t.NoError(err)

		err = distribute(nil)
		t.ErrorIs(err, context.Canceled)

		util.TraverseSliceV1(workers, func(i int, worker string) (bool, error) {
			ok, err := unmerge(worker, []uint64{uint64(i)})
			t.False(ok)
			t.ErrorIs(err, context.Canceled)

			return true, nil
		})

		util.TraverseSliceV1(workers, func(i int, worker string) (bool, error) {
			err := states(
				context.Background(),
				worker,
				func(func(string) error) error {
					return nil
				},
			)
			t.ErrorIs(err, context.Canceled)

			return true, nil
		})

		var called bool

		err = done(func(_, _ fixedtree.Tree) error {
			called = true

			return nil
		})
		t.ErrorIs(err, context.Canceled)
		t.False(called)
	})

	t.Run("cancel before done", func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		workers := t.workers(3)
		ophs := t.ophs(3)

		distribute, unmerge, states, done, err := f(ctx, workers, 1, 1, ophs)
		t.NoError(err)

		t.NoError(distribute(nil))

		cancel()

		util.TraverseSliceV1(workers, func(i int, worker string) (bool, error) {
			ok, err := unmerge(worker, []uint64{uint64(i)})
			t.False(ok)
			t.ErrorIs(err, context.Canceled)

			return true, nil
		})

		util.TraverseSliceV1(workers, func(i int, worker string) (bool, error) {
			err := states(
				context.Background(),
				worker,
				func(func(string) error) error {
					return nil
				},
			)
			t.ErrorIs(err, context.Canceled)

			return true, nil
		})

		var called bool

		err = done(func(_, _ fixedtree.Tree) error {
			called = true

			return nil
		})
		t.ErrorIs(err, context.Canceled)
		t.False(called)
	})
}

func (t *testManagerTrees) TestOps() {
	targs := t.newArgs()

	f := newFuncManagerTrees(targs, t.newCheckWorkersFunc())

	t.Run("empty ops", func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		workers := t.workers(3)

		distribute, unmerge, states, done, err := f(ctx, workers, 1, 1, nil)
		t.NoError(err)

		t.NoError(distribute(nil))

		util.TraverseSliceV1(workers, func(i int, worker string) (bool, error) {
			ok, err := unmerge(worker, nil)
			t.NoError(err)
			t.True(ok)

			return true, nil
		})

		util.TraverseSliceV1(workers, func(i int, worker string) (bool, error) {
			t.NoError(states(
				context.Background(),
				worker,
				func(func(string) error) error {
					return nil
				},
			))

			return true, nil
		})

		var called bool

		t.NoError(done(func(opsTree fixedtree.Tree, statesTree fixedtree.Tree) error {
			t.Equal(0, opsTree.Len())
			t.Equal(0, statesTree.Len())
			t.Nil(opsTree.Root())
			t.Nil(statesTree.Root())

			called = true

			return nil
		}))
		t.True(called)
	})

	t.Run("none empty ops", func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		workers := t.workers(3)
		ophs := t.ophs(3)
		stateKeys := t.stateKeysUnordered(9)

		distribute, unmerge, states, done, err := f(ctx, workers, 1, 1, ophs)
		t.NoError(err)

		t.NoError(distribute(nil))

		util.TraverseSliceV1(workers, func(i int, worker string) (bool, error) {
			ok, err := unmerge(worker, nil)
			t.NoError(err)
			t.True(ok)

			return true, nil
		})

		util.TraverseSliceV1(workers, func(i int, worker string) (bool, error) {
			t.NoError(states(
				context.Background(),
				worker,
				func(f func(string) error) error {
					util.TraverseSliceV1(stateKeys[(i*3):(i+1)*3], func(_ int, stateKey string) (bool, error) {
						t.NoError(f(stateKey))

						return true, nil
					})

					return nil
				},
			))

			return true, nil
		})

		var called bool

		t.NoError(done(func(opsTree fixedtree.Tree, statesTree fixedtree.Tree) error {
			t.Equal(len(ophs), opsTree.Len())
			t.Equal(len(stateKeys), statesTree.Len())
			t.NotNil(opsTree.Root())
			t.NotNil(statesTree.Root())

			opsTree.Traverse(func(index uint64, n fixedtree.Node) (bool, error) {
				t.Equal(ophs[index][1].String(), n.Key())

				return true, nil
			})

			slices.Sort(stateKeys)

			statesTree.Traverse(func(index uint64, n fixedtree.Node) (bool, error) {
				t.Equal(stateKeys[index], n.Key())

				return true, nil
			})

			called = true

			return nil
		}))
		t.True(called)
	})

	t.Run("none empty ops; with unmerged", func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		workers := t.workers(3)
		ophs := t.ophs(9)
		stateKeys := t.stateKeysUnordered(9)

		workersUnmerged := make([][]uint64, len(workers))
		var unmerged []uint64
		util.TraverseSliceV1(workers, func(i int, worker string) (bool, error) {
			workersUnmerged[i] = []uint64{uint64(i)}
			unmerged = append(unmerged, uint64(i))

			return true, nil
		})

		distribute, unmerge, states, done, err := f(ctx, workers, 1, 1, ophs)
		t.NoError(err)

		t.NoError(distribute(nil))

		util.TraverseSliceV1(workers, func(i int, worker string) (bool, error) {
			ok, err := unmerge(worker, workersUnmerged[i])

			t.NoError(err)
			t.True(ok)

			return true, nil
		})

		util.TraverseSliceV1(workers, func(i int, worker string) (bool, error) {
			t.NoError(states(
				context.Background(),
				worker,
				func(f func(string) error) error {
					util.TraverseSliceV1(stateKeys[(i*3):(i+1)*3], func(_ int, stateKey string) (bool, error) {
						t.NoError(f(stateKey))

						return true, nil
					})

					return nil
				},
			))

			return true, nil
		})

		var called bool

		t.NoError(done(func(opsTree fixedtree.Tree, statesTree fixedtree.Tree) error {
			t.Equal(len(ophs), opsTree.Len())
			t.Equal(len(stateKeys), statesTree.Len())
			t.NotNil(opsTree.Root())
			t.NotNil(statesTree.Root())

			opsTree.Traverse(func(index uint64, n fixedtree.Node) (bool, error) {
				var expected string

				switch {
				case slices.Index(unmerged, index) < 0:
					expected = base.NewInStateOperationFixedtreeNode(ophs[index][1], "").Key()
				default:
					expected = base.NewNotInStateOperationFixedtreeNode(ophs[index][1], "").Key()
				}

				t.Equal(expected, n.Key())

				return true, nil
			})

			slices.Sort(stateKeys)

			statesTree.Traverse(func(index uint64, n fixedtree.Node) (bool, error) {
				t.Equal(stateKeys[index], n.Key())

				return true, nil
			})

			called = true

			return nil
		}))
		t.True(called)
	})

	t.Run("empty states", func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		workers := t.workers(3)
		ophs := t.ophs(3)

		distribute, unmerge, states, done, err := f(ctx, workers, 1, 1, ophs)
		t.NoError(err)

		t.NoError(distribute(nil))

		util.TraverseSliceV1(workers, func(i int, worker string) (bool, error) {
			ok, err := unmerge(worker, nil)
			t.NoError(err)
			t.True(ok)

			return true, nil
		})

		util.TraverseSliceV1(workers, func(i int, worker string) (bool, error) {
			t.NoError(states(
				context.Background(),
				worker,
				func(f func(string) error) error {
					return nil
				},
			))

			return true, nil
		})

		var called bool

		t.NoError(done(func(opsTree fixedtree.Tree, statesTree fixedtree.Tree) error {
			t.Equal(len(ophs), opsTree.Len())
			t.Equal(0, statesTree.Len())
			t.NotNil(opsTree.Root())
			t.Nil(statesTree.Root())

			opsTree.Traverse(func(index uint64, n fixedtree.Node) (bool, error) {
				t.Equal(ophs[index][1].String(), n.Key())

				return true, nil
			})

			called = true

			return nil
		}))
		t.True(called)
	})
}

func TestManagerTrees(t *testing.T) {
	defer goleak.VerifyNone(t)

	suite.Run(t, new(testManagerTrees))
}
