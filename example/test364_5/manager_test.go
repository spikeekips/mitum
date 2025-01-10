package main

import (
	"context"
	"slices"
	"strings"
	"sync"
	"testing"

	"github.com/pkg/errors"
	"github.com/spikeekips/mitum/base"
	"github.com/spikeekips/mitum/example/test364_5/fixedtree"
	dutil "github.com/spikeekips/mitum/example/test364_5/util"
	"github.com/spikeekips/mitum/util"
	"github.com/stretchr/testify/suite"
	"go.uber.org/goleak"
)

type baseTestManager struct{}

func (t *baseTestManager) checkOpsTree(ophs [][2]util.Hash, expectedReasons map[uint64]string, tr *fixedtree.Tree) error {
	if err := tr.IsValid(nil); err != nil {
		return err
	}

	if len(ophs) != tr.Len() {
		return errors.Errorf("len ops, %d != tr.Len, %d", len(ophs), tr.Len())
	}

	return tr.Traverse(func(index uint64, n fixedtree.Node) (bool, error) {
		if err := n.IsValid(nil); err != nil {
			return false, err
		}

		op := ophs[index]
		b, ok := n.(*OperationTreeNode)
		if !ok {
			return false, errors.Errorf("expected *OperationTreeNode, but %T", n)
		}

		if b.Hash() == nil {
			return false, errors.Errorf("nil node hash")
		}

		if op[0].String() != b.Key() {
			return false, errors.Errorf("expected key, %q, but %q", op[0].String(), b.Key())
		}
		if !op[1].Equal(b.Fact()) {
			return false, errors.Errorf("expected fact, %q, but %q", op[1].String(), b.Fact())
		}

		if e, found := expectedReasons[index]; found {
			if e != b.Reason() {
				return false, errors.Errorf("expected reason, %q, but %q", e, b.Reason())
			}
		} else {
			if b.Reason() != "" {
				return false, errors.Errorf("expected empty reason, but %q", b.Reason())
			}
		}

		return true, nil
	})
}

func (t *baseTestManager) checkStatesTree(expectedStates [][2]interface{}, tr *fixedtree.Tree) error {
	if err := tr.IsValid(nil); err != nil {
		return err
	}

	if len(expectedStates) != tr.Len() {
		return errors.Errorf("expected, %d != tr.Len, %d", len(expectedStates), tr.Len())
	}

	slices.SortFunc(expectedStates, func(a, b [2]interface{}) int {
		return strings.Compare(a[0].(string), b[0].(string))
	})

	return tr.Traverse(func(index uint64, n fixedtree.Node) (bool, error) {
		a := expectedStates[index]
		b, ok := n.(*StateTreeNode)
		if !ok {
			return false, errors.Errorf("expected *StateTreeNode, but %T", n)
		}

		if e := a[0].(string); e != b.Key() {
			return false, errors.Errorf("expected key, %q, but %q", e, b.Key())
		}
		if e := a[1].(util.Hash); !e.Equal(b.StateHash()) {
			return false, errors.Errorf("expected state hash, %q, but %q", e, b.StateHash())
		}

		return true, nil
	})
}

func (t *baseTestManager) baseGenerateNodesHashFunc() fixedtree.GenerateNodesHashFunc {
	return fixedtree.NewFuncAsyncGenerateNodesHashByFollowingParent(1 << 9)
}

type testManager struct {
	dutil.BaseSuite
	baseTestWorkers
	baseTestManager
}

func (t *testManager) newOptions() *managerOptions {
	return newManagerOptions()
}

func (t *testManager) TestNew() {
	t.Run("empty work id", func() {
		_, _, _, _, err := newFuncManager(
			context.Background(),
			t.newOptions(),
			"",
			t.workers(3),
			1, 1,
			t.randomOphs(3),
		)
		t.ErrorContains(err, "empty work id")
	})

	t.Run("empty ophs", func() {
		_, _, _, _, err := newFuncManager(
			context.Background(),
			t.newOptions(),
			util.UUID().String(),
			t.workers(3),
			1, 1,
			nil,
		)
		t.ErrorContains(err, "empty ophs")
	})

	t.Run("empty workers", func() {
		_, _, _, _, err := newFuncManager(
			context.Background(),
			t.newOptions(),
			util.UUID().String(),
			nil,
			1, 1,
			t.randomOphs(33),
		)
		t.ErrorContains(err, "empty workers")
	})

	t.Run("zero ops share", func() {
		_, _, _, _, err := newFuncManager(
			context.Background(),
			t.newOptions(),
			util.UUID().String(),
			t.workers(3),
			0, 1,
			t.randomOphs(33),
		)
		t.ErrorContains(err, "zero ops share")
	})

	t.Run("zero states share", func() {
		_, _, _, _, err := newFuncManager(
			context.Background(),
			t.newOptions(),
			util.UUID().String(),
			t.workers(3),
			1, 0,
			t.randomOphs(33),
		)
		t.ErrorContains(err, "zero states share")
	})
}

func (t *testManager) TestBroadcastManagerDone() {
	t.Run("no workers", func() {
		ctx, cancel := t.Ctx()
		defer cancel(nil)

		workers := t.workers(9)

		sendOps, workerOwners, workerResult, wait, err := newFuncManager(
			ctx,
			t.newOptions(),
			util.UUID().String(),
			workers,
			1, 1,
			t.randomOphs(33),
		)
		t.NoError(err)
		t.NoError(sendOps(func(context.Context, string, string) (
			opindexOpHashesFunc, dutil.InErrorFunc[error], error,
		) {
			return func(uint64, util.Hash, util.Hash) error { return nil },
				func(err error) error { return err },
				nil
		}))

		owners := workers

		for worker := range slices.Values(workers) {
			t.T().Logf("worker owners: worker=%q owners=%v", worker, owners)

			t.NoError(workerOwners(worker, owners))
		}

		for worker := range slices.Values(owners) {
			t.T().Logf("worker result: worker=%q", worker)

			_, _, done, err := workerResult(worker)
			t.NoError(err)
			t.NoError(done(nil))
		}

		var nonOwners []string
		var l sync.Mutex

		t.NoError(wait(
			func(_ context.Context, worker string) error {
				t.T().Logf("non state owner: worker=%q", worker)

				l.Lock()
				nonOwners = append(nonOwners, worker)
				l.Unlock()

				return nil
			},
			func(opsTree, statesTree util.Hash) (base.Manifest, error) {
				t.T().Logf("manifest: ops tree=%v states tree=%v", opsTree, statesTree)

				return nil, nil
			},
		))

		t.Empty(nonOwners)
	})

	t.Run("some workers", func() {
		ctx, cancel := t.Ctx()
		defer cancel(nil)

		workers := t.workers(9)

		sendOps, workerOwners, workerResult, wait, err := newFuncManager(
			ctx,
			t.newOptions(),
			util.UUID().String(),
			workers,
			1, 1,
			t.randomOphs(33),
		)
		t.NoError(err)
		t.NoError(sendOps(func(context.Context, string, string) (
			opindexOpHashesFunc, dutil.InErrorFunc[error], error,
		) {
			return func(uint64, util.Hash, util.Hash) error { return nil },
				func(err error) error { return err },
				nil
		}))

		owners := workers[:2]

		for worker := range slices.Values(workers) {
			t.T().Logf("worker owners: worker=%q owners=%v", worker, owners)

			t.NoError(workerOwners(worker, owners))
		}

		for worker := range slices.Values(workers) {
			t.T().Logf("worker result: worker=%q", worker)

			_, _, done, err := workerResult(worker)
			t.NoError(err)
			t.NoError(done(nil))
		}

		var nonOwners []string
		var l sync.Mutex

		t.NoError(wait(
			func(_ context.Context, worker string) error {
				t.T().Logf("non state owner: worker=%q", worker)

				l.Lock()
				nonOwners = append(nonOwners, worker)
				l.Unlock()

				return nil
			},
			func(opsTree, statesTree util.Hash) (base.Manifest, error) {
				t.T().Logf("manifest: ops tree=%v states tree=%v", opsTree, statesTree)

				return nil, nil
			},
		))

		slices.Sort(nonOwners)
		t.Equal(workers[2:], nonOwners)
	})
}

func TestManager(t *testing.T) {
	defer goleak.VerifyNone(t)

	suite.Run(t, new(testManager))
}
