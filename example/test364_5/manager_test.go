package dop5

import (
	"context"
	"iter"
	"maps"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
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

	for index, n := range tr.Traverse() {
		if err := n.IsValid(nil); err != nil {
			return err
		}

		op := ophs[index]
		b, ok := n.(*operationTreeNode)
		if !ok {
			return errors.Errorf("expected *OperationTreeNode, but %T", n)
		}

		if b.Hash() == "" {
			return errors.Errorf("nil node hash")
		}

		if op[0].String() != b.Key() {
			return errors.Errorf("expected key, %q, but %q", op[0].String(), b.Key())
		}
		if !op[1].Equal(b.Fact()) {
			return errors.Errorf("expected fact, %q, but %q", op[1].String(), b.Fact())
		}

		if e, found := expectedReasons[index]; found {
			if e != b.Reason() {
				return errors.Errorf("expected reason, %q, but %q", e, b.Reason())
			}
		} else {
			if b.Reason() != "" {
				return errors.Errorf("expected empty reason, but %q", b.Reason())
			}
		}
	}

	return nil
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

	for index, n := range tr.Traverse() {
		a := expectedStates[index]
		b, ok := n.(*stateTreeNode)
		if !ok {
			return errors.Errorf("expected *StateTreeNode, but %T", n)
		}

		if e := a[0].(string); e != b.Key() {
			return errors.Errorf("expected key, %q, but %q", e, b.Key())
		}
		if e := a[1].(util.Hash); !e.Equal(b.StateHash()) {
			return errors.Errorf("expected state hash, %q, but %q", e, b.StateHash())
		}
	}

	return nil
}

func (t *baseTestManager) baseGenerateNodesHashFunc() fixedtree.GenerateNodesHashFunc {
	return fixedtree.NewFuncAsyncGenerateNodesHashByFollowingParent(1 << 9)
}

func (t *baseTestManager) processOps(
	ctx context.Context,
	facts []OperationFact,
	getState getStateFunc,
) (opsTree, statesTree *fixedtree.Tree, _ []base.State, _ map[string]int64, _ error) {
	local := "local"
	height := base.Height(33)
	workers := []string{local}

	ophs := make([][2]util.Hash, len(facts))
	for i, fact := range slices.All(facts) {
		ophs[i] = [2]util.Hash{fact.Hash(), fact.Hash()}
	}

	mophs := func(yield func(uint64, [2]util.Hash) bool) {
		for i, v := range slices.All(ophs) {
			if !yield(uint64(i), v) {
				return
			}
		}
	}

	createState := func(stateKey string, value base.StateValue, prev util.Hash, ops []util.Hash) (base.State, error) {
		return base.NewBaseState(height, stateKey, value, prev, ops), nil
	}

	startOpsFixedNodes, unmergedOp, doneOpsTree, err := newFuncGenerateOperationFixedtree(ctx, ophs, func(f dutil.InErrorFunc[context.Context]) error {
		return f(ctx)
	})
	if err != nil {
		return nil, nil, nil, nil, err
	}

	startOpsFixedNodes()

	initialInsert, _, _, wait, closeWork, err := NewFuncWorker(
		ctx,
		NewWorkerOptions(local),
		getState,
		createState,
		nil,
		workers, workers,
		1, 1,
		uint64(len(facts)),
	)
	if err != nil {
		return nil, nil, nil, nil, err
	}
	defer closeWork(nil)

	err = initialInsert(
		func(string) []string { return workers },
		func(context.Context, string) (
			WorkerProcessInsertSMVsFunc,
			dutil.In2ErrorFunc[[]string, []string],
			error,
		) {
			return nil, nil, nil
		},
		func(_ context.Context, opindex uint64, _, _ util.Hash) (OperationFact, bool, string, error) {
			if opindex >= uint64(len(facts)) {
				return nil, false, "", nil
			}

			return facts[opindex], true, "", nil
		},
		mophs,
		nil,
	)
	if err != nil {
		return nil, nil, nil, nil, err
	}

	g := fixedtree.NewFuncAsyncGenerateNodesHashByFollowingParent(1 << 9)

	var states []base.State

	err = wait(
		func(context.Context, string, string, []uint64) error { return nil },
		func(
			ctx context.Context,
			_ string,
			reasons map[uint64]string,
			statesSize uint64,
			f dutil.InErrorFunc[dutil.InErrorFunc[base.State]],
		) error {
			defer closeWork(nil)

			for opindex, reason := range maps.All(reasons) {
				if err := unmergedOp(opindex, reason); err != nil {
					return err
				}
			}

			switch tr, err := doneOpsTree(g); {
			case err != nil:
				return err
			default:
				opsTree = tr
			}

			return f(func(st base.State) error { return nil })
		},
		func(sts []base.State) error {
			states = sts

			return nil
		},
	)

	slices.SortFunc(states, func(a, b base.State) int {
		return strings.Compare(a.Key(), b.Key())
	})

	statesNodes := make([]fixedtree.Node, len(states))

	for i, st := range slices.All(states) {
		n, err := newStateTreeNode(st.Key(), st.Hash())
		if err != nil {
			return nil, nil, nil, nil, err
		}

		statesNodes[i] = n
	}

	if err := g(ctx, statesNodes, nil); err != nil {
		return nil, nil, nil, nil, err
	}

	statesTree = newStatesTree(statesNodes)

	values := map[string]int64{}

	for st := range slices.Values(states) {
		values[st.Key()] = st.Value().(*AccountSV).Balance
	}

	return opsTree, statesTree, states, values, nil
}

type testManager struct {
	dutil.BaseSuite
	baseTestWorkers
	baseTestManager
}

func (t *testManager) newOptions() *ManagerOptions {
	return NewManagerOptions()
}

func (t *testManager) TestNew() {
	t.Run("empty ophs", func() {
		_, _, _, _, _, err := NewFuncManager(
			context.Background(),
			t.newOptions(),
			nil,
			t.workers(3),
			1, 1,
			nil,
		)
		t.ErrorContains(err, "empty ophs")
	})

	t.Run("empty workers", func() {
		_, _, _, _, _, err := NewFuncManager(
			context.Background(),
			t.newOptions(),
			nil,
			nil,
			1, 1,
			t.randomOphs(33),
		)
		t.ErrorContains(err, "empty workers")
	})

	t.Run("zero ops share", func() {
		_, _, _, _, _, err := NewFuncManager(
			context.Background(),
			t.newOptions(),
			nil,
			t.workers(3),
			0, 1,
			t.randomOphs(33),
		)
		t.ErrorContains(err, "zero ops share")
	})

	t.Run("zero states share", func() {
		_, _, _, _, _, err := NewFuncManager(
			context.Background(),
			t.newOptions(),
			nil,
			t.workers(3),
			1, 0,
			t.randomOphs(33),
		)
		t.ErrorContains(err, "zero states share")
	})
}

func (t *testManager) TestStartWorkers() {
	getOpsWorkers := func(workers []string, opsShare uint64, ophs [][2]util.Hash) []string {
		w, _, err := opsNShareWorkers(workers, opsShare, ophs)
		t.IfErrPanic(err)

		return opsWorkersFromSeq(w)
	}

	run := func(
		workers []string,
		opsShare, statesShare uint64,
		ophs [][2]util.Hash,
		failedWorkers []string,
		cancelWorkf ManagerCancelWorkFunc,
		closeWorkf managerCloseWorkFunc,
	) error {
		ctx, cancel := t.Ctx()
		defer cancel(nil)

		opsWorkers := getOpsWorkers(workers, opsShare, ophs)
		t.T().Logf("ops share=%d ops workers=%v", opsShare, opsWorkers)

		sendOps, workerOwners, workerResult, _, wait, err := NewFuncManager(
			ctx,
			t.newOptions(),
			func(ctx context.Context, worker string, err error) error {
				t.Print("worker canceled: worker=%q err=%+v", worker, err)

				return cancelWorkf(ctx, worker, err)
			},
			workers,
			opsShare, statesShare,
			ophs,
		)
		t.IfErrPanic(err)
		err = sendOps(
			func(_ context.Context,
				worker string,
				_, _ []string,
				_, _, _ uint64,
			) error {
				if slices.Index(failedWorkers, worker) < 0 {
					return nil
				}

				return errors.Errorf("failed, worker=%q", worker)
			},
			func(context.Context, string, uint64) (
				OpindexOpHashesFunc, dutil.InErrorFunc[error], error,
			) {
				return func(uint64, util.Hash, util.Hash) error { return nil },
					func(err error) error { return err },
					nil
			},
		)
		if err != nil {
			return err
		}

		for worker := range slices.Values(workers) {
			err := workerOwners(worker, workers)
			t.T().Logf("worker owners: worker=%q owners=%v err=%v", worker, workers, err)
		}

		for worker := range slices.Values(workers) {
			_, _, done, err := workerResult(worker)
			if err == nil {
				err = done(nil)
			}

			t.T().Logf("worker result: worker=%q err=%v", worker, err)
		}

		return wait(
			func(_ context.Context, worker string) error {
				t.T().Logf("non state owner: worker=%q", worker)

				return nil
			},
			func(opsTree, statesTree *fixedtree.Tree) (base.Manifest, error) {
				t.T().Logf("manifest: ops tree=%v states tree=%v", opsTree.Root(), statesTree.Root())

				return nil, nil
			},
			func(ctx context.Context, worker string) error {
				t.T().Logf("worker done: worker=%q", worker)

				return closeWorkf(ctx, worker)
			},
		)
	}

	t.Run("no failed", func() {
		workers := t.workers(9)

		var addDone dutil.AppendSliceAddFunc[string]
		var doneDone dutil.OutFunc[[]string]

		addDone, doneDone, err := dutil.NewLockComparableUniqueAppendSlice[string](uint64(len(workers)), 0)
		t.NoError(err)

		t.NoError(run(
			workers,
			1, 1,
			t.randomOphs(3),
			nil,
			func(_ context.Context, worker string, err error) error {
				t.IfErrPanicf(err, "worker canceled by manager")

				return nil
			},
			func(_ context.Context, worker string) error {
				addDone(worker)

				return nil
			},
		))

		collected := doneDone()
		slices.Sort(collected)

		t.Equal(workers, collected)
	})

	t.Run("failed ops worker; under max failed", func() {
		var called int64

		t.NoError(run(
			t.workers(9),
			2, 1,
			t.randomOphs(3),
			[]string{"w0"},
			func(_ context.Context, worker string, err error) error {
				t.IfErrPanicf(err, "worker canceled by manager")

				return nil
			},
			func(_ context.Context, worker string) error {
				atomic.AddInt64(&called, 1)

				return nil
			},
		))

		t.True(called > 0)
	})

	t.Run("failed ops worker; over max failed", func() {
		var cancelCalled, doneCalled int64

		err := run(
			t.workers(9),
			1, 1,
			t.randomOphs(3),
			[]string{"w0", "w1"},
			func(_ context.Context, worker string, err error) error {
				atomic.AddInt64(&cancelCalled, 1)

				return nil
			},
			func(_ context.Context, worker string) error {
				atomic.AddInt64(&doneCalled, 1)

				return nil
			},
		)
		t.ErrorIs(err, errWorkersFailed)
		t.True(cancelCalled > 0)
		t.True(doneCalled < 1)
	})

	t.Run("failed non-ops worker; over max failed", func() {
		var doneCalled int64

		t.NoError(run(
			t.workers(9),
			1, 1,
			t.randomOphs(3),
			[]string{"w6", "w7", "w8"},
			func(_ context.Context, worker string, err error) error {
				t.IfErrPanicf(err, "worker canceled by manager")

				return nil
			},
			func(_ context.Context, worker string) error {
				atomic.AddInt64(&doneCalled, 1)

				return nil
			},
		))

		t.True(doneCalled > 0)
	})
}

func (t *testManager) TestBroadcastManagerDone() {
	t.Run("no workers", func() {
		ctx, cancel := t.Ctx()
		defer cancel(nil)

		workers := t.workers(9)

		sendOps, workerOwners, workerResult, _, wait, err := NewFuncManager(
			ctx,
			t.newOptions(),
			nil,
			workers,
			1, 1,
			t.randomOphs(33),
		)
		t.NoError(err)
		t.NoError(sendOps(
			func(context.Context,
				string,
				[]string, []string,
				uint64, uint64, uint64,
			) error {
				return nil
			},
			func(context.Context, string, uint64) (
				OpindexOpHashesFunc, dutil.InErrorFunc[error], error,
			) {
				return func(uint64, util.Hash, util.Hash) error { return nil },
					func(err error) error { return err },
					nil
			},
		))

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
			func(opsTree, statesTree *fixedtree.Tree) (base.Manifest, error) {
				t.T().Logf("manifest: ops tree=%v states tree=%v", opsTree.Root(), statesTree.Root())

				return nil, nil
			},
			func(_ context.Context, worker string) error {
				t.T().Logf("done worker: worker=%q", worker)

				return nil
			},
		))

		t.Empty(nonOwners)
	})

	t.Run("some workers", func() {
		ctx, cancel := t.Ctx()
		defer cancel(nil)

		workers := t.workers(9)

		sendOps, workerOwners, workerResult, _, wait, err := NewFuncManager(
			ctx,
			t.newOptions(),
			nil,
			workers,
			1, 1,
			t.randomOphs(33),
		)
		t.NoError(err)
		t.NoError(sendOps(
			func(context.Context,
				string,
				[]string, []string,
				uint64, uint64, uint64,
			) error {
				return nil
			},
			func(context.Context, string, uint64) (
				OpindexOpHashesFunc, dutil.InErrorFunc[error], error,
			) {
				return func(uint64, util.Hash, util.Hash) error { return nil },
					func(err error) error { return err },
					nil
			},
		))

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
			func(opsTree, statesTree *fixedtree.Tree) (base.Manifest, error) {
				t.T().Logf("manifest: ops tree=%v states tree=%v", opsTree.Root(), statesTree.Root())

				return nil, nil
			},
			func(_ context.Context, worker string) error {
				t.T().Logf("done worker: worker=%q", worker)

				return nil
			},
		))

		slices.Sort(nonOwners)
		t.Equal(workers[2:], nonOwners)
	})
}

func (t *testManager) TestCloseWork() {
	run := func(
		workers, statesOwners []string,
		closeWorkf managerCloseWorkFunc,
	) error {
		ctx, cancel := t.Ctx()
		defer cancel(nil)

		sendOps, workerOwners, workerResult, _, wait, err := NewFuncManager(
			ctx,
			t.newOptions(),
			func(_ context.Context, worker string, err error) error {
				t.IfErrPanicf(err, "worker canceled: worker=%q", worker)

				return err
			},
			workers,
			2, 2,
			t.randomOphs(3),
		)

		t.NoError(err)
		t.NoError(sendOps(
			func(context.Context, string, []string, []string, uint64, uint64, uint64) error { return nil },
			func(context.Context, string, uint64) (OpindexOpHashesFunc, dutil.InErrorFunc[error], error) {
				return func(uint64, util.Hash, util.Hash) error { return nil },
					func(err error) error { return err },
					nil
			},
		))

		for worker := range slices.Values(workers) {
			t.T().Logf("worker owners: worker=%q owners=%v", worker, statesOwners)

			t.NoError(workerOwners(worker, statesOwners))
		}

		for worker := range slices.Values(workers) {
			t.T().Logf("worker result: worker=%q", worker)

			_, _, done, err := workerResult(worker)
			t.NoError(err)
			t.NoError(done(nil))
		}

		return wait(
			func(_ context.Context, worker string) error {
				t.Print("non state owner: worker=%q", worker)

				return nil
			},
			func(opsTree, statesTree *fixedtree.Tree) (base.Manifest, error) {
				t.Print("manifest: ops tree=%v states tree=%v", opsTree.Root(), statesTree.Root())

				return nil, nil
			},
			func(ctx context.Context, worker string) error {
				t.Print("done worker: worker=%q", worker)

				return closeWorkf(ctx, worker)
			},
		)
	}

	t.Run("ok", func() {
		workers := t.workers(3)
		t.NoError(run(
			workers, workers,
			func(context.Context, string) error { return nil },
		))
	})

	t.Run("ops worker, not in states owners failed; under max failed", func() {
		workers := t.workers(6)

		t.NoError(run(
			workers,
			workers[3:],
			func(_ context.Context, worker string) error {
				if worker == workers[0] {
					return errors.Errorf("showme")
				}

				return nil
			},
		))
	})

	t.Run("ops worker, not in states owners failed; over max failed", func() {
		workers := t.workers(6)

		err := run(
			workers,
			workers[3:],
			func(_ context.Context, worker string) error {
				switch {
				case worker == workers[0],
					worker == workers[1]:
					return errors.Errorf("showme")
				}

				return nil
			},
		)
		t.ErrorIs(err, errWorkersFailed)
	})

	t.Run("states owner, not in ops owners failed; under max failed", func() {
		workers := t.workers(6)

		t.NoError(run(
			workers,
			workers[3:],
			func(_ context.Context, worker string) error {
				switch {
				case worker == workers[3]:
					return errors.Errorf("showme")
				}

				return nil
			},
		))
	})
}

func TestManager(t *testing.T) {
	defer goleak.VerifyNone(t)

	suite.Run(t, new(testManager))
}

type testFailedWorkerPool struct {
	dutil.BaseSuite
	baseTestWorkers
}

func (t *testFailedWorkerPool) TestWorkerError() {
	t.Run("unknown worker", func() {
		workers := t.workers(3)

		pool := newFailedWorkerPool(
			workers,
			workers[:2],
			1, 1,
		)

		added, failed, err := pool.workerError(util.UUID().String(), t.RandomError())
		t.ErrorContains(err, "unknown worker")
		t.False(added)
		t.False(failed)
	})

	t.Run("nil error", func() {
		workers := t.workers(3)

		pool := newFailedWorkerPool(
			workers,
			workers[:2],
			1, 1,
		)

		added, failed, err := pool.workerError(workers[0], nil)
		t.ErrorContains(err, "nil error")
		t.False(added)
		t.False(failed)
	})

	t.Run("failed; ops workers", func() {
		workers := t.workers(3)
		opsWorkers := workers[:2]

		pool := newFailedWorkerPool(
			workers,
			opsWorkers,
			uint64(len(opsWorkers)),
			1,
		)

		added, failed, err := pool.workerError(opsWorkers[0], t.RandomError())
		t.NoError(err)
		t.True(added)
		t.False(failed)
		added, failed, err = pool.workerError(opsWorkers[1], t.RandomError())
		t.NoError(err)
		t.True(added)
		t.True(failed)
	})
}

func (t *testFailedWorkerPool) TestSubosReady() {
	t.Run("not failed; not yet ready", func() {
		workers := t.workers(6)
		opsWorkers := workers[:3]
		subOwners := workers[3:]

		pool := newFailedWorkerPool(
			workers,
			opsWorkers,
			1,
			uint64(len(subOwners)),
		)

		t.False(pool.statesOwnersReady(subOwners))
	})

	t.Run("failed; not yet ready", func() {
		workers := t.workers(6)
		opsWorkers := workers[:3]
		subOwners := workers[3:]

		pool := newFailedWorkerPool(
			workers,
			opsWorkers,
			1,
			uint64(len(subOwners)),
		)

		added, failed, err := pool.workerError(opsWorkers[0], t.RandomError())
		t.NoError(err)
		t.True(added)
		t.True(failed)

		t.True(pool.statesOwnersReady(subOwners))
	})

	t.Run("sub owners worker error; not yet ready", func() {
		workers := t.workers(6)
		opsWorkers := workers[:3]
		subOwners := workers[3:]

		pool := newFailedWorkerPool(
			workers,
			opsWorkers,
			1,
			uint64(len(subOwners)),
		)

		for worker := range slices.Values(subOwners) {
			added, failed, err := pool.workerError(worker, t.RandomError())
			t.NoError(err)
			t.True(added)
			t.False(failed)
		}

		t.True(pool.statesOwnersReady(subOwners))
	})
}

func (t *testFailedWorkerPool) TestActiveWorkers() {
	t.Run("before failed", func() {
		workers := t.workers(3)

		pool := newFailedWorkerPool(
			workers,
			workers,
			1, 1,
		)

		var actives []string
		failed, seq := pool.activeWorkers()
		t.False(failed)

		for worker := range seq {
			actives = append(actives, worker)
		}
		t.Equal(workers, actives)
	})

	t.Run("worker error", func() {
		workers := t.workers(3)

		pool := newFailedWorkerPool(
			workers,
			workers,
			2, 1,
		)

		added, failed, err := pool.workerError(workers[0], t.RandomError())
		t.NoError(err)
		t.True(added)
		t.False(failed)

		failed, seq := pool.activeWorkers()
		t.False(failed)

		var actives []string
		for worker := range seq {
			actives = append(actives, worker)
		}
		t.Equal(workers[1:], actives)
	})
}

func (t *testFailedWorkerPool) TestActiveOpsWorkers() {
	t.Run("before failed", func() {
		workers := t.workers(3)
		opsWorkers := workers[1:]

		pool := newFailedWorkerPool(
			workers,
			opsWorkers,
			uint64(len(opsWorkers)),
			1,
		)

		var actives []string
		t.False(pool.activeOpsWorkers(func(seq iter.Seq[string]) {
			for worker := range seq {
				actives = append(actives, worker)
			}
		}))
		t.Equal(opsWorkers, actives)

		added, failed, err := pool.workerError(opsWorkers[0], t.RandomError())
		t.NoError(err)
		t.True(added)
		t.False(failed)

		actives = actives[:0]
		t.False(pool.activeOpsWorkers(func(seq iter.Seq[string]) {
			for worker := range seq {
				actives = append(actives, worker)
			}
		}))
		t.Equal(opsWorkers[1:], actives)
	})

	t.Run("after failed", func() {
		workers := t.workers(3)
		opsWorkers := workers[:2]

		pool := newFailedWorkerPool(
			workers,
			opsWorkers,
			uint64(len(opsWorkers)),
			1,
		)

		for worker := range slices.Values(pool.opsWorkers()) {
			pool.workerError(worker, t.RandomError())
		}

		var actives []string
		t.True(pool.activeOpsWorkers(func(seq iter.Seq[string]) {
			for worker := range seq {
				actives = append(actives, worker)
			}
		}))

		t.Empty(actives)
	})
}

func (t *testFailedWorkerPool) TestActiveSubOwners() {
	t.Run("before ready", func() {
		workers := t.workers(6)
		opsWorkers := workers[:3]
		subOwners := workers[3:]

		pool := newFailedWorkerPool(
			workers,
			opsWorkers,
			uint64(len(opsWorkers)),
			uint64(len(subOwners)),
		)

		var actives []string
		failed, err := pool.activeStatesOwners(func(seq iter.Seq[string]) {
			for worker := range seq {
				actives = append(actives, worker)
			}
		})
		t.ErrorContains(err, "not ready")
		t.False(failed)
		t.Empty(actives)
	})

	t.Run("after ready", func() {
		workers := t.workers(6)
		opsWorkers := workers[:3]
		subOwners := workers[3:]

		pool := newFailedWorkerPool(
			workers,
			opsWorkers,
			uint64(len(opsWorkers)),
			uint64(len(subOwners)),
		)

		t.False(pool.statesOwnersReady(subOwners))

		var actives []string
		failed, err := pool.activeStatesOwners(func(seq iter.Seq[string]) {
			for worker := range seq {
				actives = append(actives, worker)
			}
		})
		t.NoError(err)
		t.False(failed)

		t.Equal(subOwners, actives)

		added, failed, err := pool.workerError(subOwners[0], t.RandomError())
		t.NoError(err)
		t.True(added)
		t.False(failed)

		actives = actives[:0]

		failed, err = pool.activeStatesOwners(func(seq iter.Seq[string]) {
			for worker := range seq {
				actives = append(actives, worker)
			}
		})
		t.NoError(err)
		t.False(failed)

		t.Equal(subOwners[1:], actives)
	})

	t.Run("after ready, but failed", func() {
		workers := t.workers(6)
		opsWorkers := workers[:3]
		subOwners := workers[3:]

		pool := newFailedWorkerPool(
			workers,
			opsWorkers,
			1,
			uint64(len(subOwners)),
		)

		t.False(pool.statesOwnersReady(subOwners))

		added, failed, err := pool.workerError(opsWorkers[0], t.RandomError())
		t.NoError(err)
		t.True(added)
		t.True(failed)

		var actives []string
		failed, err = pool.activeStatesOwners(func(seq iter.Seq[string]) {
			for worker := range seq {
				actives = append(actives, worker)
			}
		})
		t.NoError(err)
		t.True(failed)

		t.Empty(actives)
	})
}

func TestFailedWorkerPool(t *testing.T) {
	defer goleak.VerifyNone(t)

	suite.Run(t, new(testFailedWorkerPool))
}
