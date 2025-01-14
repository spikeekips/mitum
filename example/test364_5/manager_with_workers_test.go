package main

import (
	"context"
	"fmt"
	"maps"
	"slices"
	"strings"
	"sync"
	"testing"

	"github.com/spikeekips/mitum/base"
	"github.com/spikeekips/mitum/example/test364_5/fixedtree"
	dutil "github.com/spikeekips/mitum/example/test364_5/util"
	"github.com/spikeekips/mitum/util"
	"github.com/stretchr/testify/suite"
	"go.uber.org/goleak"
)

type testManagerWithWorkers struct {
	dutil.BaseSuite
	baseTestAccountStateValueMerger
	baseTestWorkers
	baseTestManager
}

func (t *testManagerWithWorkers) newManagerOptions() *managerOptions {
	return newManagerOptions()
}

func (t *testManagerWithWorkers) newWorkerOptions(local string) *workerOptions {
	return newWorkerOptions(local)
}

func (t *testManagerWithWorkers) randomOps(n int) ([]Operation, [][2]util.Hash) {
	ops := t.baseTestAccountStateValueMerger.randomAddOps(n, 1)

	ophs := make([][2]util.Hash, len(ops))
	for i, op := range slices.All(ops) {
		ophs[i] = [2]util.Hash{op.Hash(), op.Fact().Hash()}
	}

	return ops, ophs
}

func (t *testManagerWithWorkers) workerIfErrCancel(worker string, cancel dutil.InErrorFunc[error]) dutil.InErrorFunc[error] {
	return func(err error) error {
		t.Println("error: worker=%q err=%w", worker, err)
		if err != nil {
			return cancel(err)
		}

		return nil
	}
}

func (t *testManagerWithWorkers) workerIfErrPanic(worker string, err error) error {
	if err != nil {
		err = fmt.Errorf("panic: worker=%q err=%w", worker, err)
	}

	t.IfErrPanic(err)

	return nil
}

func (t *testManagerWithWorkers) newWorkers(
	ctx context.Context,
	workID string,
	workers []string,
	opsShare, statesShare uint64,
	ophs [][2]util.Hash,
	getState getStateFunc,
) (
	workersInitialInserts map[string]workerInitialInsertFunc,
	workersInserts map[string]workerProcessWorkerInsertStateMergeValuesFunc,
	workersUnmerge map[string]workerUnmergedFunc,
	workersWaits map[string]workerWaitFunc,
) {
	workersInitialInserts = map[string]workerInitialInsertFunc{}
	workersInserts = map[string]workerProcessWorkerInsertStateMergeValuesFunc{}
	workersUnmerge = map[string]workerUnmergedFunc{}
	workersWaits = map[string]workerWaitFunc{}

	var opsWorkers []string

	{
		w, _, err := opsNShareWorkers(workers, opsShare, ophs)
		t.NoError(err)
		opsWorkers = w()
	}

	for local := range slices.Values(workers) {
		t, i, u, w := t.newWorker(
			ctx,
			workID,
			local,
			workers, opsWorkers,
			opsShare, statesShare,
			uint64(len(ophs)),
			getState,
		)

		workersInitialInserts[local] = t
		workersInserts[local] = i
		workersUnmerge[local] = u
		workersWaits[local] = w
	}

	return workersInitialInserts,
		workersInserts,
		workersUnmerge,
		workersWaits
}

func (t *testManagerWithWorkers) newWorker(
	ctx context.Context,
	workID, local string,
	workers, opsWorkers []string,
	opsShare, statesShare uint64,
	maxOpsSize uint64,
	getState getStateFunc,
) (
	workerInitialInsertFunc,
	workerProcessWorkerInsertStateMergeValuesFunc,
	workerUnmergedFunc,
	workerWaitFunc,
) {
	newState := func(stateKey string, value base.StateValue, ops []util.Hash) (base.State, error) {
		st := base.NewBaseState(
			33,
			stateKey,
			value,
			nil,
			ops,
		)
		// t.Println("new state: key=%q hash=%q ops=%v value=%s", st.Key(), st.Hash(), ops, util.MustMarshalJSONString(value))

		return st, nil
	}

	initialInsert, workerInsert, workerUnmerge, workerWait, err := newFuncWorker(
		ctx,
		t.newWorkerOptions(local),
		getState,
		newState,
		workID,
		workers, opsWorkers,
		opsShare, statesShare,
		maxOpsSize,
	)
	t.workerIfErrPanic(local, err)

	return initialInsert, workerInsert, workerUnmerge, workerWait
}

func (t *testManagerWithWorkers) newManager(
	ctx context.Context,
	workers []string,
	opsShare, statesShare uint64,
	ops []Operation,
	getState getStateFunc,
	getStateOwners getStateOwnersFunc,
) (
	sendOps dutil.ErrorFunc,
	workersWait dutil.ErrorFunc,
	wait func() (_, _ *fixedtree.Tree, _ []base.State, _ error),
) {
	switch nworkers := uint64(len(workers)); {
	case opsShare > nworkers:
		panic(fmt.Sprintf("opsShare=%d > workers=%d", opsShare, nworkers))
	case statesShare > nworkers:
		panic(fmt.Sprintf("statesShare=%d > workers=%d", statesShare, nworkers))
	}

	workID := util.UUID().String()

	ophs := make([][2]util.Hash, len(ops))
	for i, op := range slices.All(ops) {
		ophs[i] = [2]util.Hash{op.Hash(), op.Fact().Hash()}
	}

	{
		w, d, err := opsNShareWorkers(workers, opsShare, ophs)
		t.NoError(err)
		opsWorkers = w()
	}

	managerSendOps, managerWorkerOwners, managerWorkerResult, managerWait, err := newFuncManager(
		ctx,
		t.newManagerOptions(),
		workID,
		workers,
		opsShare, statesShare,
		ophs,
	)
	t.NoError(err)

	workersInitialInserts, workersInserts, workersUnmerge, workersWaits := t.newWorkers(
		ctx,
		workID,
		workers,
		opsShare, statesShare,
		ophs,
		getState,
	)

	getOperation := t.getOperationFunc(ops)
	distributeValues := func(local string) newWorkerDistributeStateMergeValuesFunc {
		return func(ctx context.Context, workID, worker string) (
			workerProcessInsertStateMergeValuesFunc,
			dutil.In2ErrorFunc[[]string, []string],
			error,
		) {
			i, o, err := workersInserts[worker](local)

			switch {
			case err != nil:
				t.Println("distribute worker: local=%q worker=%q err=%+v", local, worker, err)

				return nil, nil, err
			case i == nil:
				return nil, nil, nil
			}

			return func(opindex uint64, fact util.Hash, values []StateMergeValue) error {
					stateKeys := extractStateMergeValueKeysFromValues(values)

					err := i(opindex, fact, values)
					t.Println("distribute worker: insert: local=%q worker=%q opindex=%d fact=%q state keys=%v err=%+v", local, worker, opindex, fact, stateKeys, err)

					return err
				},
				func(subs, affected []string) error {
					t.Println("> distribute worker: owners: local=%q worker=%q sub=%v affected=%v", local, worker, subs, affected)
					err := o(subs, affected)

					t.Println("< distribute worker: owners: local=%q worker=%q sub=%v affected=%v err=%+v", local, worker, subs, affected, err)

					return err
				},
				nil
		}
	}

	sendOps = func() error {
		return managerSendOps(
			func(
				_ context.Context,
				workID, worker string,
				workers, opsWorkers []string,
				opsShare, statesShare uint64,
				maxOpsSize uint64,
			) error {
				//t.Println("start worker: work id=%q worker=%q workers=%v opsWorkers=%v ops share=%d states share=%d max ops size=%d",
				//	workID, worker,
				//	workers, opsWorkers,
				//	opsShare, statesShare,
				//	maxOpsSize,
				//)

				return nil
			},
			func(_ context.Context, _, worker string) (opindexOpHashesFunc, dutil.InErrorFunc[error], error) {
				wops := map[uint64][2]util.Hash{}

				return func(opindex uint64, oph, fact util.Hash) error {
						t.Println("manager initial insert: worker=%q opindex=%d fact=%q", worker, opindex, fact)

						wops[opindex] = [2]util.Hash{oph, fact}

						return nil
					},
					func(err error) error {
						t.Println("manager initial insert done: worker=%q err=%+v", worker, err)

						if err == nil {
							err := workersInitialInserts[worker](
								getStateOwners,
								distributeValues(worker),
								getOperation,
								maps.All(wops),
								func(
									_ context.Context,
									local,
									_ string,
									owners []string,
								) error {
									t.Println("worker report manager: local=%q owners=%v", worker, owners)
									return managerWorkerOwners(worker, owners)
								},
							)

							t.Println("manager workers initial insert: worker=%q err=%+v", worker, err)
						}

						return nil // FIXME
					},
					nil
			},
		)
	}

	var addState dutil.AppendSliceAddFunc[base.State]
	var doneState dutil.OutFunc[[]base.State]

	switch a, d, err := dutil.NewLockUniqueAppendSlice[base.State, string](1<<9, 1<<9,
		func(n base.State) string { return n.Key() }); {
	case err != nil:
		panic(err)
	default:
		addState, doneState = a, d
	}

	workersWait = func() error {
		var wg sync.WaitGroup

		for worker, f := range maps.All(workersWaits) {
			wg.Add(1)

			go func() {
				defer wg.Done()

				err := f(
					func(_ context.Context, local, workID, worker string, unmerged []uint64) error {
						err := workersUnmerge[worker](local, unmerged)

						t.Println("worker broadcast unmerged to worker: local=%q worker=%q unmerged=%v err=%+v", local, worker, unmerged, err)

						return nil // FIXME
					},
					func(
						_ context.Context,
						local,
						workID string,
						reasons map[uint64]string,
						statesSize uint64,
						f dutil.InErrorFunc[dutil.InErrorFunc[base.State]],
					) error {
						t.Println("worker report result: local=%q reasons=%v states size=%d", local, reasons, statesSize)

						sendReasons, sendState, done, err := managerWorkerResult(local)
						if err != nil || sendReasons == nil {
							return err
						}

						if err := sendReasons(reasons); err != nil {
							return err
						}

						return done(f(func(st base.State) error {
							t.Println("worker report result: send state: local=%q key=%q %s", local, st.Key(), util.MustMarshalJSONString(st))

							addState(st)

							return sendState(st.Key(), st.Hash())
						}))
					},
				)
				t.Println("worker wait: local=%q err=%+v", worker, err)
			}()
		}

		wg.Wait()

		return nil
	}

	wait = func() (opsTree, statesTree *fixedtree.Tree, states []base.State, _ error) {
		return opsTree, statesTree, states, managerWait(
			func(_ context.Context, worker string) error {
				t.T().Logf("manager worker insert done: worker=%q", worker)

				_, o, err := workersInserts[worker](managerName)
				if err != nil {
					return err
				}

				return o(nil, nil)
			},
			func(o, s *fixedtree.Tree) (base.Manifest, error) {
				t.T().Logf("wait done: ops tree=%q states tree=%q", o.Root(), s.Root())

				opsTree, statesTree = o, s

				states = doneState()

				slices.SortFunc(states, func(a, b base.State) int {
					return strings.Compare(a.Key(), b.Key())
				})

				return nil, nil
			},
		)
	}

	return sendOps, workersWait, wait
}

func (t *testManagerWithWorkers) processCase(
	workersSize, opsShare, statesShare uint64,
	ops []Operation,
	getStateOwners getStateOwnersFunc,
	getState getStateFunc,
) (
	expectedOpsTree, expectedStatesTree *fixedtree.Tree,
	states []base.State,
	_ error,
) {
	ctx, cancel := t.Ctx()
	defer cancel(nil)

	workers := t.workers(int(workersSize))

	if getStateOwners == nil {
		f, stateOwnersClose, err := stateOwnersFuncByCount(workers, statesShare)
		t.NoError(err)
		defer stateOwnersClose()

		getStateOwners = f
	}

	if getState == nil {
		getState = func(string) (base.State, bool, error) {
			return nil, false, nil
		}
	}

	sendOps, workersWait, wait := t.newManager(
		ctx,
		workers,
		opsShare, statesShare,
		ops,
		getState,
		func(stateKey string) []string {
			owners := getStateOwners(stateKey)

			t.Println("state owners: state key=%q owners=%v", stateKey, owners)

			return owners
		},
	)

	t.NoError(sendOps())
	t.NoError(workersWait())

	return wait()
}

func (t *testManagerWithWorkers) runCase(
	workersSize, opsShare, statesShare uint64,
	getStateOwners getStateOwnersFunc,
	getState getStateFunc,
	ops []Operation,
	expectedOpsTree, expectedStatesTree *fixedtree.Tree,
	expectedStates []base.State,
) {
	t.T().Helper()

	if getState == nil {
		getState = func(stateKey string) (base.State, bool, error) {
			return nil, false, nil
		}
	}

	t.Run(
		fmt.Sprintf("%d worker, ops share=%d states share=%d", workersSize, opsShare, statesShare),
		func() {
			opsTree, statesTree, states, err := t.processCase(workersSize, opsShare, statesShare, ops, getStateOwners, getState)
			t.NoError(err)

			t.T().Logf("ops tree root=%q states tree root=%q", opsTree.Root(), statesTree.Root())

			opsTree.Traverse(func(index uint64, n fixedtree.Node) (bool, error) {
				t.T().Logf("ops tree: index=%d key=%q %s", index, n.Key(), util.MustMarshalJSONString(n))

				return true, nil
			})
			statesTree.Traverse(func(index uint64, n fixedtree.Node) (bool, error) {
				t.T().Logf("states tree: index=%d key=%q %s", index, n.Key(), util.MustMarshalJSONString(n))

				return true, nil
			})

			for i, st := range slices.All(states) {
				t.T().Logf("state: index=%d key=%q %s", i, st.Key(), util.MustMarshalJSONString(st))
			}

			t.True(expectedOpsTree.Root().Equal(opsTree.Root()), "%q != %q", expectedOpsTree.Root(), opsTree.Root())
			t.True(expectedStatesTree.Root().Equal(statesTree.Root()), "%q != %q", expectedStatesTree.Root(), statesTree.Root())

			if expectedStates != nil {
				t.Equal(len(expectedStates), len(states))

				for i, b := range slices.All(states) {
					a := expectedStates[i]

					t.True(base.IsEqualState(a, b))
				}
			}
		},
	)
}

func (t *testManagerWithWorkers) TestOneCase() {
	t.T().Skip()

	ops, _ := t.randomOps(19)

	expectedOpsTree, expectedStatesTree, expectedStates, err := t.processOps(
		context.Background(),
		ops,
		func(string) (base.State, bool, error) { return nil, false, nil },
	)
	t.NoError(err)

	for range 333 {
		t.runCase(12, 12, 1, nil, nil, ops, expectedOpsTree, expectedStatesTree, expectedStates)
	}
}

func (t *testManagerWithWorkers) TestWorkersOnlyAddValues() {
	ops, _ := t.randomOps(19)

	expectedOpsTree, expectedStatesTree, expectedStates, err := t.processOps(
		context.Background(),
		ops,
		func(string) (base.State, bool, error) { return nil, false, nil },
	)
	t.NoError(err)

	t.T().Logf("expected: ops tree=%q states tree=%q", expectedOpsTree.Root(), expectedStatesTree.Root())

	t.Run("one worker", func() {
		t.runCase(1, 1, 1, nil, nil, ops, expectedOpsTree, expectedStatesTree, nil)
	})

	t.Run("1 ops share, 1 states share", func() {
		var opsShare, statesShare uint64 = 1, 1

		t.runCase(3, opsShare, statesShare, nil, nil, ops, expectedOpsTree, expectedStatesTree, expectedStates)
		t.runCase(6, opsShare, statesShare, nil, nil, ops, expectedOpsTree, expectedStatesTree, expectedStates)
	})

	t.Run("2 ops share, 1 states share", func() {
		var opsShare, statesShare uint64 = 2, 1

		t.runCase(3, opsShare, statesShare, nil, nil, ops, expectedOpsTree, expectedStatesTree, expectedStates)
		t.runCase(6, opsShare, statesShare, nil, nil, ops, expectedOpsTree, expectedStatesTree, expectedStates)
	})

	t.Run("permutation", func() {
		var maxWorkersSize uint64 = uint64(len(ops))

		for workersSize := uint64(1); workersSize <= maxWorkersSize; workersSize++ {
			for opsShare := uint64(1); opsShare <= workersSize; opsShare++ {
				for statesShare := uint64(1); statesShare <= workersSize; statesShare++ {
					t.runCase(workersSize, opsShare, statesShare, nil, nil, ops, expectedOpsTree, expectedStatesTree, expectedStates)
				}
			}
		}
	})
}

func (t *testManagerWithWorkers) TestWorkersWithSubValues() {
	pub0 := base.NewDummyPublickey("p0")
	pub1 := base.NewDummyPublickey("p1")
	pub2 := base.NewDummyPublickey("p2")
	st0 := t.newState(pub0, 1)
	st1 := t.newState(pub1, 2)
	st2 := t.newState(pub2, 3)

	prevStates := map[string]base.State{
		st0.Key(): st0,
		st1.Key(): st1,
		st2.Key(): st2,
	}

	expectedStatesValues := map[string]int64{}
	for stateKey := range maps.Keys(prevStates) {
		switch stateKey {
		case st0.Key():
			expectedStatesValues[stateKey] = 2
		case st1.Key():
			expectedStatesValues[stateKey] = 3
		case st2.Key():
			expectedStatesValues[stateKey] = 2
		}
	}

	ops := []Operation{
		t.newDummyOp([]byte("0"), pub0, 1, pub1, -2), // 0
		t.newDummyOp([]byte("1"), pub1, -2, pub2, 3), // 1: ignored
		t.newDummyOp([]byte("2"), pub0, -1, pub1, 2), // 2
		t.newDummyOp([]byte("3"), pub0, -1, pub1, 2), // 3: ignored
		t.newDummyOp([]byte("4"), pub0, 1, pub2, -3), // 4
		t.newDummyOp([]byte("5"), pub1, 1, pub2, 2),  // 5
	}

	for opindex, op := range slices.All(ops) {
		t.T().Logf("op: opindex=%d hash=%q fact=%q", opindex, op.Hash(), op.Fact().Hash())
	}

	getState := func(stateKey string) (base.State, bool, error) {
		if st, found := prevStates[stateKey]; found {
			return st, true, nil
		}

		panic(fmt.Sprintf("unknown state, %q", stateKey))

		return nil, false, nil
	}

	expectedOpsTree, expectedStatesTree, expectedStates, err := t.processOps(context.Background(), ops, getState)
	t.NoError(err)

	t.T().Logf("expected: ops tree root=%q states tree root=%q", expectedOpsTree.Root(), expectedStatesTree.Root())

	expectedOpsTree.Traverse(func(index uint64, n fixedtree.Node) (bool, error) {
		t.T().Logf("expected ops tree: index=%d key=%q %s", index, n.Key(), util.MustMarshalJSONString(n))

		return true, nil
	})
	expectedStatesTree.Traverse(func(index uint64, n fixedtree.Node) (bool, error) {
		t.T().Logf("expected states tree: index=%d key=%q %s", index, n.Key(), util.MustMarshalJSONString(n))

		return true, nil
	})

	for i, st := range slices.All(expectedStates) {
		t.T().Logf("expected state: index=%d key=%q %s", i, st.Key(), util.MustMarshalJSONString(st))
	}

	for st := range slices.Values(expectedStates) {
		ac, ok := st.Value().(*AccountStateValue)
		t.True(ok)

		t.Equal(expectedStatesValues[st.Key()], ac.Balance)
	}

	t.Run("one worker", func() {
		t.T().Skip() // FIXME remove

		var opsShare, statesShare uint64 = 1, 1

		t.runCase(1, opsShare, statesShare, nil, getState, ops, expectedOpsTree, expectedStatesTree, expectedStates)
	})

	t.Run("1 ops share, 1 states share", func() {
		/*
			     opindex   | subs  | affected
			w0 | 0 2 3 4   | w1 w2 | w1 w2
			w1 | 0 1 2 3 5 | w0 w1 | w0 w2
			w2 | 1 4 5     | w1 w2 | w0
		*/

		var opsShare, statesShare uint64 = 1, 1

		getStateOwners := func(stateKey string) (owners []string) {
			var owner string

			if _, found := prevStates[stateKey]; found {
				switch stateKey {
				case st0.Key():
					owner = "w0"
				case st1.Key():
					owner = "w1"
				case st2.Key():
					owner = "w2"
				}
			}

			if owner == "" {
				panic(fmt.Sprintf("unknown state, %q", stateKey))
			}

			return []string{owner}
		}

		t.runCase(3, opsShare, statesShare, getStateOwners, getState, ops, expectedOpsTree, expectedStatesTree, expectedStates)
		// FIXME remove t.runCase(6, opsShare, statesShare, nil, getState, ops, expectedOpsTree, expectedStatesTree, expectedStates)
	})
}

func TestManagerWithWorkers(t *testing.T) {
	defer goleak.VerifyNone(t)

	suite.Run(t, new(testManagerWithWorkers))
}
