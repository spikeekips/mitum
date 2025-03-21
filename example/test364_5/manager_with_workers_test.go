package dop5

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
	baseTestAccountSVM
	baseTestWorkers
	baseTestManager
}

func (t *testManagerWithWorkers) newManagerOptions() *ManagerOptions {
	return NewManagerOptions()
}

func (t *testManagerWithWorkers) randomFacts(n int) ([]OperationFact, [][2]util.Hash) {
	facts := t.baseTestAccountSVM.randomAddFacts(n, 1)

	ophs := make([][2]util.Hash, len(facts))
	for i, fact := range slices.All(facts) {
		ophs[i] = [2]util.Hash{fact.Hash(), fact.Hash()}
	}

	return facts, ophs
}

func (t *testManagerWithWorkers) workerIfErrCancel(worker string, cancel dutil.InErrorFunc[error]) dutil.InErrorFunc[error] {
	return func(err error) error {
		t.Print("error: worker=%q err=%w", worker, err)
		if err != nil {
			return cancel(err)
		}

		return nil
	}
}

func (t *testManagerWithWorkers) newWorkers(
	ctx context.Context,
	workers []string,
	opsShare, statesShare uint64,
	ophs [][2]util.Hash,
	getState GetStateFunc,
	reportWorkerCancelWork dutil.In2ErrorFunc[string, error],
) (
	workersInitialInserts map[string]WorkerInitialInsertFunc,
	workersInserts map[string]WorkerProcessWorkerInsertSMVsFunc,
	workersUnmerge map[string]WorkerUnmergedFunc,
	workersWaits map[string]WorkerWaitFunc,
	workersCloses map[string]WorkerCloseFunc,
) {
	workersInitialInserts = map[string]WorkerInitialInsertFunc{}
	workersInserts = map[string]WorkerProcessWorkerInsertSMVsFunc{}
	workersUnmerge = map[string]WorkerUnmergedFunc{}
	workersWaits = map[string]WorkerWaitFunc{}
	workersCloses = map[string]WorkerCloseFunc{}

	var opsWorkers []string

	{
		w, _, err := opsNShareWorkers(workers, opsShare, ophs)
		t.NoError(err)
		opsWorkers = opsWorkersFromSeq(w)
	}

	for local := range slices.Values(workers) {
		t, i, u, w, c := t.newWorker(
			ctx,
			local,
			workers, opsWorkers,
			opsShare, statesShare,
			uint64(len(ophs)),
			getState,
			func(err error) error {
				return reportWorkerCancelWork(local, err)
			},
		)

		workersInitialInserts[local] = t
		workersInserts[local] = i
		workersUnmerge[local] = u
		workersWaits[local] = w
		workersCloses[local] = c
	}

	return workersInitialInserts,
		workersInserts,
		workersUnmerge,
		workersWaits,
		workersCloses
}

func (t *testManagerWithWorkers) newWorker(
	ctx context.Context,
	local string,
	workers, opsWorkers []string,
	opsShare, statesShare uint64,
	maxOpsSize uint64,
	getState GetStateFunc,
	reportCancelWork dutil.InErrorFunc[error],
) (
	WorkerInitialInsertFunc,
	WorkerProcessWorkerInsertSMVsFunc,
	WorkerUnmergedFunc,
	WorkerWaitFunc,
	WorkerCloseFunc,
) {
	createState := func(stateKey string, value base.StateValue, prev util.Hash, ops []util.Hash) (base.State, error) {
		st := base.NewBaseState(
			33,
			stateKey,
			value,
			prev,
			ops,
		)
		// t.Println("new state: key=%q hash=%q ops=%v value=%s", st.Key(), st.Hash(), ops, util.MustMarshalJSONString(value))

		return st, nil
	}

	initialInsert, workerInsert, workerUnmerge, workerWait, closeWork, err := NewFuncWorker(
		ctx,
		t.newWorkerOptions(local),
		getState,
		createState,
		reportCancelWork,
		workers, opsWorkers,
		opsShare, statesShare,
		maxOpsSize,
	)
	t.PrintIfErrPanic(err, "worker=%q", local)

	return initialInsert, workerInsert, workerUnmerge, workerWait, closeWork
}

func (t *testManagerWithWorkers) newManager(
	ctx context.Context,
	workers []string,
	opsShare, statesShare uint64,
	facts []OperationFact,
	getState GetStateFunc,
	getStateOwners getStateOwnersFunc,
	expectedWorkersOwners map[string]map[string][][]string,
	expectedStatesValues map[string]int64,
) (
	sendOps dutil.ErrorFunc,
	wait func() (_, _ *fixedtree.Tree, _ []base.State, _ error),
) {
	switch nworkers := uint64(len(workers)); {
	case opsShare > nworkers:
		t.Panicf(nil, "opsShare=%d > workers=%d", opsShare, nworkers)
	case statesShare > nworkers:
		t.Panicf(nil, "statesShare=%d > workers=%d", statesShare, nworkers)
	}

	ophs := make([][2]util.Hash, len(facts))
	for i, fact := range slices.All(facts) {
		ophs[i] = [2]util.Hash{fact.Hash(), fact.Hash()}
	}

	managerSendOps, managerWorkerOwners, managerWorkerResult, managerWorkerErr, managerWait, err := NewFuncManager(
		ctx,
		t.newManagerOptions(),
		func(_ context.Context, worker string, err error) error {
			t.IfErrPanicf(err, "worker canceled by manager")

			return nil
		},
		workers,
		opsShare, statesShare,
		ophs,
	)
	t.NoError(err)

	workersInitialInserts, workersInserts, workersUnmerge, workersWaits, workersCloses := t.newWorkers(
		ctx,
		workers,
		opsShare, statesShare,
		ophs,
		getState,
		managerWorkerErr,
	)

	getOperationFact := newFuncGetOperationFact(facts)
	distributeValues := func(local string) NewWorkerDistributeSMVsFunc {
		return func(ctx context.Context, worker string) (
			WorkerProcessInsertSMVsFunc,
			dutil.In2ErrorFunc[[]string, []string],
			error,
		) {
			i, o, err := workersInserts[worker](local)

			switch {
			case err != nil:
				t.Print("distribute worker: local=%q worker=%q err=%+v", local, worker, err)

				return nil, nil, err
			case i == nil:
				return nil, nil, nil
			}

			return func(opindex uint64, facth util.Hash, values []SMV) error {
					stateKeys := extractSMVKeysFromValues(values)

					err := i(opindex, facth, values)
					t.Print("distribute worker: insert: local=%q worker=%q opindex=%d fact=%q state keys=%v err=%+v", local, worker, opindex, facth, stateKeys, err)

					return err
				},
				func(subws, affected []string) error {
					err := o(subws, affected)

					t.Print("distribute worker: owners: local=%q worker=%q sub=%v affected=%v err=%+v", local, worker, subws, affected, err)

					if expectedWorkersOwners != nil {
						a := expectedWorkersOwners[local][worker]
						asubws := a[0]
						aaffected := a[1]

						if !slices.Equal(asubws, subws) {
							t.Panicf(nil, "sub: local=%q worker=%q expected=%v result=%v", local, worker, asubws, subws)
						}

						if !slices.Equal(aaffected, affected) {
							t.Panicf(nil, "affected: local=%q worker=%q expected=%v result=%v", local, worker, aaffected, affected)
						}
					}

					return err
				},
				nil
		}
	}

	sendOps = func() error {
		return managerSendOps(
			func(
				_ context.Context,
				worker string,
				workers, opsWorkers []string,
				opsShare, statesShare uint64,
				maxOpsSize uint64,
			) error {
				//t.Println("start worker: work id=%q worker=%q workers=%v opsWorkers=%v ops share=%d states share=%d max ops size=%d",
				//	worker,
				//	workers, opsWorkers,
				//	opsShare, statesShare,
				//	maxOpsSize,
				//)

				return nil
			},
			func(_ context.Context, worker string, maxItems uint64) (OpindexOpHashesFunc, dutil.InErrorFunc[error], error) {
				wops := map[uint64][2]util.Hash{}

				return func(opindex uint64, oph, facth util.Hash) error {
						t.Print("manager initial insert: worker=%q opindex=%d fact=%q", worker, opindex, facth)

						wops[opindex] = [2]util.Hash{oph, facth}

						return nil
					},
					func(err error) error {
						if err == nil {
							err := workersInitialInserts[worker](
								getStateOwners,
								distributeValues(worker),
								getOperationFact,
								maps.All(wops),
								func(
									_ context.Context,
									owners []string,
								) error {
									err := managerWorkerOwners(worker, owners)
									t.Print("worker report manager: owners: local=%q owners=%v err=%v", worker, owners, err)

									return nil
								},
							)

							t.Print("manager workers initial insert done: worker=%q err=%+v", worker, err)
						}

						return nil
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
		t.IfErrPanicf(err, "append base.State")
	default:
		addState, doneState = a, d
	}

	workersWait := func() error {
		var wg sync.WaitGroup

		for local, f := range maps.All(workersWaits) {
			wg.Add(1)

			go func() {
				defer wg.Done()

				err := f(
					func(_ context.Context, worker string, unmerged []uint64) error {
						err := workersUnmerge[worker](local, unmerged)

						t.Print("worker broadcast unmerged to worker: local=%q worker=%q unmerged=%v err=%+v", local, worker, unmerged, err)

						return nil
					},
					func(
						_ context.Context,
						reasons map[uint64]string,
						statesSize uint64,
						f dutil.InErrorFunc[dutil.InErrorFunc[base.State]],
					) error {
						sendReasons, sendState, done, err := managerWorkerResult(local)
						t.Print("worker report result: local=%q reasons=%v states size=%d sendReasons=%v", local, reasons, statesSize, sendReasons == nil)

						if err != nil || sendReasons == nil {
							return err
						}

						if err := sendReasons(reasons); err != nil {
							t.Print("worker report result: send reasons: local=%q err=%+v", local, err)

							return err
						}

						return done(f(func(st base.State) error {
							addState(st)

							err := sendState(st.Key(), st.Hash())
							t.Print("worker report result: send state: local=%q key=%q err=%+v %s", local, st.Key(), err, util.MustMarshalJSONString(st))

							if expectedStatesValues != nil {
								b := st.Value().(*AccountSV).Balance

								e, found := expectedStatesValues[st.Key()]
								t.Print("worker report result: state: local=%q key=%q expected=%v result=%v found=%v", local, st.Key(), e, b, found)
								switch {
								case !found:
									t.Panicf(nil, "unknown state: worker=%q state key=%q", local, st.Key())
								case b != e:
									t.Panicf(nil, "wrong balance: worker=%q state key=%q expected=%d result=%d",
										local, st.Key(), e, b)
								}
							}

							return err
						}))
					},
					func(sts []base.State) error {
						return nil
					},
				)
				t.Print("worker wait: local=%q err=%+v", local, err)
			}()
		}

		wg.Wait()

		return nil
	}

	workersClosed := make(chan error, 1)
	go func() {
		workersClosed <- workersWait()
	}()

	wait = func() (opsTree, statesTree *fixedtree.Tree, states []base.State, _ error) {
		err := managerWait(
			func(_ context.Context, worker string) error {
				var err1 error

				_, o, err0 := workersInserts[worker](managerName)

				if err != nil {
					err1 = o(nil, nil)
				}

				t.T().Logf("manager worker insert done: worker=%q err0=%+v err1=%v", worker, err0, err1)

				switch {
				case err0 != nil:
					return err0
				default:
					return err1
				}
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
			func(_ context.Context, worker string) error {
				t.T().Logf("done worker: worker=%q", worker)

				return workersCloses[worker](nil)
			},
		)
		t.Print("manager wait done")

		if err == nil {
			err = <-workersClosed
		}

		return opsTree, statesTree, states, err
	}

	return sendOps, wait
}

func (t *testManagerWithWorkers) processCase(
	workersSize, opsShare, statesShare uint64,
	facts []OperationFact,
	getStateOwners getStateOwnersFunc,
	getState GetStateFunc,
	expectedWorkersOwners map[string]map[string][][]string,
	expectedStatesValues map[string]int64,
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

	statesOwners := map[string][]string{}
	for fact := range slices.Values(facts) {
		sub, adds, _, err := fact.SMVs(ctx)
		t.NoError(err)

		if sub != nil {
			stateKey := sub.Key()
			if _, found := statesOwners[stateKey]; !found {
				statesOwners[stateKey] = getStateOwners(stateKey)
			}
		}

		for value := range slices.Values(adds) {
			stateKey := value.Key()
			if _, found := statesOwners[stateKey]; !found {
				statesOwners[stateKey] = getStateOwners(stateKey)
			}
		}
	}

	stateKeys := slices.Collect(maps.Keys(statesOwners))
	slices.Sort(stateKeys)
	for stateKey := range slices.Values(stateKeys) {
		t.Print("state owners: state key=%q owners=%v", stateKey, statesOwners[stateKey])
	}

	sendOps, wait := t.newManager(
		ctx,
		workers,
		opsShare, statesShare,
		facts,
		getState,
		getStateOwners,
		expectedWorkersOwners,
		expectedStatesValues,
	)

	t.NoError(sendOps())

	return wait()
}

func (t *testManagerWithWorkers) runCase(
	workersSize, opsShare, statesShare uint64,
	getStateOwners getStateOwnersFunc,
	getState GetStateFunc,
	facts []OperationFact,
	expectedOpsTree, expectedStatesTree *fixedtree.Tree,
	expectedStates []base.State,
	expectedWorkersOwners map[string]map[string][][]string,
	expectedStatesValues map[string]int64,
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
			opsTree, statesTree, states, err := t.processCase(workersSize, opsShare, statesShare, facts, getStateOwners, getState,
				expectedWorkersOwners, expectedStatesValues)
			t.NoError(err)

			t.T().Logf("ops tree root=%q states tree root=%q", opsTree.Root(), statesTree.Root())

			for index, n := range opsTree.Traverse() {
				t.T().Logf("ops tree: index=%d key=%q %s", index, n.Key(), util.MustMarshalJSONString(n))
			}
			for index, n := range statesTree.Traverse() {
				t.T().Logf("states tree: index=%d key=%q %s", index, n.Key(), util.MustMarshalJSONString(n))
			}

			for i, st := range slices.All(states) {
				t.T().Logf("state: index=%d key=%q %s", i, st.Key(), util.MustMarshalJSONString(st))
			}

			t.Equal(expectedOpsTree.Root(), opsTree.Root(), "%q != %q", expectedOpsTree.Root(), opsTree.Root())
			t.Equal(expectedStatesTree.Root(), statesTree.Root(), "%q != %q", expectedStatesTree.Root(), statesTree.Root())

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

	facts, _ := t.randomFacts(19)

	expectedOpsTree, expectedStatesTree, expectedStates, expectedStatesValues, err := t.processOps(
		context.Background(),
		facts,
		func(string) (base.State, bool, error) { return nil, false, nil },
	)
	t.NoError(err)

	for range 333 {
		t.runCase(12, 12, 1, nil, nil, facts, expectedOpsTree, expectedStatesTree, expectedStates, nil, expectedStatesValues)
	}
}

func (t *testManagerWithWorkers) TestWorkersOnlyAddValues() {
	var opsSize uint64 = 9
	facts, _ := t.randomFacts(int(opsSize))

	expectedOpsTree, expectedStatesTree, expectedStates, expectedStatesValues, err := t.processOps(
		context.Background(),
		facts,
		func(string) (base.State, bool, error) { return nil, false, nil },
	)
	t.NoError(err)

	t.T().Logf("expected: ops tree=%q states tree=%q", expectedOpsTree.Root(), expectedStatesTree.Root())

	t.Run("one worker", func() {
		t.runCase(1, 1, 1, nil, nil, facts, expectedOpsTree, expectedStatesTree, nil, nil, expectedStatesValues)
	})

	t.Run("1 ops share, 1 states share", func() {
		var opsShare, statesShare uint64 = 1, 1

		t.runCase(3, opsShare, statesShare, nil, nil, facts, expectedOpsTree, expectedStatesTree, expectedStates, nil, expectedStatesValues)
		t.runCase(6, opsShare, statesShare, nil, nil, facts, expectedOpsTree, expectedStatesTree, expectedStates, nil, expectedStatesValues)
	})

	t.Run("2 ops share, 1 states share", func() {
		var opsShare, statesShare uint64 = 2, 1

		t.runCase(3, opsShare, statesShare, nil, nil, facts, expectedOpsTree, expectedStatesTree, expectedStates, nil, expectedStatesValues)
		t.runCase(6, opsShare, statesShare, nil, nil, facts, expectedOpsTree, expectedStatesTree, expectedStates, nil, expectedStatesValues)
	})

	t.Run("permutation", func() {
		var maxWorkersSize uint64 = opsSize * 2

		for workersSize := uint64(1); workersSize <= maxWorkersSize; workersSize++ {
			o := opsSize
			if o > workersSize {
				o = workersSize
			}

			for opsShare := uint64(1); opsShare <= o; opsShare++ {
				for statesShare := uint64(1); statesShare <= workersSize; statesShare++ {
					t.runCase(workersSize, opsShare, statesShare, nil, nil, facts, expectedOpsTree, expectedStatesTree, expectedStates, nil, expectedStatesValues)
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

	facts := []OperationFact{
		t.newDummyFact([]byte("0"), pub0, 1, pub1, -2), // 0
		t.newDummyFact([]byte("1"), pub1, -2, pub2, 3), // 1: ignored
		t.newDummyFact([]byte("2"), pub0, -1, pub1, 2), // 2
		t.newDummyFact([]byte("3"), pub0, -1, pub1, 2), // 3: ignored
		t.newDummyFact([]byte("4"), pub0, 1, pub2, -3), // 4
		t.newDummyFact([]byte("5"), pub1, 1, pub2, 2),  // 5
	}

	for opindex, fact := range slices.All(facts) {
		t.T().Logf("op: opindex=%d hash=%q fact=%q", opindex, fact.Hash(), fact.Hash())
	}

	getState := func(stateKey string) (base.State, bool, error) {
		if st, found := prevStates[stateKey]; found {
			return st, true, nil
		}

		t.Panicf(nil, "unknown state, %q", stateKey)

		return nil, false, nil
	}

	expectedOpsTree, expectedStatesTree, expectedStates, expectedStatesValues, err := t.processOps(context.Background(), facts, getState)
	t.NoError(err)

	t.T().Logf("expected: ops tree root=%q states tree root=%q", expectedOpsTree.Root(), expectedStatesTree.Root())

	for index, n := range expectedOpsTree.Traverse() {
		t.T().Logf("expected ops tree: index=%d key=%q %s", index, n.Key(), util.MustMarshalJSONString(n))
	}
	for index, n := range expectedStatesTree.Traverse() {
		t.T().Logf("expected states tree: index=%d key=%q %s", index, n.Key(), util.MustMarshalJSONString(n))
	}

	for i, st := range slices.All(expectedStates) {
		t.T().Logf("expected state: index=%d key=%q %s", i, st.Key(), util.MustMarshalJSONString(st))
	}

	for st := range slices.Values(expectedStates) {
		ac, ok := st.Value().(*AccountSV)
		t.True(ok)

		t.Equal(expectedStatesValues[st.Key()], ac.Balance)
	}

	var sampleCalled bool

	t.Run("sample", func() {
		t.T().Skip() // NOTE uncomment for sample

		//      related   | ops | subs  | affected
		// w0 | 0 2 3 4   | 0 3 | w1 w2 | w1
		// w1 | 0 1 2 3 5 | 1 4 | w0    | w0 w2
		// w2 | 1 4 5     | 2 5 | w1    | w0

		// getStateOwners := func(stateKey string) (owners []string) { // NOTE comment
		//	var owner string

		//	if _, found := prevStates[stateKey]; found {
		//		switch stateKey {
		//		case st0.Key():
		//			owner = "w0"
		//		case st1.Key():
		//			owner = "w1"
		//		case st2.Key():
		//			owner = "w2"
		//		}
		//	}

		//	if owner == "" {
		//		t.Panicf(nil, "unknown state, %q", stateKey)
		//	}

		//	return []string{owner}
		//}

		//expectedWorkersOwners := map[string]map[string][][]string{
		//	"w0": {"w1": {{"w0"}, {"w0", "w2"}}, "w2": {{"w1"}, {"w0"}}},
		//	"w1": {"w0": {{"w1", "w2"}, {"w1"}}, "w2": {{"w1"}, {"w0"}}},
		//	"w2": {"w0": {{"w1", "w2"}, {"w1"}}, "w1": {{"w0"}, {"w0", "w2"}}},
		//}

		for range 333 {
			// t.runCase(3, 3, 1, nil, getState, ops, expectedOpsTree, expectedStatesTree, expectedStates, nil, expectedStatesValues)
			// t.runCase(3, 3, 1, nil, getState, ops, expectedOpsTree, expectedStatesTree, expectedStates, nil, expectedStatesValues)
			// t.runCase(7, 1, 5, nil, getState, ops, expectedOpsTree, expectedStatesTree, expectedStates, nil, expectedStatesValues)
			// t.runCase(9, 1, 3, nil, getState, ops, expectedOpsTree, expectedStatesTree, expectedStates, nil, expectedStatesValues)
			// t.runCase(11, 6, 9, nil, getState, ops, expectedOpsTree, expectedStatesTree, expectedStates, nil, expectedStatesValues)
			// t.runCase(13, 1, 10, nil, getState, ops, expectedOpsTree, expectedStatesTree, expectedStates, nil, expectedStatesValues)
			// t.runCase(11, 4, 8, nil, getState, ops, expectedOpsTree, expectedStatesTree, expectedStates, nil, expectedStatesValues)
			t.runCase(14, 3, 8, nil, getState, facts, expectedOpsTree, expectedStatesTree, expectedStates, nil, expectedStatesValues)
		}

		sampleCalled = true
	})

	if sampleCalled {
		return
	}

	t.Run("one worker", func() {
		var opsShare, statesShare uint64 = 1, 1

		t.runCase(1, opsShare, statesShare, nil, getState, facts, expectedOpsTree, expectedStatesTree, expectedStates, nil, expectedStatesValues)
	})

	t.Run("1 ops share, 1 states share", func() {
		var opsShare, statesShare uint64 = 1, 1

		t.runCase(3, opsShare, statesShare, nil, getState, facts, expectedOpsTree, expectedStatesTree, expectedStates, nil, expectedStatesValues)
		t.runCase(6, opsShare, statesShare, nil, getState, facts, expectedOpsTree, expectedStatesTree, expectedStates, nil, expectedStatesValues)
	})

	t.Run("2 ops share, 1 states share", func() {
		var opsShare, statesShare uint64 = 2, 1

		t.runCase(3, opsShare, statesShare, nil, getState, facts, expectedOpsTree, expectedStatesTree, expectedStates, nil, expectedStatesValues)
		t.runCase(6, opsShare, statesShare, nil, getState, facts, expectedOpsTree, expectedStatesTree, expectedStates, nil, expectedStatesValues)
	})

	t.Run("permutation", func() {
		var opsSize uint64 = uint64(len(facts))
		var maxWorkersSize uint64 = opsSize * 2

		for workersSize := uint64(1); workersSize <= maxWorkersSize; workersSize++ {
			o := opsSize
			if o > workersSize {
				o = workersSize
			}

			for opsShare := uint64(1); opsShare <= o; opsShare++ {
				for statesShare := uint64(1); statesShare <= workersSize; statesShare++ {
					t.runCase(workersSize, opsShare, statesShare, nil, getState, facts, expectedOpsTree, expectedStatesTree, expectedStates, nil, expectedStatesValues)
				}
			}
		}
	})
}

func TestManagerWithWorkers(t *testing.T) {
	defer goleak.VerifyNone(t)

	suite.Run(t, new(testManagerWithWorkers))
}
