package main

import (
	"context"
	"fmt"
	"maps"
	"slices"
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
	baseTestAccountStateMerger
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
	ops := t.baseTestAccountStateMerger.randomAddOps(n, 1)

	ophs := make([][2]util.Hash, len(ops))
	for i, op := range dutil.SliceAll(ops) {
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
) (
	workerInitialInsertFunc,
	workerProcessWorkerInsertStateMergeValuesFunc,
	workerUnmergedFunc,
	workerWaitFunc,
) {
	getState := func(string) (base.State, bool, error) {
		return nil, false, nil
	}

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
	getStateOwners getStateOwnersFunc,
) (
	sendOps dutil.ErrorFunc,
	workersWait dutil.ErrorFunc,
	wait func() (util.Hash, util.Hash, error),
) {
	switch nworkers := uint64(len(workers)); {
	case opsShare > nworkers:
		panic(fmt.Sprintf("opsShare=%d > workers=%d", opsShare, nworkers))
	case statesShare > nworkers:
		panic(fmt.Sprintf("statesShare=%d > workers=%d", statesShare, nworkers))
	}

	workID := util.UUID().String()

	ophs := make([][2]util.Hash, len(ops))
	for i, op := range dutil.SliceAll(ops) {
		ophs[i] = [2]util.Hash{op.Hash(), op.Fact().Hash()}
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
					err := i(opindex, fact, values)
					if err != nil {
						t.Println("distribute worker: insert: local=%q worker=%q opindex=%d err=%+v", local, worker, opindex, err)
					}

					return err
				},
				func(subs, affected []string) error {
					err := o(subs, affected)
					if err != nil {
						t.Println("distribute worker: owners: local=%q worker=%q err=%+v", local, worker, err)
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
						wops[opindex] = [2]util.Hash{oph, fact}

						return nil
					},
					func(err error) error {
						t.Println("manager initial insert: worker=%q err=%+v", worker, err)

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

	wait = func() (opsTree, statesTree util.Hash, _ error) {
		return opsTree, statesTree, managerWait(
			func(_ context.Context, worker string) error {
				t.T().Logf("manager worker insert done: worker=%q", worker)

				_, o, err := workersInserts[worker](managerName)
				if err != nil {
					return err
				}

				return o(nil, nil)
			},
			func(o, s util.Hash) (base.Manifest, error) {
				t.T().Logf("wait done: ops tree=%q states tree=%q", o, s)

				opsTree, statesTree = o, s

				return nil, nil
			},
		)
	}

	return sendOps, workersWait, wait
}

func (t *testManagerWithWorkers) processCase(
	workersSize, opsShare, statesShare uint64,
	ops []Operation,
) (
	expectedOpsTree, expectedStatesTree util.Hash,
	_ error,
) {
	ctx, cancel := t.Ctx()
	defer cancel(nil)

	workers := t.workers(int(workersSize))
	getStateOwners, stateOwnersClose, err := stateOwnersFuncByCount(workers, statesShare)
	t.NoError(err)
	defer stateOwnersClose()

	sendOps, workersWait, wait := t.newManager(
		ctx,
		workers,
		opsShare, statesShare,
		ops,
		getStateOwners,
	)

	t.NoError(sendOps())
	t.NoError(workersWait())

	return wait()
}

func (t *testManagerWithWorkers) runCase(
	workersSize, opsShare, statesShare uint64,
	ops []Operation,
	expectedOpsTree, expectedStatesTree *fixedtree.Tree,
) {
	t.Run(
		fmt.Sprintf("%d worker, ops share=%d states share=%d", workersSize, opsShare, statesShare),
		func() {
			opsTree, statesTree, err := t.processCase(workersSize, opsShare, statesShare, ops)
			t.NoError(err)

			t.True(expectedOpsTree.Root().Equal(opsTree), "%q != %q", expectedOpsTree.Root(), opsTree)
			t.True(expectedStatesTree.Root().Equal(statesTree), "%q != %q", expectedStatesTree.Root(), statesTree)
		},
	)
}

func (t *testManagerWithWorkers) TestOneCase() {
	t.T().Skip()

	ops, _ := t.randomOps(19)

	expectedOpsTree, expectedStatesTree, err := t.processOps(
		context.Background(),
		ops,
		func(string) (base.State, bool, error) { return nil, false, nil },
	)
	t.NoError(err)

	for range 333 {
		t.runCase(12, 12, 1, ops, expectedOpsTree, expectedStatesTree)
	}
}

func (t *testManagerWithWorkers) TestWorkersSameResult() {
	ops, _ := t.randomOps(19)

	expectedOpsTree, expectedStatesTree, err := t.processOps(
		context.Background(),
		ops,
		func(string) (base.State, bool, error) { return nil, false, nil },
	)
	t.NoError(err)

	t.Run("one worker", func() {
		t.runCase(1, 1, 1, ops, expectedOpsTree, expectedStatesTree)
	})

	t.Run("1 ops share, 1 states share", func() {
		var opsShare, statesShare uint64 = 1, 1

		t.runCase(3, opsShare, statesShare, ops, expectedOpsTree, expectedStatesTree)
		t.runCase(6, opsShare, statesShare, ops, expectedOpsTree, expectedStatesTree)
	})

	t.Run("2 ops share, 1 states share", func() {
		var opsShare, statesShare uint64 = 2, 1

		t.runCase(3, opsShare, statesShare, ops, expectedOpsTree, expectedStatesTree)
		t.runCase(6, opsShare, statesShare, ops, expectedOpsTree, expectedStatesTree)
	})

	t.Run("permutation", func() {
		var maxWorkersSize uint64 = uint64(len(ops))

		for workersSize := uint64(1); workersSize <= maxWorkersSize; workersSize++ {
			for opsShare := uint64(1); opsShare <= workersSize; opsShare++ {
				for statesShare := uint64(1); statesShare <= workersSize; statesShare++ {
					t.runCase(workersSize, opsShare, statesShare, ops, expectedOpsTree, expectedStatesTree)
				}
			}
		}
	})
}

// FIXME test; sub ops

func TestManagerWithWorkers(t *testing.T) {
	defer goleak.VerifyNone(t)

	suite.Run(t, new(testManagerWithWorkers))
}
