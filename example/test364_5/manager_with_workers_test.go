package main

import (
	"context"
	"fmt"
	"maps"
	"slices"
	"sync"
	"testing"

	"github.com/spikeekips/mitum/base"
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

func (t *testManagerWithWorkers) getOperationFunc(ops []Operation) getOperationFunc {
	return func(_ context.Context, opindex uint64, _, _ util.Hash) (Operation, bool, error) {
		if opindex >= uint64(len(ops)) {
			return nil, false, nil
		}

		return ops[opindex], true, nil
	}
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
	managerSendOps dutil.In2ErrorFunc[managerStartWorkerFunc, newManagerWorkerSendOpFunc],
	managerWorkerResult managerInsertWorkerResultFunc,
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
			managerSendOps,
			managerWorkerResult,
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
	managerSendOps dutil.In2ErrorFunc[managerStartWorkerFunc, newManagerWorkerSendOpFunc],
	managerWorkerResult managerInsertWorkerResultFunc,
	workID,
	local string,
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
		managerSendOps, managerWorkerResult,
		workID,
		workers,
		opsShare, statesShare,
		ophs,
	)

	getOperation := t.getOperationFunc(ops)
	distributeValues := func(local string) newWorkerDistributeStateMergeValuesFunc {
		return func(ctx context.Context, workID, worker string) (
			workerProcessInsertStateMergeValuesFunc,
			dutil.In2ErrorFunc[[]string /* subs */, []string /* affected */],
			error,
		) {
			return workersInserts[worker](local)
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
						return workersInitialInserts[worker](
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
						t.Println("worker broadcast unmerged to worker: local=%q worker=%q unmerged=%v", local, worker, unmerged)

						return workersUnmerge[worker](local, unmerged)
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
				t.Println("worker wait: local=%q err=%v", worker, err)
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

func (t *testManagerWithWorkers) TestWorkersSameResult() {
	ops, _ := t.randomOps(19)

	var oneOpsTree, oneStatesTree util.Hash

	t.Run("one worker", func() {
		ctx, cancel := t.Ctx()
		defer cancel(nil)

		workers := t.workers(1)
		var opsShare uint64 = 1
		getStateOwners, stateOwnersClose, err := stateOwnersFuncByCount(workers, opsShare)
		t.NoError(err)
		defer stateOwnersClose()

		sendOps, workersWait, wait := t.newManager(
			ctx,
			workers,
			opsShare, 1,
			ops,
			getStateOwners,
		)

		t.NoError(sendOps())
		t.NoError(workersWait())

		oneOpsTree, oneStatesTree, err = wait()
		t.NoError(err)
	})

	t.Run("1 ops share, 1 states share", func() {
		var opsShare, statesShare uint64 = 1, 1

		t.Run("3 worker", func() {
			ctx, cancel := t.Ctx()
			defer cancel(nil)

			workers := t.workers(3)
			getStateOwners, stateOwnersClose, err := stateOwnersFuncByCount(workers, opsShare)
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

			opsTree, statesTree, err := wait()
			t.NoError(err)

			t.True(oneOpsTree.Equal(opsTree))
			t.True(oneStatesTree.Equal(statesTree))
		})

		t.Run("6 worker", func() {
			ctx, cancel := t.Ctx()
			defer cancel(nil)

			workers := t.workers(3)
			getStateOwners, stateOwnersClose, err := stateOwnersFuncByCount(workers, opsShare)
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

			opsTree, statesTree, err := wait()
			t.NoError(err)

			t.True(oneOpsTree.Equal(opsTree))
			t.True(oneStatesTree.Equal(statesTree))
		})
	})

	t.Run("2 ops share, 1 states share", func() {
		var opsShare, statesShare uint64 = 2, 1

		t.Run("3 worker", func() {
			ctx, cancel := t.Ctx()
			defer cancel(nil)

			workers := t.workers(3)
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

			opsTree, statesTree, err := wait()
			t.NoError(err)

			t.True(oneOpsTree.Equal(opsTree))
			t.True(oneStatesTree.Equal(statesTree))
		})

		t.Run("6 worker", func() {
			ctx, cancel := t.Ctx()
			defer cancel(nil)

			workers := t.workers(6)
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

			opsTree, statesTree, err := wait()
			t.NoError(err)

			t.True(oneOpsTree.Equal(opsTree))
			t.True(oneStatesTree.Equal(statesTree))
		})
	})

	t.Run("test case", func() {
		t.T().Skip()

		workersSize := 4
		var opsShare, statesShare uint64 = 4, 4

		t.Run(
			fmt.Sprintf("%d worker, ops share=%d states share=%d",
				workersSize, opsShare, statesShare),
			func() {
				ctx, cancel := t.Ctx()
				defer cancel(nil)

				workers := t.workers(workersSize)
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

				opsTree, statesTree, err := wait()
				t.NoError(err)

				t.True(oneOpsTree.Equal(opsTree))
				t.True(oneStatesTree.Equal(statesTree))
			},
		)
	})

	t.Run("permutation", func() {
		var maxWorkersSize uint64 = uint64(len(ops))

		for w := uint64(1); w <= maxWorkersSize; w++ {
			for opsShare := uint64(1); opsShare <= w; opsShare++ {
				for statesShare := uint64(1); statesShare <= w; statesShare++ {
					t.Run(
						fmt.Sprintf("%d worker, ops share=%d states share=%d",
							w, opsShare, statesShare),
						func() {
							ctx, cancel := t.Ctx()
							defer cancel(nil)

							workers := t.workers(int(w))
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

							opsTree, statesTree, err := wait()
							t.NoError(err)

							t.True(oneOpsTree.Equal(opsTree), "%q != %q", oneOpsTree, opsTree)
							t.True(oneStatesTree.Equal(statesTree), "%q != %q", oneStatesTree, statesTree)
						},
					)
				}
			}
		}
	})
}

// FIXME check ops tree and states tree
// FIXME test; sub ops

func TestManagerWithWorkers(t *testing.T) {
	defer goleak.VerifyNone(t)

	suite.Run(t, new(testManagerWithWorkers))
}
