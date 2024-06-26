package main

import (
	"context"
	"fmt"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/spikeekips/mitum/base"
	"github.com/spikeekips/mitum/util"
	"github.com/spikeekips/mitum/util/logging"
	"github.com/stretchr/testify/suite"
	"go.uber.org/goleak"
)

type testProcessWorker struct {
	baseTestMergeOp
}

func (t *testProcessWorker) newArgs(
	local string,
	workers []string,
	ops []Operation,
) (
	_ *PhaseMergerArgs,
	_ *workerPhaseMergerArgs,
	_ *workerMergerArgs,
	_ *processWorkerArgs,
	vp util.LockedMap[uint64, []StateMergeValue],
	states util.LockedMap[string, base.State],
) {
	if len(workers) < 1 {
		panic("empty workers")
	}

	mops := map[uint64]Operation{}

	for i := range ops {
		mops[uint64(i)] = ops[i]
	}

	vp, _, states, _, _ = t.baseTestMergeOp.getFuncs()

	pargs := NewPhaseMergerArgs()
	wpargs := newWorkerPhaseMergerArgs()
	wmargs := newWorkerMergerArgs()
	args := newProcessWorkerArgs()

	wmargs.NewWorkerPhaseMergerFuncs = func(phase int) (
		*workerPhaseMergerArgs,
		newWorkerPhaseMergerFunc,
		error,
	) {
		return newWorkerPhaseMergerArgs(), newWorkerPhaseMerger, nil
	}

	args.EnoughWorkers = uint64(len(workers))
	args.BroadcastPhaseDone = func(context.Context, int, []uint64, []uint64) error {
		return nil
	}
	args.NewPhaseMergerFuncs = func() (
		*PhaseMergerArgs,
		newPhaseMergerFunc,
		error,
	) {
		return pargs, NewPhaseMerger, nil
	}

	args.NewStateValuesProcessorFuncs = func() (
		*stateValuesProcessorArgs,
		newStateValuesProcessorFunc,
		error,
	) {
		nargs := newStateValuesProcessorArgs()

		nargs.GetOperation = func(_ context.Context, opindex uint64, _ util.Hash) (Operation, bool, error) {
			i, found := mops[opindex]

			return i, found, nil
		}

		nargs.SetLogging(args.Logging)

		return nargs, newStateValuesProcessor, nil
	}

	args.NewWorkerMergerFuncs = func() (
		*workerMergerArgs,
		newWorkerMergerFunc,
		error,
	) {
		return wmargs, newWorkerMerger, nil
	}

	stateOwnersf, _ := serialStateOwnersFunc(workers, 1)
	stateOwners, _ := newStateOwnersFunc(local, workers, stateOwnersf)
	args.StateOwners = stateOwners

	args.GetStateFunc = func(stateKey string) (base.State, bool, error) {
		i, found := states.Value(stateKey)

		return i, found, nil
	}
	args.DistributeStateMergeValues = func() (
		distributeStateMergeValuesFunc,
		func(context.Context) error,
		error,
	) {
		return func(context.Context, string, uint64, util.Hash, []StateMergeValue) error {
				return nil
			},
			func(context.Context) error { return nil },
			nil
	}

	args.ReportFuncs = func(context.Context) (
		workerMergerUnmergedOpsFunc,
		reportStatesFuncs,
		error,
	) {
		return func([]uint64) error { return nil },
			func() (reportStateFunc, func(), error) {
				return func(context.Context, string, util.Hash) error { return nil },
					func() {},
					nil
			}, nil
	}

	args.SaveFuncs = func() (
		saveOperationFuncs,
		saveStateFuncs,
		saveReasonsFuncs,
		error,
	) {
		return func() (
				saveOperationFunc,
				func(error) error,
				error,
			) {
				return func(uint64, Operation) error { return nil },
					func(error) error { return nil },
					nil
			},
			func() (
				saveStateFunc,
				func(error) error,
				error,
			) {
				return func(base.State) error { return nil },
					func(error) error { return nil },
					nil
			},
			func() (
				saveReasonFunc,
				func(error) error,
				error,
			) {
				return func(uint64, string) error { return nil },
					func(error) error { return nil },
					nil
			},
			nil
	}

	args.SetLogging(logging.TestNilLogging)

	return pargs, wpargs, wmargs, args, vp, states
}

func (t *testProcessWorker) TestCancel() {
	t.Run("before insert ops", func() {
		ctx, cancel := context.WithCancel(context.Background())

		ops := t.randomOps(1, 1)

		_, _, _, args, _, _ := t.newArgs("w0", []string{"w0"}, ops)

		initials, _, _, wait, err := newProcessWorker(ctx, args, 3)
		t.NoError(err)

		cancel()

		insert, _ := initials()

		err = insert(0, ops[0].Hash())
		t.ErrorIs(err, context.Canceled)

		err = wait(nil)
		t.ErrorIs(err, context.Canceled)
	})

	t.Run("after insert ops", func() {
		ctx, cancel := context.WithCancel(context.Background())

		ops := t.randomOps(1, 1)

		_, _, _, args, _, _ := t.newArgs("w0", []string{"w0"}, ops)

		initials, _, _, wait, err := newProcessWorker(ctx, args, 3)
		t.NoError(err)

		insert, insertDone := initials()

		t.NoError(insert(0, ops[0].Hash()))

		cancel()

		insertDone()
		err = wait(nil)
		t.ErrorIs(err, context.Canceled)
	})

	t.Run("after wait", func() {
		ctx, cancel := context.WithCancel(context.Background())

		ops := t.randomOps(1, 1)

		_, _, _, args, _, _ := t.newArgs("w0", []string{"w0"}, ops)

		initials, _, _, wait, err := newProcessWorker(ctx, args, 3)
		t.NoError(err)

		insert, insertDone := initials()

		t.NoError(insert(0, ops[0].Hash()))
		insertDone()
		t.NoError(wait(nil))

		cancel()

		err = wait(nil)
		t.ErrorIs(err, context.Canceled)
	})
}

func (t *testProcessWorker) TestLocalMerge() {
	prv0, prv1, prv2 := base.NewDummyPrivatekey("0"), base.NewDummyPrivatekey("1"), base.NewDummyPrivatekey("2")
	pub0, pub1, pub2 := prv0.Publickey(), prv1.Publickey(), prv2.Publickey()

	sts := []base.State{
		t.newState(pub0, 1),
		t.newState(pub1, 1),
		t.newState(pub2, 1),
	}

	ops := []Operation{
		t.newOp(prv0, pub1, 1),
		t.newOp(prv0, pub2, 1), // 1: ignored
		t.newOp(prv1, pub0, 1),
		t.newOp(prv1, pub2, 1), // 3: ignored
		t.newOp(prv2, pub0, 1),
		t.newOp(prv2, pub1, 1), // 5: ignored
	}

	/*
		            | p0 | p1 | p2 |
		       base |  1 |  1 |  1 |
		p0 -> p1, 1 |  0 |  2 |    |
		p1 -> p0, 1 |  1 |  1 |    |
		p2 -> p0, 1 |  2 |    |  0 |
	*/

	expectedBalances := map[string]int64{sts[0].Key(): 2, sts[2].Key(): 0}
	expectedUnmergedOps := []uint64{1, 3, 5}
	//expectedMergedOps := map[ /* state key */ string][]uint64{
	//	key0: {0, 2, 4},
	//	key1: {0, 2},
	//	key2: {4},
	//}
	expectedReasons := map[uint64]string{
		1: "previous issuer found",
		3: "previous issuer found",
		5: "previous issuer found",
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	_, _, _, args, _, states := t.newArgs("w0", []string{"w0"}, ops)
	args.SetLogging(logging.TestNilLogging)

	reportedStates := util.NewSingleLockedMap[string, util.Hash]()

	unmergedch := make(chan []uint64, 1)

	args.ReportFuncs = func(context.Context) (
		workerMergerUnmergedOpsFunc,
		reportStatesFuncs,
		error,
	) {
		return func(unmergedOps []uint64) error {
				unmergedch <- unmergedOps
				return nil
			},
			func() (reportStateFunc, func(), error) {
				return func(_ context.Context, stateKey string, h util.Hash) error {
						if !reportedStates.SetValue(stateKey, h) {
							return util.ErrFound.Errorf("reported state, %q", stateKey)
						}

						return nil
					},
					func() {},
					nil
			}, nil
	}

	savedOps := util.NewSingleLockedMap[uint64, Operation]()
	savedStates := util.NewSingleLockedMap[string, base.State]()
	reasons := util.NewSingleLockedMap[uint64, string]()

	args.SaveFuncs = func() (
		saveOperationFuncs,
		saveStateFuncs,
		saveReasonsFuncs,
		error,
	) {
		return func() (
				saveOperationFunc,
				func(error) error,
				error,
			) {
				return func(opindex uint64, op Operation) error {
						if !savedOps.SetValue(opindex, op) {
							return util.ErrFound.Errorf("save operation, %d", opindex)
						}

						return nil
					},

					func(error) error { return nil },
					nil
			},
			func() (
				saveStateFunc,
				func(error) error,
				error,
			) {
				return func(st base.State) error {
						if !savedStates.SetValue(st.Key(), st) {
							return util.ErrFound.Errorf("save state, %q", st.Key())
						}

						return nil
					},

					func(error) error { return nil },
					nil
			},
			func() (
				saveReasonFunc,
				func(error) error,
				error,
			) {
				return func(opindex uint64, reason string) error {
						t.True(reasons.SetValue(opindex, reason))

						return nil
					},
					func(error) error { return nil },
					nil
			},
			nil
	}

	for i := range sts {
		st := sts[i]

		states.SetValue(st.Key(), st)
	}

	initials, _, _, wait, err := newProcessWorker(ctx, args, 3)
	t.NoError(err)

	initial, initialDone := initials()

	util.TraverseSliceV1(ops, func(i int, op Operation) (bool, error) {
		return true, initial(uint64(i), op.Hash())
	})
	initialDone()

	t.NoError(wait(func(unmergedOps []uint64) error {
		slices.Sort(unmergedOps)
		t.Equal(expectedUnmergedOps, unmergedOps)

		return nil
	}))

	t.Equal(expectedReasons, reasons.Map())

	select {
	case <-time.After(time.Millisecond * 33):
		t.Fail("wait unmerged ops")
	case i := <-unmergedch:
		slices.Sort(i)

		t.Equal(expectedUnmergedOps, i)
	}

	t.Equal(len(ops), savedOps.Len())

	for i := range ops {
		s, found := savedOps.Value(uint64(i))
		t.True(found)
		base.EqualOperation(t.Assert(), ops[i], s)
	}

	t.Equal(reportedStates.Len(), savedStates.Len())

	savedStates.Traverse(func(stateKey string, st base.State) bool {
		h, found := reportedStates.Value(stateKey)
		t.True(found)
		t.True(st.Hash().Equal(h))

		b, found := expectedBalances[stateKey]
		t.True(found)
		t.Equal(b, st.Value().(AccountStateValue).Balance)

		return true
	})
}

func (t *testProcessWorker) testArgsNWorkers(
	worker string,
	workers []string,
	ops []Operation,
	sts []base.State,
	reportUnmerged workerMergerUnmergedOpsFunc,
	reportState reportStateFunc,
	saveOperation saveOperationFunc,
	saveState saveStateFunc,
	saveReason saveReasonFunc,
) *processWorkerArgs {
	_, _, _, args, _, states := t.newArgs(worker, workers, ops)
	args.SetLogging(logging.TestNilLogging)

	for i := range sts {
		st := sts[i]

		states.SetValue(st.Key(), st)
	}

	args.ReportFuncs = func(context.Context) (
		workerMergerUnmergedOpsFunc,
		reportStatesFuncs,
		error,
	) {
		return reportUnmerged,
			func() (reportStateFunc, func(), error) {
				return reportState, func() {}, nil
			}, nil
	}

	args.SaveFuncs = func() (
		saveOperationFuncs,
		saveStateFuncs,
		saveReasonsFuncs,
		error,
	) {
		return func() (saveOperationFunc, func(error) error, error) {
				return saveOperation, func(error) error { return nil }, nil
			},
			func() (saveStateFunc, func(error) error, error) {
				return saveState, func(error) error { return nil }, nil
			},
			func() (saveReasonFunc, func(error) error, error) {
				return saveReason, func(error) error { return nil }, nil
			},
			nil
	}

	return args
}

func (t *testProcessWorker) testMergeNWorkers(n uint64) func() {
	// FIXME
	/*
	   process_worker_test.go:838:
	               Error Trace:    /workspace/mitum/src/example/test364_4/process_worker_test.go:838
	                                                       /home/spikeekips/.gvm/gos/go1.21.9/src/runtime/asm_arm64.s:1197
	               Error:          Received unexpected error:
	                               > job worker: no more new job
	                               no more new job; 031b6a07-7b2c-40f0-b853-f64a8b2bb39c
	                               github.com/spikeekips/mitum/example/test364_4.NewBaseJobWorker
	                                       /workspace/mitum/src/example/test364_4/worker.go:37
	                               github.com/spikeekips/mitum/example/test364_4.newConcPhaseJobWorkerArgs.func2
	                                       /workspace/mitum/src/example/test364_4/phase_worker_conc.go:35
	                               github.com/spikeekips/mitum/example/test364_4.newConcPhaseJobWorker
	                                       /workspace/mitum/src/example/test364_4/phase_worker_conc.go:76
	                               github.com/spikeekips/mitum/example/test364_4.NewConcPhaseJobWorker
	                                       /workspace/mitum/src/example/test364_4/phase_worker_conc.go:151
	                               github.com/spikeekips/mitum/example/test364_4.NewPhaseMergerArgs.func2
	                                       /workspace/mitum/src/example/test364_4/phase_merger.go:48
	                               github.com/spikeekips/mitum/example/test364_4.NewPhaseMerger
	                                       /workspace/mitum/src/example/test364_4/phase_merger.go:110
	                               github.com/spikeekips/mitum/example/test364_4.newWorkerPhaseMerger
	                                       /workspace/mitum/src/example/test364_4/worker_phase_merger.go:82
	                               github.com/spikeekips/mitum/example/test364_4.newWorkerPhaseMergerFuncsForWorkerMerger
	                                       /workspace/mitum/src/example/test364_4/worker_merger.go:381
	                               github.com/spikeekips/mitum/example/test364_4.newWorkerMerger.func1
	                                       /workspace/mitum/src/example/test364_4/worker_merger.go:127
	                               github.com/spikeekips/mitum/example/test364_4.newWorkerMerger.func3
	                                       /workspace/mitum/src/example/test364_4/worker_merger.go:246
	                               github.com/spikeekips/mitum/example/test364_4.NewContextDoneFunc.func1.1.1
	                                       /workspace/mitum/src/example/test364_4/util.go:125
	                               runtime.goexit
	                                       /home/spikeekips/.gvm/gos/go1.21.9/src/runtime/asm_arm64.s:1197
	                               github.com/spikeekips/mitum/example/test364_4.NewBaseJobWorker.func5
	                                       /workspace/mitum/src/example/test364_4/worker.go:58
	                               github.com/spikeekips/mitum/example/test364_4.NewBaseJobWorker.func7
	                                       /workspace/mitum/src/example/test364_4/worker.go:95
	                               github.com/spikeekips/mitum/example/test364_4.(*BaseJobWorker).NewJob
	                                       /workspace/mitum/src/example/test364_4/worker.go:131
	                               github.com/spikeekips/mitum/example/test364_4.newConcPhaseJobWorker.func4
	                                       /workspace/mitum/src/example/test364_4/phase_worker_conc.go:120
	                               github.com/spikeekips/mitum/example/test364_4.NewPhaseMerger.func3
	                                       /workspace/mitum/src/example/test364_4/phase_merger.go:242
	                               github.com/spikeekips/mitum/example/test364_4.newWorkerPhaseMerger.newWorkerPhaseMergerInsertFunc.func6.1
	                                       /workspace/mitum/src/example/test364_4/worker_phase_merger.go:274
	                               github.com/spikeekips/mitum/util.TraverseSliceV1[...]
	                                       /workspace/mitum/src/util/slice.go:204
	                               github.com/spikeekips/mitum/example/test364_4.newWorkerPhaseMerger.newWorkerPhaseMergerInsertFunc.func6
	                                       /workspace/mitum/src/example/test364_4/worker_phase_merger.go:273
	                               github.com/spikeekips/mitum/example/test364_4.newWorkerPhaseMerger.func2
	                                       /workspace/mitum/src/example/test364_4/worker_phase_merger.go:101
	                               github.com/spikeekips/mitum/example/test364_4.newWorkerPhaseMerger.func4
	                                       /workspace/mitum/src/example/test364_4/worker_phase_merger.go:165
	                               github.com/spikeekips/mitum/example/test364_4.newWorkerMerger.func1.1
	                                       /workspace/mitum/src/example/test364_4/worker_merger.go:136
	                               github.com/spikeekips/mitum/example/test364_4.newWorkerMerger.func2
	                                       /workspace/mitum/src/example/test364_4/worker_merger.go:226
	                               github.com/spikeekips/mitum/example/test364_4.newWorkerMergerFuncsForProcessWorker.func3
	                                       /workspace/mitum/src/example/test364_4/process_worker.go:495
	                               github.com/spikeekips/mitum/example/test364_4.(*testProcessWorker).TestMergeNWorkers.(*testProcessWorker).testMergeNWorkers.func1.6.1
	                                       /workspace/mitum/src/example/test364_4/process_worker_test.go:649
	                               github.com/spikeekips/mitum/example/test364_4.TraverseMap[...]
	                                       /workspace/mitum/src/example/test364_4/util.go:22
	                               github.com/spikeekips/mitum/example/test364_4.(*testProcessWorker).TestMergeNWorkers.(*testProcessWorker).testMergeNWorkers.func1.6
	                                       /workspace/mitum/src/example/test364_4/process_worker_test.go:644
	                               github.com/spikeekips/mitum/example/test364_4.newWorkerMerger.func1.3.1
	                                       /workspace/mitum/src/example/test364_4/worker_merger.go:168
	                               github.com/spikeekips/mitum/example/test364_4.newWorkerPhaseMerger.func5.1
	                                       /workspace/mitum/src/example/test364_4/worker_phase_merger.go:195
	                               github.com/spikeekips/mitum/example/test364_4.NewPhaseMerger.func5.1
	                                       /workspace/mitum/src/example/test364_4/phase_merger.go:260
	                               github.com/spikeekips/mitum/example/test364_4.isXMergedOpsFuncs.func3.1
	                                       /workspace/mitum/src/example/test364_4/phase_merger.go:482
	                               github.com/spikeekips/mitum/example/test364_4.addXMergedOpsFuncs.func2.2.1
	                                       /workspace/mitum/src/example/test364_4/phase_merger.go:418
	                               github.com/spikeekips/mitum/util.CompactAppendSliceV1[...].func6
	                                       /workspace/mitum/src/util/slice.go:543
	                               github.com/spikeekips/mitum/example/test364_4.addXMergedOpsFuncs.func2.2
	                                       /workspace/mitum/src/example/test364_4/phase_merger.go:417
	                               github.com/spikeekips/mitum/util.CompactAppendSliceV1[...].func6
	                                       /workspace/mitum/src/util/slice.go:546
	                               github.com/spikeekips/mitum/example/test364_4.addXMergedOpsFuncs.func2
	                                       /workspace/mitum/src/example/test364_4/phase_merger.go:416
	                               github.com/spikeekips/mitum/example/test364_4.isXMergedOpsFuncs.func3
	                                       /workspace/mitum/src/example/test364_4/phase_merger.go:481
	                               github.com/spikeekips/mitum/example/test364_4.NewPhaseMerger.func5
	                                       /workspace/mitum/src/example/test364_4/phase_merger.go:255
	                               github.com/spikeekips/mitum/example/test364_4.newWorkerPhaseMerger.func5
	                                       /workspace/mitum/src/example/test364_4/worker_phase_merger.go:190
	                               github.com/spikeekips/mitum/example/test364_4.newWorkerMerger.func1.3
	                                       /workspace/mitum/src/example/test364_4/worker_merger.go:159
	                               runtime.goexit
	                                       /home/spikeekips/.gvm/gos/go1.21.9/src/runtime/asm_arm64.s:1197
	*/
	return func() {
		prv0, prv1, prv2 := base.NewDummyPrivatekey("0"), base.NewDummyPrivatekey("1"), base.NewDummyPrivatekey("2")
		pub0, pub1, pub2 := prv0.Publickey(), prv1.Publickey(), prv2.Publickey()

		sts := []base.State{
			t.newState(pub0, 1),
			t.newState(pub1, 1),
			t.newState(pub2, 1),
		}

		ops := []Operation{
			t.newOp(prv0, pub1, 1),
			t.newOp(prv0, pub2, 1), // 1: ignored
			t.newOp(prv1, pub0, 1),
			t.newOp(prv1, pub2, 1), // 3: ignored
			t.newOp(prv2, pub0, 1),
			t.newOp(prv2, pub1, 1), // 5: ignored
		}

		/*
			            | p0 | p1 | p2 |
			       base |  1 |  1 |  1 |
			p0 -> p1, 1 |  0 |  2 |    |
			p1 -> p0, 1 |  1 |  1 |    |
			p2 -> p0, 1 |  2 |    |  0 |
		*/

		expectedBalances := map[string]int64{sts[0].Key(): 2, sts[2].Key(): 0}
		expectedUnmergedOps := []uint64{1, 3, 5}
		//expectedMergedOps := map[ /* state key */ string][]uint64{
		//	key0: {0, 2, 4},
		//	key1: {0, 2},
		//	key2: {4},
		//}
		expectedReasons := map[uint64]string{
			1: "previous issuer found",
			3: "previous issuer found",
			5: "previous issuer found",
		}

		reportedUnmerged := util.NewSingleLockedMap[string, []uint64]()
		reportedStates := util.NewSingleLockedMap[string, []util.Hash]()
		savedOps := util.NewSingleLockedMap[string, map[uint64]Operation]()
		savedStates := util.NewSingleLockedMap[string, []base.State]()
		savedReasons := util.NewSingleLockedMap[string, map[uint64]string]()

		reportUnmerged := func(worker string) workerMergerUnmergedOpsFunc {
			return func(unmergedOps []uint64) error {
				if !reportedUnmerged.SetValue(worker, unmergedOps) {
					return util.ErrFound.Errorf("unmergedOps, worker=%q", worker)
				}

				return nil
			}
		}

		reportState := func(worker string) reportStateFunc {
			return func(_ context.Context, stateKey string, h util.Hash) error {
				_, _, _ = reportedStates.Set(worker, func(prev []util.Hash, _ bool) ([]util.Hash, error) {
					prev = append(prev, h)

					return prev, nil
				})

				return nil
			}
		}
		saveOperation := func(worker string) saveOperationFunc {
			return func(opindex uint64, op Operation) error {
				_, _, _ = savedOps.Set(worker, func(prev map[uint64]Operation, prevfound bool) (map[uint64]Operation, error) {
					switch {
					case !prevfound:
						prev = map[uint64]Operation{}
					default:
						if _, found := prev[opindex]; found {
							return nil, util.ErrFound.Errorf("saveOp, worker=%q, opindex=%d", worker, opindex)
						}
					}

					prev[opindex] = op

					return prev, nil
				})

				return nil
			}
		}
		saveState := func(worker string) saveStateFunc {
			return func(st base.State) error {
				_, _, _ = savedStates.Set(worker, func(prev []base.State, _ bool) ([]base.State, error) {
					prev = append(prev, st)

					return prev, nil
				})

				return nil
			}
		}
		saveReason := func(worker string) func(opindex uint64, reason string) error {
			return func(opindex uint64, reason string) error {
				_, _, _ = savedReasons.Set(worker, func(prev map[uint64]string, prevfound bool) (map[uint64]string, error) {
					switch {
					case !prevfound:
						prev = map[uint64]string{}
					default:
						if _, found := prev[opindex]; found {
							return nil, util.ErrFound.Errorf("saveReason, worker=%q, opindex=%d", worker, opindex)
						}
					}

					prev[opindex] = reason

					return prev, nil
				})

				return nil
			}
		}

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		nargs := map[string]*processWorkerArgs{}
		initials := map[string]processWorkerInitialOpFuncs{}
		inserts := map[string]insertWorkerOpFuncs{}
		workerPhaseDones := map[string]workerMergerPhaseDoneFunc{}
		waits := map[string]workerMergerWaitFunc{}

		workerNames := make([]string, n)

		stateOwnersf, _ := serialStateOwnersFunc(workerNames, 1)

		for i := range workerNames {
			local := fmt.Sprintf("w%d", i)
			workerNames[i] = local

			args := t.testArgsNWorkers(
				local,
				workerNames,
				ops,
				sts,
				reportUnmerged(local),
				reportState(local),
				saveOperation(local),
				saveState(local),
				saveReason(local),
			)
			args.local = local
			// args.SetLogging(logging.TestLogging)

			if local == "w0" {
				args.SetLogging(logging.TestLogging)
			}

			stateOwners, _ := newStateOwnersFunc(local, workerNames, stateOwnersf)
			args.StateOwners = stateOwners

			args.BroadcastPhaseDone = func(ctx context.Context, phase int, mergedOps, unmergedOps []uint64) error {
				t.T().Logf(
					"broadcast phase done: local=%q phase=%d merged=%v unmerged=%v",
					local,
					phase,
					mergedOps,
					unmergedOps,
				)

				return TraverseMap(workerPhaseDones, func(worker string, f workerMergerPhaseDoneFunc) (bool, error) {
					if worker == local {
						return true, nil
					}

					return true, f(phase, local, mergedOps, unmergedOps)
				})
			}

			args.DistributeStateMergeValues = func() (
				distributeStateMergeValuesFunc,
				func(context.Context) error,
				error,
			) {
				ds := util.NewSingleLockedMap[string, insertOpFunc]()
				donech := make(chan struct{})

				var once sync.Once

				return func(ctx context.Context, worker string, opindex uint64, oph util.Hash, values []StateMergeValue) error {
						var statekeys []string

						util.TraverseSliceV1(values, func(_ int, value StateMergeValue) (bool, error) {
							statekeys = append(statekeys, value.Key())

							return true, nil
						})

						t.T().Logf("worker insert value: local=%q worker=%q opindex=%d state_keys=%v", local, worker, opindex, statekeys)

						once.Do(func() {
							util.TraverseSliceV1(workerNames, func(_ int, worker string) (bool, error) {
								if worker == local {
									return true, nil
								}

								i, d := inserts[worker](local)

								go func() {
									select {
									case <-ctx.Done():
									case <-donech:
									}

									d()
								}()

								ds.SetValue(worker, i)

								return true, nil
							})
						})

						switch f, found := ds.Value(worker); {
						case !found:
							return util.ErrNotFound.Errorf("distributeStateMergeValuesFunc, %q", worker)
						default:
							return f(opindex, oph, values)
						}
					},
					func(context.Context) error {
						close(donech)

						return nil
					},
					nil
			}

			args.ReportFuncs = func(context.Context) (
				workerMergerUnmergedOpsFunc,
				reportStatesFuncs,
				error,
			) {
				return func(unmergedOps []uint64) error {
						t.T().Logf("report unmerged ops: worker=%q unmerged ops=%v", local, unmergedOps)

						return nil
					},
					func() (reportStateFunc, func(), error) {
						return func(_ context.Context, stateKey string, h util.Hash) error {
								return nil
							},
							func() {},
							nil
					}, nil
			}

			args.SaveFuncs = func() (
				saveOperationFuncs,
				saveStateFuncs,
				saveReasonsFuncs,
				error,
			) {
				return func() (
						saveOperationFunc,
						func(error) error,
						error,
					) {
						return func(opindex uint64, op Operation) error {
								return nil
							},

							func(error) error { return nil },
							nil
					},
					func() (
						saveStateFunc,
						func(error) error,
						error,
					) {
						return func(st base.State) error {
								return nil
							},

							func(error) error { return nil },
							nil
					},
					func() (
						saveReasonFunc,
						func(error) error,
						error,
					) {
						return func(opindex uint64, reason string) error {
								t.T().Logf("save reasons: worker=%q opindex=%d reason=%q", local, opindex, reason)

								return nil
							},
							func(error) error { return nil },
							nil
					},
					nil
			}

			nargs[local] = args

			wctx, wctxCancel := context.WithCancel(ctx)
			defer wctxCancel()

			initial, insert, workerPhaseDone, wait, err := newProcessWorker(wctx, args, 3)
			t.NoError(err)

			initials[local] = initial
			inserts[local] = insert
			workerPhaseDones[local] = workerPhaseDone
			waits[local] = wait
		}

		workerOphs := map[string]map[uint64]util.Hash{}
		workerOps := map[string][]uint64{}

		for i := range ops {
			worker := workerNames[i%len(workerNames)]

			if _, found := workerOphs[worker]; !found {
				workerOphs[worker] = map[uint64]util.Hash{}
			}

			opindex := uint64(i)

			workerOphs[worker][opindex] = ops[i].Hash()
			workerOps[worker] = append(workerOps[worker], opindex)
		}

		t.T().Log("initials")
		t.NoError(util.TraverseSliceV1(workerNames, func(_ int, worker string) (bool, error) {
			insert, done := initials[worker]()

			defer done()

			t.T().Logf("worker initial: worker=%q opindex=%d", worker, workerOps[worker])

			return true, TraverseMap(workerOphs[worker], func(opindex uint64, oph util.Hash) (bool, error) {
				return true, insert(opindex, oph)
			})
		}))

		var waitwg sync.WaitGroup

		for i := range waits {
			waitwg.Add(1)

			worker := i

			go func() {
				defer waitwg.Done()

				t.T().Logf("wait: worker=%q", worker)

				wait := waits[worker]
				err := wait(func(unmergedOps []uint64) error {
					t.T().Logf("wait callback: worker=%q unmerged ops=%v", worker, unmergedOps)

					return nil
				})
				t.NoError(err, "worker=%q %+v", worker, err)
			}()
		}

		waitwg.Wait()
		t.T().Log("wait done")

		//
		// 	initials, _, _, wait, err := newProcessWorker(ctx, args, 3)
		// 	t.NoError(err)
		//
		// 	initial, initialDone := initials()
		//
		// 	util.TraverseSliceV1(ops, func(i int, op Operation) (bool, error) {
		// 		return true, initial(uint64(i), op.Hash())
		// 	})
		// 	initialDone()
		//
		// 	t.NoError(wait(func(unmergedOps []uint64) error {
		// 		slices.Sort(unmergedOps)
		// 		t.Equal(expectedUnmergedOps, unmergedOps)
		//
		// 		return nil
		// 	}))
		//
		// 	t.Equal(expectedReasons, reasons.Map())
		//
		// 	select {
		// 	case <-time.After(time.Millisecond * 33):
		// 		t.Fail("wait unmerged ops")
		// 	case i := <-unmergedch:
		// 		slices.Sort(i)
		//
		// 		t.Equal(expectedUnmergedOps, i)
		// 	}
		//
		// 	t.Equal(len(ops), savedOps.Len())
		//
		// 	for i := range ops {
		// 		s, found := savedOps.Value(uint64(i))
		// 		t.True(found)
		// 		base.EqualOperation(t.Assert(), ops[i], s)
		// 	}
		//
		// 	t.Equal(reportedStates.Len(), savedStates.Len())
		//
		// 	savedStates.Traverse(func(stateKey string, st base.State) bool {
		// 		h, found := reportedStates.Value(stateKey)
		// 		t.True(found)
		// 		t.True(st.Hash().Equal(h))
		//
		// 		b, found := expectedBalances[stateKey]
		// 		t.True(found)
		// 		t.Equal(b, st.Value().(AccountStateValue).Balance)
		//
		// 		return true
		// 	})

		_ = ctx
		_ = expectedBalances
		_ = expectedUnmergedOps
		_ = expectedReasons
	}
}

func (t *testProcessWorker) TestMergeNWorkers() {
	// t.Run("1", t.testMergeNWorkers(1))
	t.Run("4", t.testMergeNWorkers(4))
}

// FIXME test bigger size workers than ops

func TestProcessWorker(t *testing.T) {
	t.Skip()

	defer goleak.VerifyNone(t)

	suite.Run(t, new(testProcessWorker))
}
