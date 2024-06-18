package main

import (
	"context"
	"slices"
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
	args.DistributeStateMergeValues = func(context.Context, string, uint64, util.Hash, []StateMergeValue) error {
		return nil
	}

	args.ReportFuncs = func(context.Context) (
		workerMergerUnmergedOpsFunc,
		reportStatesFuncs,
		error,
	) {
		return func([]uint64) error { return nil },
			func() (
				func(context.Context, string, util.Hash) error,
				func(),
				error,
			) {
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
			func() (
				func(context.Context, string, util.Hash) error,
				func(),
				error,
			) {
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

func TestProcessWorker(t *testing.T) {
	defer goleak.VerifyNone(t)

	suite.Run(t, new(testProcessWorker))
}
