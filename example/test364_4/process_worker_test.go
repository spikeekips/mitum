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
	args.UpdateReasons = func(context.Context, int, map[uint64]string) error {
		return nil
	}
	args.SaveOperation = func(uint64, Operation) error {
		return nil
	}

	args.ReportResult = func(context.Context) (
		workerMergerUnmergedOpsFunc,
		reportResultStatesFuncs,
		error,
	) {
		return func([]uint64) error { return nil },
			func() (
				func(context.Context, string, base.State) error,
				func(),
				error,
			) {
				return func(context.Context, string, base.State) error { return nil },
					func() {},
					nil
			}, nil
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

	reasonch := make(chan [2]interface{}, 1)
	args.UpdateReasons = func(_ context.Context, phase int, reasons map[uint64]string) error {
		reasonch <- [2]interface{}{phase, reasons}

		return nil
	}

	unmergedch := make(chan []uint64, 1)
	merged := util.NewSingleLockedMap[string, int64]()

	args.ReportResult = func(context.Context) (
		workerMergerUnmergedOpsFunc,
		reportResultStatesFuncs,
		error,
	) {
		return func(unmergedOps []uint64) error {
				unmergedch <- unmergedOps
				return nil
			},
			func() (
				func(context.Context, string, base.State) error,
				func(),
				error,
			) {
				return func(_ context.Context, stateKey string, st base.State) error {
						merged.SetValue(stateKey, st.Value().(AccountStateValue).Balance)

						return nil
					},
					func() {},
					nil
			}, nil
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

	select {
	case <-time.After(time.Millisecond * 33):
		t.Fail("wait updated reasons")
	case i := <-reasonch:
		phase := i[0].(int)
		reasons := i[1].(map[uint64]string)

		t.Equal(-1, phase)
		t.Equal(expectedReasons, reasons)
	}

	select {
	case <-time.After(time.Millisecond * 33):
		t.Fail("wait unmerged ops")
	case i := <-unmergedch:
		slices.Sort(i)

		t.Equal(expectedUnmergedOps, i)
	}

	t.Equal(
		map[string]int64{sts[0].Key(): 2, sts[2].Key(): 0},
		merged.Map(),
	)
}

func TestProcessWorker(t *testing.T) {
	defer goleak.VerifyNone(t)

	suite.Run(t, new(testProcessWorker))
}
