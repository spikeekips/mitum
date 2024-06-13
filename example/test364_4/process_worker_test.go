package main

import (
	"context"
	"testing"

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
	mergers util.LockedMap[string, StateMerger],
	states util.LockedMap[string, base.State],
) {
	if len(workers) < 1 {
		panic("empty workers")
	}

	mops := map[uint64]Operation{}

	for i := range ops {
		mops[uint64(i)] = ops[i]
	}

	vp, mergers, states, _, _ = t.baseTestMergeOp.getFuncs()

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

		nargs.GetOperation = func(_ context.Context, opindex uint64) (Operation, bool, error) {
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
		pargs.SetLogging(args.Logging)
		wpargs.SetLogging(args.Logging)
		wmargs.SetLogging(args.Logging)

		return wmargs, newWorkerMerger, nil
	}

	stateOwnersf, _ := serialStateOwnersFunc(workers, 1)
	stateOwners, _ := newStateOwnersFunc(local, workers, stateOwnersf)
	args.StateOwners = stateOwners

	args.GetStateFunc = func(string) (base.State, bool, error) {
		return nil, false, nil
	}
	args.DistributeStateMergeValues = func(context.Context, string, uint64, []StateMergeValue) error {
		return nil
	}
	args.UpdateReasons = func(context.Context, int, map[uint64]string) error {
		return nil
	}
	args.SaveOperation = func(uint64, Operation) error {
		return nil
	}

	args.SetLogging(logging.TestNilLogging)

	return pargs, wpargs, wmargs, args, vp, mergers, states
}

func (t *testProcessWorker) TestCancel() {
	t.Run("before insert ops", func() {
		ctx, cancel := context.WithCancel(context.Background())

		ops := t.randomOps(1, 1)
		values := t.processOps(ops)

		_, _, _, args, _, _, _ := t.newArgs("w0", []string{"w0"}, ops)

		insert, _, wait, err := newProcessWorker(ctx, args, []uint64{0})
		t.NoError(err)

		cancel()

		err = insert("w0", 0, values[0])
		t.ErrorIs(err, context.Canceled)

		err = wait()
		t.ErrorIs(err, context.Canceled)
	})

	t.Run("after insert ops", func() {
		ctx, cancel := context.WithCancel(context.Background())

		ops := t.randomOps(1, 1)
		values := t.processOps(ops)

		_, _, _, args, _, _, _ := t.newArgs("w0", []string{"w0"}, ops)
		args.SetLogging(logging.TestLogging)

		insert, _, wait, err := newProcessWorker(ctx, args, []uint64{0})
		t.NoError(err)

		t.NoError(insert("w0", 0, values[0]))

		cancel()

		err = wait()
		t.ErrorIs(err, context.Canceled)
	})

	t.Run("after wait", func() {
		ctx, cancel := context.WithCancel(context.Background())

		ops := t.randomOps(1, 1)
		values := t.processOps(ops)

		_, _, _, args, _, _, _ := t.newArgs("w0", []string{"w0"}, ops)
		args.SetLogging(logging.TestLogging)

		insert, _, wait, err := newProcessWorker(ctx, args, []uint64{0})
		t.NoError(err)

		t.NoError(insert(localWorkerName, 0, values[0]))
		t.NoError(wait())

		cancel()

		err = wait()
		t.ErrorIs(err, context.Canceled)
	})
}

func TestProcessWorker(t *testing.T) {
	defer goleak.VerifyNone(t)

	suite.Run(t, new(testProcessWorker))
}
