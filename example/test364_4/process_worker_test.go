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
	ops map[uint64]Operation,
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

	vp, mergers, states, _, getValues := t.baseTestMergeOp.getFuncs()

	pargs := NewPhaseMergerArgs()
	wpargs := newWorkerPhaseMergerArgs()
	wmargs := newWorkerMergerArgs()
	args := newProcessWorkerArgs()

	args.NewStateValuesProcessorFuncs = func() (
		*stateValuesProcessorArgs,
		newStateValuesProcessorFunc,
		error,
	) {
		nargs := newStateValuesProcessorArgs()
		nargs.SetLogging(args.Logging)

		nargs.GetOperation = func(_ context.Context, opindex uint64) (Operation, bool, error) {
			i, found := ops[opindex]

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
		getState := func(stateKey string) (base.State, bool, error) {
			i, found := states.Value(stateKey)

			return i, found, nil
		}

		pargs.GetMergersFunc, _ = newGetMergersFunc(mergers, getState)
		pargs.GetValuesFunc = getValues
		extractKeys, _ := newExtractStateMergeValueKeysFunc(1 << 9)
		pargs.ExtractStateMergeValueKeysFunc = extractKeys

		wpargs.NewPhaseMergerFuncs = func() (
			*PhaseMergerArgs,
			newPhaseMergerFunc,
			error,
		) {
			return pargs, NewPhaseMerger, nil
		}
		wpargs.EnoughWorkers = uint64(len(workers))
		wpargs.IsLocalOp = func(uint64) bool { return true }

		wmargs.NewWorkerPhaseMergerFuncs = func(ctx context.Context, _ int, args *workerMergerArgs) (
			workerPhaseMergerInsertFunc,
			workerPhaseMergerWaitFunc,
			error,
		) {
			return newWorkerPhaseMerger(ctx, wpargs)
		}

		wmargs.BroadcastPhaseInsertDone = func(context.Context, int, []uint64, []uint64) error {
			return nil
		}
		wmargs.UpdateReasons = func(context.Context, int, map[uint64]string) error {
			return nil
		}

		pargs.SetLogging(args.Logging)
		wpargs.SetLogging(args.Logging)
		wmargs.SetLogging(args.Logging)

		return wmargs, newWorkerMerger, nil
	}

	stateOwnersf, _ := serialStateOwnersFunc(workers, 1)
	stateOwners, _ := newStateOwnersFunc(local, workers, stateOwnersf)
	args.StateOwners = stateOwners

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

func (t *testProcessWorker) randomOps(n int, amount int64) map[uint64]Operation {
	m := map[uint64]Operation{}

	for i := 0; i < n; i++ {
		m[uint64(i)] = t.randomOp(amount)
	}

	return m
}

func (t *testProcessWorker) TestCancel() {
	t.Run("before insert ops", func() {
		ctx, cancel := context.WithCancel(context.Background())

		ops := t.randomOps(1, 1)

		_, _, _, args, _, _, _ := t.newArgs("w0", []string{"w0"}, ops)

		insertOps, _, wait, err := newProcessWorker(ctx, args)
		t.NoError(err)

		cancel()

		err = insertOps([]uint64{0})
		t.ErrorIs(err, context.Canceled)

		err = wait()
		t.ErrorIs(err, context.Canceled)
	})

	t.Run("after insert ops", func() {
		ctx, cancel := context.WithCancel(context.Background())

		ops := t.randomOps(1, 1)

		_, _, _, args, _, _, _ := t.newArgs("w0", []string{"w0"}, ops)

		insertOps, _, wait, err := newProcessWorker(ctx, args)
		t.NoError(err)

		t.NoError(insertOps([]uint64{0}))

		cancel()

		err = wait()
		t.ErrorIs(err, context.Canceled)
	})

	t.Run("after wait", func() {
		ctx, cancel := context.WithCancel(context.Background())

		ops := t.randomOps(1, 1)

		_, _, _, args, _, _, _ := t.newArgs("w0", []string{"w0"}, ops)
		args.SetLogging(logging.TestLogging)

		insertOps, _, wait, err := newProcessWorker(ctx, args)
		t.NoError(err)

		t.NoError(insertOps([]uint64{0}))
		t.NoError(wait())

		cancel()
		t.NoError(wait())
	})
}

func TestProcessWorker(t *testing.T) {
	defer goleak.VerifyNone(t)

	suite.Run(t, new(testProcessWorker))
}
