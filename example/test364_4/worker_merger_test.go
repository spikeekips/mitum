package main

import (
	"context"
	"testing"
	"time"

	"github.com/spikeekips/mitum/base"
	"github.com/spikeekips/mitum/util"
	"github.com/spikeekips/mitum/util/logging"
	"github.com/stretchr/testify/suite"
	"go.uber.org/goleak"
)

type testWorkerMerger struct {
	baseTestMergeOp
}

func (t *testWorkerMerger) newArgs(workers uint64) (
	_ *PhaseMergerArgs,
	_ *workerPhaseMergerArgs,
	_ *workerMergerArgs,
	vp util.LockedMap[uint64, []StateMergeValue],
	mergers util.LockedMap[string, StateMerger],
	states util.LockedMap[string, base.State],
) {
	vp, mergers, states, _, getValues := t.baseTestMergeOp.getFuncs()

	getState := func(stateKey string) (base.State, bool, error) {
		i, found := states.Value(stateKey)

		return i, found, nil
	}

	pargs := NewPhaseMergerArgs()
	pargs.GetMergersFunc, _ = newGetMergersFunc(mergers, getState)
	pargs.GetValuesFunc = getValues
	extractKeys, _ := newExtractStateMergeValueKeysFunc(1 << 9)
	pargs.ExtractStateMergeValueKeysFunc = extractKeys

	wpargs := newWorkerPhaseMergerArgs()
	wpargs.NewPhaseMergerFuncs = func() (
		*PhaseMergerArgs,
		newPhaseMergerFunc,
		error,
	) {
		return pargs, NewPhaseMerger, nil
	}
	wpargs.EnoughWorkers = workers
	wpargs.IsLocalOp = func(uint64) bool { return true }

	args := newWorkerMergerArgs()

	args.NewWorkerPhaseMergerFuncs = func(phase int) (
		*workerPhaseMergerArgs,
		newWorkerPhaseMergerFunc,
		error,
	) {
		return wpargs, newWorkerPhaseMerger, nil
	}

	args.BroadcastPhaseDone = func(context.Context, int, []uint64, []uint64) error {
		return nil
	}
	args.UpdateReasons = func(context.Context, int, map[uint64]string) error {
		return nil
	}

	args.SetLogging(logging.TestNilLogging)

	return pargs, wpargs, args, vp, mergers, states
}

func (t *testWorkerMerger) TestCancel() {
	t.Run("before insert", func() {
		ctx, cancel := context.WithCancel(context.Background())

		_, _, args, _, _, _ := t.newArgs(1)

		insert, wait, err := newWorkerMerger(ctx, args)
		t.NoError(err)

		cancel()

		err = insert(0, localWorkerName, nil, nil)
		t.ErrorIs(err, context.Canceled)
		err = wait()
		t.ErrorIs(err, context.Canceled)
	})

	t.Run("after insert", func() {
		ctx, cancel := context.WithCancel(context.Background())

		_, _, args, _, _, _ := t.newArgs(3)

		insert, wait, err := newWorkerMerger(ctx, args)
		t.NoError(err)

		t.NoError(insert(0, localWorkerName, nil, nil))

		cancel()

		err = wait()
		t.ErrorIs(err, context.Canceled)
	})

	t.Run("after 0 phase", func() {
		ctx, cancel := context.WithCancel(context.Background())

		_, _, args, vp, _, _ := t.newArgs(3)

		nextPhaseCh := make(chan int, 1)

		newWorkerPhaseMergerFuncs := args.NewWorkerPhaseMergerFuncs
		args.NewWorkerPhaseMergerFuncs = func(phase int) (
			*workerPhaseMergerArgs,
			newWorkerPhaseMergerFunc,
			error,
		) {
			if phase == 1 {
				go func() {
					t.T().Log("new phase:", phase)

					nextPhaseCh <- phase
				}()
			}

			return newWorkerPhaseMergerFuncs(phase)
		}

		args.SetLogging(logging.TestLogging)

		insert, wait, err := newWorkerMerger(ctx, args)
		t.NoError(err)

		ops := make([]uint64, 9)
		for i := range ops {
			opindex := uint64(i)

			_ = t.randomMergeValuesVP(vp, opindex, 1)
			ops[i] = opindex
		}

		t.NoError(insert(0, localWorkerName, ops[:3], nil))
		t.NoError(insert(0, "w0", ops[3:6], nil))
		t.NoError(insert(0, "w1", ops[6:], nil))

		select {
		case <-time.After(time.Second * 4):
			t.Fail("wait new next phase")
		case <-nextPhaseCh:
			cancel()

			t.T().Log("canceled")
		}

		err = wait()
		t.ErrorIs(err, context.Canceled)
		err = wait()
		t.ErrorIs(err, context.Canceled)
	})
}

func (t *testWorkerMerger) TestPhaseDone() {
	t.Run("0 EnoughWorkers", func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		_, _, args, _, _, _ := t.newArgs(0)

		_, _, err := newWorkerMerger(ctx, args)
		t.ErrorIs(err, util.ErrInvalid, err)
	})

	t.Run("1 EnoughWorkers", func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		_, _, args, vp, _, _ := t.newArgs(1)

		insert, wait, err := newWorkerMerger(ctx, args)
		t.NoError(err)

		ops := make([]uint64, 3)
		for i := range ops {
			opindex := uint64(i)

			_ = t.randomMergeValuesVP(vp, opindex, 1)
			ops[i] = opindex
		}

		t.NoError(insert(0, localWorkerName, ops, nil))
		t.NoError(wait())
	})

	t.Run("over 1 EnoughWorkers", func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		_, _, args, vp, _, _ := t.newArgs(2)

		other := "w0"

		var insert workerMergerPhaseDoneFunc
		var wait func() error

		nextphasech := make(chan int, 1)
		args.BroadcastPhaseDone = func(_ context.Context, phase int, _, _ []uint64) error {
			t.NoError(insert(phase, other, nil, nil))

			nextphasech <- phase

			return nil
		}

		var err error

		insert, wait, err = newWorkerMerger(ctx, args)
		t.NoError(err)

		ops := make([]uint64, 6)
		for i := range ops {
			opindex := uint64(i)

			_ = t.randomMergeValuesVP(vp, opindex, 1)
			ops[i] = opindex
		}

		t.NoError(insert(0, localWorkerName, ops[:3], nil))
		t.NoError(insert(0, other, ops[3:], nil))

		select {
		case <-time.After(time.Second):
			t.Fail("wait to enter next phase")
		case phase := <-nextphasech:
			t.Equal(1, phase)
		}

		t.NoError(wait())
	})
}

func (t *testWorkerMerger) TestUpdateReasons() {
	t.Run("no reasons", func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		_, _, args, vp, _, _ := t.newArgs(1)

		var insert workerMergerPhaseDoneFunc
		var wait func() error

		reasonsch := make(chan [2]interface{}, 1)
		args.UpdateReasons = func(_ context.Context, phase int, reasons map[uint64]string) error {
			reasonsch <- [2]interface{}{phase, reasons}

			return nil
		}

		var err error

		insert, wait, err = newWorkerMerger(ctx, args)
		t.NoError(err)

		ops := make([]uint64, 3)
		for i := range ops {
			opindex := uint64(i)

			_ = t.randomMergeValuesVP(vp, opindex, 1)
			ops[i] = opindex
		}

		t.NoError(insert(0, localWorkerName, ops, nil))

		select {
		case <-time.After(time.Second):
		case i := <-reasonsch:
			phase := i[0].(int)
			reasons := i[1].(map[uint64]string)

			t.Fail("unexpected reasons", "phase=%d reasons=%v", phase, reasons)
		}

		t.NoError(wait())
	})

	t.Run("reasons", func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		_, _, args, vp, _, _ := t.newArgs(1)

		var insert workerMergerPhaseDoneFunc
		var wait func() error

		reasonsch := make(chan [2]interface{}, 1)
		args.UpdateReasons = func(_ context.Context, phase int, reasons map[uint64]string) error {
			reasonsch <- [2]interface{}{phase, reasons}

			return nil
		}

		var err error

		insert, wait, err = newWorkerMerger(ctx, args)
		t.NoError(err)

		ops := make([]uint64, 3)
		for i := range ops {
			opindex := uint64(i)

			_ = t.randomMergeValuesVP(vp, opindex, -1)
			ops[i] = opindex
		}

		t.NoError(insert(0, localWorkerName, ops, nil))

		select {
		case <-time.After(time.Second):
			t.Fail("wait reasons")
		case i := <-reasonsch:
			phase := i[0].(int)
			reasons := i[1].(map[uint64]string)

			t.Equal(0, phase)
			t.Equal(
				map[uint64]string{0: "insufficient balance", 1: "insufficient balance", 2: "insufficient balance"},
				reasons,
			)
		}

		t.NoError(wait())
	})
}

func TestWorkerMerger(t *testing.T) {
	defer goleak.VerifyNone(t)

	suite.Run(t, new(testWorkerMerger))
}
