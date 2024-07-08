package main

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/spikeekips/mitum/base"
	"github.com/spikeekips/mitum/util"
	"github.com/spikeekips/mitum/util/logging"
	"github.com/stretchr/testify/suite"
	"go.uber.org/goleak"
)

type testWorkerPhaseMerger struct {
	baseTestMergeOp
}

func (t *testWorkerPhaseMerger) newArgs() (
	_ *PhaseMergerArgs,
	_ *workerPhaseMergerArgs,
	vp util.LockedMap[uint64, []StateMergeValue],
	mergers util.LockedMap[string, StateMerger],
	states util.LockedMap[string, base.State],
) {
	t.T().Helper()

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

	args := newWorkerPhaseMergerArgs()
	args.NewPhaseMergerFuncs = func() (
		*PhaseMergerArgs,
		newPhaseMergerFunc,
		error,
	) {
		return pargs, NewPhaseMerger, nil
	}

	args.EnoughWorkers = 3
	args.FilterMergeOps = func(m, u []uint64) ([]uint64, []uint64) { return m, u }

	args.SetLogging(logging.TestNilLogging)

	return pargs, args, vp, mergers, states
}

func (t *testWorkerPhaseMerger) newWorker(
	ctx context.Context,
	args *workerPhaseMergerArgs,
) (
	func(worker string, mergeOps, unmergeOps []uint64) error,
	workerPhaseMergerWaitFunc,
	error,
) {
	t.T().Helper()

	return newWorkerPhaseMerger(ctx, args)
}

func (t *testWorkerPhaseMerger) TestCancel() {
	t.Run("before insert", func() {
		ctx, cancel := context.WithCancel(context.Background())

		_, args, vp, _, _ := t.newArgs()
		insert, wait, err := newWorkerPhaseMerger(ctx, args)
		t.NoError(err)

		cancel()

		var opindex uint64 = 1
		_ = t.randomMergeValuesVP(vp, opindex, 1)

		err = insert(localWorkerName, []uint64{opindex}, nil)
		t.ErrorIs(err, context.Canceled)

		err = wait(nil)
		t.ErrorIs(err, context.Canceled)
	})

	t.Run("just after insert", func() {
		ctx, cancel := context.WithCancel(context.Background())

		_, args, vp, _, _ := t.newArgs()
		insert, wait, err := newWorkerPhaseMerger(ctx, args)
		t.NoError(err)

		var opindex uint64 = 1
		_ = t.randomMergeValuesVP(vp, opindex, 1)

		t.NoError(insert(localWorkerName, []uint64{opindex}, nil))

		cancel()

		err = wait(nil)
		t.ErrorIs(err, context.Canceled)
	})

	t.Run("just after insert", func() {
		ctx, cancel := context.WithCancel(context.Background())

		_, args, vp, _, _ := t.newArgs()
		insert, wait, err := newWorkerPhaseMerger(ctx, args)
		t.NoError(err)

		var opindex uint64 = 1
		_ = t.randomMergeValuesVP(vp, opindex, 1)

		t.NoError(insert(localWorkerName, []uint64{opindex}, nil))

		cancel()

		err = wait(nil)
		t.ErrorIs(err, context.Canceled)
	})
}

func (t *testWorkerPhaseMerger) TestWait() {
	t.Run("empty ops", func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		_, args, _, _, _ := t.newArgs()
		args.EnoughWorkers = 1

		insert, wait, err := newWorkerPhaseMerger(ctx, args)
		t.NoError(err)

		t.NoError(insert(localWorkerName, nil, nil))

		t.NoError(wait(func(newMerged, newUnmerged []uint64, reasons map[uint64]string, isDone bool) error {
			t.Empty(newMerged)
			t.Empty(newUnmerged)
			t.Empty(reasons)
			t.True(isDone)

			return nil
		}))
	})

	t.Run("empty ops; wait before insert", func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		_, args, _, _, _ := t.newArgs()
		args.EnoughWorkers = 1

		insert, wait, err := newWorkerPhaseMerger(ctx, args)
		t.NoError(err)

		done := NewFuncContextDoneFunc(
			ctx,
			func(context.Context) error {
				return wait(nil)
			},
		)

		go done()

		<-time.After(time.Second)

		t.NoError(insert(localWorkerName, nil, nil))

		t.NoError(done())
	})

	t.Run("empty ops; insert after wait", func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		_, args, _, _, _ := t.newArgs()
		args.EnoughWorkers = 1

		insert, wait, err := newWorkerPhaseMerger(ctx, args)
		t.NoError(err)

		t.NoError(insert(localWorkerName, nil, nil))

		t.NoError(wait(func(newMerged, newUnmerged []uint64, reasons map[uint64]string, isDone bool) error {
			t.Empty(newMerged)
			t.Empty(newUnmerged)
			t.Empty(reasons)
			t.True(isDone)

			return nil
		}))

		err = insert("w0", nil, nil)
		t.ErrorIs(err, context.Canceled)
	})

	t.Run("ok", func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		_, args, vp, _, _ := t.newArgs()
		args.EnoughWorkers = 1

		insert, wait, err := newWorkerPhaseMerger(ctx, args)
		t.NoError(err)

		var opindex uint64 = 1
		_ = t.randomMergeValuesVP(vp, opindex, 1)

		t.NoError(insert(localWorkerName, []uint64{opindex}, nil))

		t.NoError(wait(func(newMerged, newUnmerged []uint64, reasons map[uint64]string, isDone bool) error {
			t.Empty(newMerged)
			t.Empty(newUnmerged)
			t.Empty(reasons)
			t.False(isDone)

			return nil
		}))
	})

	t.Run("wait before insert", func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		_, args, vp, _, _ := t.newArgs()
		args.EnoughWorkers = 1

		waitch := make(chan struct{}, 1)

		newPhaseMergerFuncs := args.NewPhaseMergerFuncs
		args.NewPhaseMergerFuncs = func() (
			*PhaseMergerArgs,
			newPhaseMergerFunc,
			error,
		) {
			args, newf, err := newPhaseMergerFuncs()
			if err != nil {
				return nil, nil, err
			}

			return args, func(ctx context.Context, args *PhaseMergerArgs) (
				phaseMergerXMergeFunc,
				phaseMergerXMergeFunc,
				func(),
				phaseMergerWaitFunc,
				error,
			) {
				m, u, d, w, err := newf(ctx, args)
				if err != nil {
					return nil, nil, nil, nil, err
				}

				return m, u, d, func(f phaseMergerWaitCallbackFunc) error {
					close(waitch)

					return w(f)
				}, nil
			}, nil
		}

		insert, wait, err := newWorkerPhaseMerger(ctx, args)
		t.NoError(err)

		done := NewFuncContextDoneFunc(
			ctx,
			func(context.Context) error {
				return wait(nil)
			},
		)

		go done()

		<-time.After(time.Second)

		var opindex uint64 = 1
		_ = t.randomMergeValuesVP(vp, opindex, 1)

		t.NoError(insert(localWorkerName, []uint64{opindex}, nil))

		t.NoError(done())

		select {
		case <-time.After(time.Millisecond * 3):
			t.Fail("phase merger wait not called")
		case <-waitch:
		}
	})

	t.Run("insert after wait", func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		_, args, vp, _, _ := t.newArgs()
		args.EnoughWorkers = 1

		insert, wait, err := newWorkerPhaseMerger(ctx, args)
		t.NoError(err)

		var opindex uint64 = 1
		_ = t.randomMergeValuesVP(vp, opindex, 1)

		t.NoError(insert(localWorkerName, []uint64{opindex}, nil))
		t.NoError(wait(func(newMerged, newUnmerged []uint64, reasons map[uint64]string, isDone bool) error {
			t.Empty(newMerged)
			t.Empty(newUnmerged)
			t.Empty(reasons)
			t.False(isDone)

			return nil
		}))

		opindex = 2
		_ = t.randomMergeValuesVP(vp, opindex, 1)

		err = insert("w0", []uint64{opindex}, nil)
		t.ErrorIs(err, context.Canceled)
	})
}

func (t *testWorkerPhaseMerger) TestFilterOps() {
	t.Run("local", func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		_, args, vp, mergers, _ := t.newArgs()
		args.EnoughWorkers = 2

		var opindex uint64 = 1
		_ = t.randomMergeValuesVP(vp, opindex, 1)

		args.FilterMergeOps = func(m, u []uint64) ([]uint64, []uint64) {
			return util.FilterSlice(m, func(op uint64) bool { return op == opindex }),
				util.FilterSlice(u, func(op uint64) bool { return op == opindex })
		}

		insert, wait, err := newWorkerPhaseMerger(ctx, args)
		t.NoError(err)

		t.NoError(insert(localWorkerName, nil, nil))
		t.NoError(insert("w0", []uint64{opindex}, nil))

		t.NoError(wait(func(newMerged, newUnmerged []uint64, reasons map[uint64]string, isDone bool) error {
			t.Empty(newMerged)
			t.Empty(newUnmerged)
			t.Empty(reasons)
			t.False(isDone)

			return nil
		}))

		merged, err := t.closeMergers(mergers)
		t.NoError(err)
		t.Equal(1, len(merged))
	})

	t.Run("not local", func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		_, args, vp, mergers, _ := t.newArgs()
		args.EnoughWorkers = 1

		var opindex uint64 = 1
		_ = t.randomMergeValuesVP(vp, opindex, 1)

		args.FilterMergeOps = func([]uint64, []uint64) ([]uint64, []uint64) { return nil, nil }

		insert, wait, err := newWorkerPhaseMerger(ctx, args)
		t.NoError(err)

		t.NoError(insert(localWorkerName, []uint64{opindex}, nil))

		t.NoError(wait(func(newMerged, newUnmerged []uint64, reasons map[uint64]string, isDone bool) error {
			t.Empty(newMerged)
			t.Empty(newUnmerged)
			t.Empty(reasons)
			t.False(isDone)

			return nil
		}))

		merged, err := t.closeMergers(mergers)
		t.NoError(err)
		t.Equal(0, len(merged))
	})
}

func (t *testWorkerPhaseMerger) newInsertWaitFuncs(
	insert workerPhaseMergerInsertFunc,
	wait workerPhaseMergerWaitFunc,
) func(worker string, mergeOps, unmergeOps []uint64, d time.Duration) (bool, error) {
	t.T().Helper()

	waitch := make(chan struct{}, 1)

	var once sync.Once

	return func(worker string, mergeOps, unmergeOps []uint64, d time.Duration) (bool, error) {
		if err := insert(worker, mergeOps, unmergeOps); err != nil {
			return false, err
		}

		once.Do(func() {
			go func() {
				if err := wait(func(newMerged, newUnmerged []uint64, reasons map[uint64]string, isDone bool) error {
					close(waitch)

					return nil
				}); err != nil {
					panic(err)
				}
			}()
		})

		select {
		case <-time.After(d):
			return false, nil
		case <-waitch:
			return true, nil
		}
	}
}

func (t *testWorkerPhaseMerger) TestEnoughWorkers() {
	t.Run("enough; local only", func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		_, args, vp, _, _ := t.newArgs()
		args.EnoughWorkers = 1
		insert, wait, err := newWorkerPhaseMerger(ctx, args)
		t.NoError(err)

		var opindex uint64 = 1
		_ = t.randomMergeValuesVP(vp, opindex, 1)

		t.NoError(insert(localWorkerName, []uint64{opindex}, nil))
		t.NoError(wait(func(newMerged, newUnmerged []uint64, reasons map[uint64]string, isDone bool) error {
			t.Empty(newMerged)
			t.Empty(newUnmerged)
			t.Empty(reasons)
			t.False(isDone)

			return nil
		}))
	})

	t.Run("enough; local only, but insert empty ops", func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		_, args, _, _, _ := t.newArgs()
		args.EnoughWorkers = 1
		insert, wait, err := newWorkerPhaseMerger(ctx, args)
		t.NoError(err)

		t.NoError(insert(localWorkerName, nil, nil))
		t.NoError(wait(func(newMerged, newUnmerged []uint64, reasons map[uint64]string, isDone bool) error {
			t.Empty(newMerged)
			t.Empty(newUnmerged)
			t.Empty(reasons)
			t.True(isDone)

			return nil
		}))

		err = wait(func([]uint64, []uint64, map[uint64]string, bool) error { return nil })
		t.ErrorIs(err, context.Canceled)
	})

	t.Run("enough; 3 workers, but insert empty ops", func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		_, args, _, _, _ := t.newArgs()
		args.EnoughWorkers = 3
		insert, wait, err := newWorkerPhaseMerger(ctx, args)
		t.NoError(err)

		insertf := t.newInsertWaitFuncs(insert, wait)

		done, err := insertf(localWorkerName, nil, nil, time.Millisecond*333)
		t.NoError(err)
		t.False(done)
		done, err = insertf("w0", nil, nil, time.Millisecond*333)
		t.NoError(err)
		t.False(done)
		done, err = insertf("w1", nil, nil, time.Second)
		t.NoError(err)
		t.True(done)
	})

	t.Run("enough; 3 workers", func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		_, args, vp, _, _ := t.newArgs()
		args.EnoughWorkers = 3

		ops := make([]uint64, 9)
		for i := range ops {
			opindex := uint64(i)
			ops[i] = opindex

			_ = t.randomMergeValuesVP(vp, opindex, 1)
		}

		insert, wait, err := newWorkerPhaseMerger(ctx, args)
		t.NoError(err)

		insertf := t.newInsertWaitFuncs(insert, wait)

		done, err := insertf(localWorkerName, ops[:3], nil, time.Millisecond*333)
		t.NoError(err)
		t.False(done)

		done, err = insertf("w0", ops[3:6], nil, time.Millisecond*333)
		t.NoError(err)
		t.False(done)

		done, err = insertf("w1", ops[6:9], nil, time.Second)
		t.NoError(err)
		t.True(done)
	})

	t.Run("enough; 3 workers; empty ops; over enough, but before local", func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		_, args, _, _, _ := t.newArgs()
		args.EnoughWorkers = 3
		insert, wait, err := newWorkerPhaseMerger(ctx, args)
		t.NoError(err)

		insertf := t.newInsertWaitFuncs(insert, wait)

		done, err := insertf("w0", nil, nil, time.Millisecond*333)
		t.NoError(err)
		t.False(done)
		done, err = insertf("w1", nil, nil, time.Millisecond*333)
		t.NoError(err)
		t.False(done)
		done, err = insertf("w2", nil, nil, time.Millisecond*333)
		t.NoError(err)
		t.False(done)
		done, err = insertf("w3", nil, nil, time.Second)
		t.NoError(err)
		t.False(done)

		done, err = insertf(localWorkerName, nil, nil, time.Second)
		t.NoError(err)
		t.True(done)
	})

	t.Run("enough; 3 workers; over enough, but before local", func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		_, args, vp, _, _ := t.newArgs()
		args.EnoughWorkers = 3

		ops := make([]uint64, 15)
		for i := range ops {
			opindex := uint64(i)
			ops[i] = opindex

			_ = t.randomMergeValuesVP(vp, opindex, 1)
		}

		insert, wait, err := newWorkerPhaseMerger(ctx, args)
		t.NoError(err)

		insertf := t.newInsertWaitFuncs(insert, wait)

		done, err := insertf("w0", ops[:3], nil, time.Millisecond*333)
		t.NoError(err)
		t.False(done)
		done, err = insertf("w1", ops[3:6], nil, time.Second)
		t.NoError(err)
		t.False(done)

		done, err = insertf(localWorkerName, ops[12:], nil, time.Second)
		t.NoError(err)
		t.True(done)
	})
}

func (t *testWorkerPhaseMerger) TestMerge() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	pub0 := base.NewDummyPublickey(util.UUID().String())
	pub1 := base.NewDummyPublickey(util.UUID().String())
	pub2 := base.NewDummyPublickey(util.UUID().String())

	sts := make([]base.State, 3)
	sts[0] = t.newState(pub0, 1)
	sts[1] = t.newState(pub1, 2)
	sts[2] = t.newState(pub2, 3)

	ops := [][]StateMergeValue{
		{
			t.newAccountStateMergeValue(pub0, 1),
			t.newAccountStateMergeValue(pub1, -2),
		},
		{ // 1: ignored
			t.newAccountStateMergeValue(pub1, -2),
			t.newAccountStateMergeValue(pub2, -3),
		},
		{
			t.newAccountStateMergeValue(pub0, -1),
			t.newAccountStateMergeValue(pub1, 3),
		},
		{ // 3: ignored
			t.newAccountStateMergeValue(pub0, -1),
			t.newAccountStateMergeValue(pub1, -2),
		},
		{
			t.newAccountStateMergeValue(pub0, 1),
			t.newAccountStateMergeValue(pub2, -3),
		},
		{
			t.newAccountStateMergeValue(pub0, 1),
			t.newAccountStateMergeValue(pub2, 4),
		},
	}

	_, args, vp, mergers, states := t.newArgs()
	args.EnoughWorkers = 3

	for i := range sts {
		states.SetValue(sts[i].Key(), sts[i])
	}

	seq := make([]uint64, len(ops))
	for i := range ops {
		opindex := uint64(i)
		vp.SetValue(opindex, ops[i])
		seq[i] = opindex
	}

	insert, wait, err := newWorkerPhaseMerger(ctx, args)
	t.NoError(err)

	t.NoError(insert("w0", seq[:3], nil))
	t.NoError(insert("w1", seq[3:], nil))
	t.NoError(insert(localWorkerName, seq, nil))

	t.NoError(wait(func(newMerged, newUnmerged []uint64, reasons map[uint64]string, isDone bool) error {
		t.Empty(newMerged)
		t.Equal([]uint64{1, 3}, newUnmerged)
		t.Equal(map[uint64]string{1: "previous issuer found", 3: "previous issuer found"}, reasons)
		t.False(isDone)

		return nil
	}))

	merged, err := t.closeMergers(mergers)
	t.NoError(err)
	t.Equal(len(sts), len(merged))

	b, found := merged[AccountStateKey(pub0)]
	t.True(found)
	t.Equal(int64(3), b)
	b, found = merged[AccountStateKey(pub1)]
	t.True(found)
	t.Equal(int64(3), b)
	b, found = merged[AccountStateKey(pub1)]
	t.True(found)
	t.Equal(int64(3), b)
}

func TestWorkerPhaseMerger(t *testing.T) {
	defer goleak.VerifyNone(t)

	suite.Run(t, new(testWorkerPhaseMerger))
}
