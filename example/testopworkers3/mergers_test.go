package testopsworkers

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/pkg/errors"
	"github.com/spikeekips/mitum/base"
	"github.com/spikeekips/mitum/util"
	"github.com/spikeekips/mitum/util/logging"
	"github.com/stretchr/testify/suite"
	"go.uber.org/goleak"
)

func (dw *DistributedStatesMergerWorker) SetWorkerName(s string) {
	dw.workerName = s

	dw.SetLogger(dw.Log().With().Str("worker_name", s).Logger())
}

type testDistributedStatesMergerWorker struct {
	baseTestAccountStateMerger
}

func (t *testDistributedStatesMergerWorker) newWorker(ctx context.Context) *DistributedStatesMergerWorker {
	args := NewDistributedStatesMergerWorkerArgs()

	dw, err := NewDistributedStatesMergerWorker(ctx, args)
	t.NoError(err)

	dw.SetLogging(logging.TestNilLogging)

	return dw
}

func (t *testDistributedStatesMergerWorker) printDW(dw *DistributedStatesMergerWorker) {
	t.T().Logf("* phaseStatuse: %d", dw.phaseStatus.Len())

	dw.phaseStatus.Traverse(func(phase int, status uint8) bool {
		t.T().Logf("  > phase=%d status=%d", phase, status)

		return true
	})

	t.T().Logf("* phaseDoneRecords: %d", dw.phaseDoneRecords.Len())
	dw.phaseDoneRecords.Traverse(func(phase int, m map[string][3]interface{}) bool {
		t.T().Logf("  > phase=%d", phase)

		for i := range m {
			t.T().Logf("    worker=%q merged_ops=%v unmerged_ops=%v is_empty=%v", i, m[i][0], m[i][1], m[i][2])
		}

		return true
	})

	t.T().Logf("* psms: %d", len(dw.psms))
	for i := range dw.psms {
		t.T().Logf("  > phase=%d is_not_nil=%v", i, dw.psms[i] != nil)
	}

	addMergedOps, doneMergedOps := util.CompactAppendSlice[uint64](len(dw.phaseDoneOps))
	addUnmergedOps, doneUnmergedOps := util.CompactAppendSlice[uint64](len(dw.phaseDoneOps))

	for i := range dw.phaseDoneOps {
		switch dw.phaseDoneOps[i][1].(xMergeFlag) {
		case mergeFlag:
			addMergedOps(i)
		case unmergeFlag:
			addUnmergedOps(i)
		}
	}

	mergedOps, unmergedOps := doneMergedOps(), doneUnmergedOps()

	t.T().Logf("* phaseOps: %d", len(mergedOps)+len(unmergedOps))
	t.T().Logf("  > merged_ops=%v", mergedOps)
	t.T().Logf("  > unmerged_ops=%v", unmergedOps)
}

func (t *testDistributedStatesMergerWorker) TestCancel() {
	t.Run("cancel; wait Done()", func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		dw := t.newWorker(ctx)
		defer dw.Cancel()

		dw.args.IsEnoughWorkers = func(n uint64) bool {
			return n > 1
		}

		bch := make(chan int, 1)

		dw.args.BroadcastPhaseDoneFunc = func(phase int, _, _ []uint64) error {
			bch <- phase

			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(time.Second * 9):
				return nil
			}
		}

		t.NoError(dw.WorkerInsertDone("w0"))
		t.NoError(dw.InsertDone())

		errch := make(chan error, 1)

		go func() {
			errch <- dw.Done()
		}()

		select {
		case <-time.After(time.Second * 3):
			t.Fail("wait broadcast phase done, but failed")
		case phase := <-bch:
			t.Equal(0, phase)
		}

		cancel()

		select {
		case <-time.After(time.Second * 3):
			t.Fail("wait cancel, but failed")
		case err := <-errch:
			t.ErrorIs(err, context.Canceled)
		}
	})

	t.Run("insert op; data race", func() {
		dw := t.newWorker(context.Background())
		defer dw.Cancel()

		dw.args.IsEnoughWorkers = func(n uint64) bool {
			return n > 0
		}

		cancelch := make(chan struct{}, 1)
		ew, _ := util.NewErrgroupWorker(context.Background(), 1<<9)

		randomMergeValues := func() []StateMergeValue {
			pub := base.NewMPrivatekey().Publickey()

			v, _ := NewAccountStateMergeValue(pub, 1)

			return []StateMergeValue{v}
		}

		go func() {
			defer ew.Done()

			for i := uint64(0); i < 333; i++ {
				i := i
				if i == 99 {
					cancelch <- struct{}{}
				}

				if err := ew.NewJob(func(context.Context, uint64) error {
					return dw.InsertOp(i, randomMergeValues())
				}); err != nil {
					break
				}
			}
		}()

		<-cancelch
		dw.Cancel()

		err := ew.Wait()
		t.Error(err)
		t.ErrorIs(err, util.ErrLockedMapClosed)
	})

	t.Run("worker phase done; data race", func() {
		dw := t.newWorker(context.Background())
		defer dw.Cancel()

		dw.args.IsEnoughWorkers = func(n uint64) bool {
			return n > 333
		}

		ops := make([]uint64, 33)
		for i := uint64(0); i < 33; i++ {
			pub := base.NewMPrivatekey().Publickey()

			v, _ := NewAccountStateMergeValue(pub, 1)

			dw.valuespool.SetValue(i, []StateMergeValue{v})

			ops[i] = i
		}

		cancelch := make(chan struct{}, 1)
		ew, _ := util.NewErrgroupWorker(context.Background(), 1<<9)

		go func() {
			defer ew.Done()

			for i := uint64(0); i < 333; i++ {
				i := i
				if i == 99 {
					cancelch <- struct{}{}
				}

				if err := ew.NewJob(func(context.Context, uint64) error {
					m, _ := util.RandomChoiceSlice(ops, 5)
					um, _ := util.RandomChoiceSlice(ops, 5)

					return dw.WorkerPhaseDone(
						fmt.Sprintf("w%d", i),
						0,
						m, um,
						false,
					)
				}); err != nil {
					break
				}
			}
		}()

		<-cancelch
		dw.Cancel()

		err := ew.Wait()
		t.Error(err)
		t.ErrorIs(err, util.ErrLockedMapClosed)
	})
}

func (t *testDistributedStatesMergerWorker) TestPhaseDoneRecord() {
	t.Run("local only; empty", func() {
		dw := t.newWorker(context.Background())
		defer dw.Cancel()

		dw.args.IsEnoughWorkers = func(n uint64) bool {
			return n > 0
		}

		isMergeDoneEnough, isEmptyEnough, err := dw.setPhaseDoneRecord(localStatesMergerWorker, 1, nil, nil, true)
		t.NoError(err)
		t.False(isMergeDoneEnough)
		t.True(isEmptyEnough)
	})

	t.Run("local only; not empty", func() {
		dw := t.newWorker(context.Background())
		defer dw.Cancel()

		dw.args.IsEnoughWorkers = func(n uint64) bool {
			return n > 0
		}

		isMergeDoneEnough, isEmptyEnough, err := dw.setPhaseDoneRecord(localStatesMergerWorker, 1, nil, nil, false)
		t.NoError(err)
		t.True(isMergeDoneEnough)
		t.False(isEmptyEnough)
	})

	t.Run("not yet local; empty", func() {
		dw := t.newWorker(context.Background())
		defer dw.Cancel()

		dw.args.IsEnoughWorkers = func(n uint64) bool {
			return n > 0
		}

		isMergeDoneEnough, isEmptyEnough, err := dw.setPhaseDoneRecord("w0", 1, nil, nil, true)
		t.NoError(err)
		t.False(isMergeDoneEnough)
		t.False(isEmptyEnough)

		isMergeDoneEnough, isEmptyEnough, err = dw.setPhaseDoneRecord("w1", 1, nil, nil, true)
		t.NoError(err)
		t.False(isMergeDoneEnough)
		t.False(isEmptyEnough)

		isMergeDoneEnough, isEmptyEnough, err = dw.setPhaseDoneRecord(localStatesMergerWorker, 1, nil, nil, true)
		t.NoError(err)
		t.False(isMergeDoneEnough)
		t.True(isEmptyEnough)
	})

	t.Run("not yet local; not empty", func() {
		dw := t.newWorker(context.Background())
		defer dw.Cancel()

		dw.args.IsEnoughWorkers = func(n uint64) bool {
			return n > 0
		}

		isMergeDoneEnough, isEmptyEnough, err := dw.setPhaseDoneRecord("w0", 1, nil, nil, false)
		t.NoError(err)
		t.False(isMergeDoneEnough)
		t.False(isEmptyEnough)

		isMergeDoneEnough, isEmptyEnough, err = dw.setPhaseDoneRecord("w1", 1, nil, nil, false)
		t.NoError(err)
		t.False(isMergeDoneEnough)
		t.False(isEmptyEnough)

		isMergeDoneEnough, isEmptyEnough, err = dw.setPhaseDoneRecord(localStatesMergerWorker, 1, nil, nil, false)
		t.NoError(err)
		t.True(isMergeDoneEnough)
		t.False(isEmptyEnough)
	})

	t.Run("not enough; local empty", func() {
		dw := t.newWorker(context.Background())
		defer dw.Cancel()

		dw.args.IsEnoughWorkers = func(n uint64) bool {
			return n > 2
		}

		isMergeDoneEnough, isEmptyEnough, err := dw.setPhaseDoneRecord("w0", 1, nil, nil, true)
		t.NoError(err)
		t.False(isMergeDoneEnough)
		t.False(isEmptyEnough)

		isMergeDoneEnough, isEmptyEnough, err = dw.setPhaseDoneRecord("w1", 1, nil, nil, true)
		t.NoError(err)
		t.False(isMergeDoneEnough)
		t.False(isEmptyEnough)

		isMergeDoneEnough, isEmptyEnough, err = dw.setPhaseDoneRecord(localStatesMergerWorker, 1, nil, nil, true)
		t.NoError(err)
		t.False(isMergeDoneEnough)
		t.True(isEmptyEnough)
	})

	t.Run("not enough; local not empty", func() {
		dw := t.newWorker(context.Background())
		defer dw.Cancel()

		dw.args.IsEnoughWorkers = func(n uint64) bool {
			return n > 0
		}

		isMergeDoneEnough, isEmptyEnough, err := dw.setPhaseDoneRecord("w0", 1, nil, nil, true)
		t.NoError(err)
		t.False(isMergeDoneEnough)
		t.False(isEmptyEnough)

		isMergeDoneEnough, isEmptyEnough, err = dw.setPhaseDoneRecord("w1", 1, nil, nil, true)
		t.NoError(err)
		t.False(isMergeDoneEnough)
		t.False(isEmptyEnough)

		isMergeDoneEnough, isEmptyEnough, err = dw.setPhaseDoneRecord(localStatesMergerWorker, 1, nil, nil, false)
		t.NoError(err)
		t.True(isMergeDoneEnough)
		t.False(isEmptyEnough)
	})
}

func (t *testDistributedStatesMergerWorker) TestValuesPool() {
	dw := t.newWorker(context.Background())
	defer dw.Cancel()

	t.Run("InsertOp: known operation", func() {
		vs := t.randomMergeValues(1)

		t.NoError(dw.InsertOp(1, vs))

		err := dw.InsertOp(1, vs)
		t.Error(err)
		t.ErrorIs(err, util.ErrFound)
	})
}

func (t *testDistributedStatesMergerWorker) TestInsertOp() {
	t.Run("ok", func() {
		dw := t.newWorker(context.Background())
		defer dw.Cancel()

		vs := t.randomMergeValues(1)
		t.NoError(dw.InsertOp(1, vs))

		t.Equal(1, len(dw.psms))
	})

	t.Run("insert same opindex", func() {
		dw := t.newWorker(context.Background())
		defer dw.Cancel()

		vs := t.randomMergeValues(1)
		t.NoError(dw.InsertOp(1, vs))

		err := dw.InsertOp(1, nil)
		t.ErrorIs(err, util.ErrFound)

		t.Equal(1, len(dw.psms))
	})

	t.Run("insert after empty done", func() {
		dw := t.newWorker(context.Background())
		defer dw.Cancel()

		dw.args.IsEnoughWorkers = func(n uint64) bool {
			return n > 0
		}

		t.NoError(dw.InsertDone())

		vs := t.randomMergeValues(1)
		err := dw.InsertOp(2, vs)
		t.ErrorIs(err, errDistributedStatesMergerWorkerAlreadyPhaseDone)
	})

	t.Run("insert after done", func() {
		dw := t.newWorker(context.Background())
		defer dw.Cancel()

		dw.args.IsEnoughWorkers = func(n uint64) bool {
			return n > 0
		}

		vs := t.randomMergeValues(1)
		t.NoError(dw.InsertOp(1, vs))
		t.NoError(dw.InsertDone())

		vs = t.randomMergeValues(1)
		err := dw.InsertOp(2, vs)
		t.ErrorIs(err, errDistributedStatesMergerWorkerAlreadyPhaseDone)
	})
}

func (t *testDistributedStatesMergerWorker) TestInsertDone() {
	dw := t.newWorker(context.Background())
	defer dw.Cancel()

	dw.args.IsEnoughWorkers = func(n uint64) bool {
		return false
	}

	t.Run("ok", func() {
		vs := t.randomMergeValues(1)
		t.NoError(dw.InsertOp(1, vs))
		t.NoError(dw.InsertDone())
	})

	t.Run("done", func() {
		err := dw.InsertDone()
		t.ErrorIs(err, util.ErrFound)
	})

	t.Run("worker done again", func() {
		t.NoError(dw.WorkerInsertDone("w0"))

		err := dw.WorkerInsertDone("w0")
		t.ErrorIs(err, util.ErrFound)
	})

	t.Run("enough worker done", func() {
		dw := t.newWorker(context.Background())
		defer dw.Cancel()

		dw.args.IsEnoughWorkers = func(n uint64) bool {
			return n > 1
		}

		t.NoError(dw.InsertDone())
		t.NoError(dw.WorkerInsertDone("w1"))

		v, found := dw.phaseStatus.Value(-1)
		t.True(found)
		t.True(v > 1)
	})

	t.Run("empty", func() {
		dw := t.newWorker(context.Background())
		defer dw.Cancel()

		dw.args.IsEnoughWorkers = func(n uint64) bool {
			return n > 1
		}

		tEqual := t.Equal
		tEmpty := t.Empty
		tNoError := t.NoError

		dw.args.BroadcastPhaseDoneFunc = func(phase int, mergedOps, unmergedOps []uint64) error {
			tEqual(0, phase)
			tEmpty(mergedOps)
			tEmpty(unmergedOps)

			go func() {
				tNoError(dw.WorkerPhaseDone("w0", phase, nil, nil, true))
			}()

			return nil
		}

		t.NoError(dw.InsertDone())
		t.NoError(dw.WorkerInsertDone("w0"))
		t.NoError(dw.Done())
	})
}

func (t *testDistributedStatesMergerWorker) TestDumpPhaseDoneRecords() {
	dw := t.newWorker(context.Background())
	defer dw.Cancel()

	dw.args.IsEnoughWorkers = func(n uint64) bool {
		return n > 2
	}

	dw.phaseDoneOps = map[uint64][2]interface{}{}

	for i := uint64(1); i < 7; i++ {
		_ = t.randomMergeValuesVP(dw.valuespool, i, 1)

		if i < 4 {
			dw.phaseDoneOps[i] = [2]interface{}{0, unmergeFlag}
		} else {
			dw.phaseDoneOps[i] = [2]interface{}{0, mergeFlag}
		}
	}

	isMergeDoneEnough, isEmptyEnough, err := dw.isEnoughWorkerPhaseDone("w0", 0, 0, []uint64{1, 2, 3}, []uint64{4, 5, 6}, false)
	t.NoError(err)
	t.False(isMergeDoneEnough)
	t.False(isEmptyEnough)

	t.Empty(dw.psms)

	isMergeDoneEnough, isEmptyEnough, err = dw.isEnoughWorkerPhaseDone(localStatesMergerWorker, 0, 0, []uint64{1, 2, 3}, []uint64{4, 5, 6}, false)
	t.NoError(err)
	t.False(isMergeDoneEnough)
	t.False(isEmptyEnough)

	t.NotEmpty(dw.psms)
	_, found := dw.psms[1]
	t.True(found)

	isMergeDoneEnough, isEmptyEnough, err = dw.isEnoughWorkerPhaseDone("w1", 0, 0, []uint64{1, 2, 3}, []uint64{4, 5, 6}, false)
	t.NoError(err)
	t.True(isMergeDoneEnough)
	t.False(isEmptyEnough)
}

func (t *testDistributedStatesMergerWorker) TestDone() {
	t.Run("empty insert; only local", func() {
		dw := t.newWorker(context.Background())
		defer dw.Cancel()

		dw.args.IsEnoughWorkers = func(n uint64) bool {
			return n > 0
		}

		t.NoError(dw.InsertDone())

		v, found := dw.phaseStatus.Value(-1)
		t.True(found)
		t.True(v > 1)

		t.NoError(dw.Done())
	})
}

func (t *testDistributedStatesMergerWorker) TestPhaseDone() {
	tNoError := t.NoError
	tTrue := t.True
	tEqual := t.Equal
	tEmpty := t.Empty
	tFail := t.Fail

	t.Run("ok", func() {
		dw := t.newWorker(context.Background())
		defer dw.Cancel()

		// 3 workers, including local
		dw.args.IsEnoughWorkers = func(n uint64) bool {
			return n > 2
		}

		workerPhaseDone := func(worker string, phase int, mergedOps, unmergedOps []uint64) {
			if err := dw.WorkerPhaseDone(worker, phase, mergedOps, unmergedOps, len(mergedOps) < 1 && len(unmergedOps) < 1); err != nil {
				dw.Log().Error().Err(err).Str("worker", worker).Msg("worker phase done")
			}
		}

		dw.args.BroadcastPhaseDoneFunc = func(phase int, mergedOps, unmergedOps []uint64) error {
			go func() {
				workerPhaseDone("w0", phase, mergedOps, unmergedOps)
				workerPhaseDone("w1", phase, mergedOps, unmergedOps)
			}()

			return nil
		}

		vs0 := t.randomMergeValues(1)
		vs1 := t.randomMergeValues(2)
		vs2 := t.randomMergeValues(3)

		avs0 := vs0[0].(AccountStateMergeValue)
		avs1 := vs1[0].(AccountStateMergeValue)
		avs2 := vs2[0].(AccountStateMergeValue)

		dw.args.UpdatedValuesFunc = func(phase int, stateKey string, merger StateMerger, mergedOps, unmergedOps []uint64, maxUpdatedValues uint64) error {
			//t.LockedLog("updated values: stateKey=%q merger=%v mergedOps=%v unmergedOps=%v maxUpdatedValues=%d",
			//	stateKey, merger != nil, mergedOps, unmergedOps, maxUpdatedValues,
			//)

			sv, err := merger.Close()
			tNoError(err)

			av, ok := sv.(AccountStateValue)
			tTrue(ok)

			switch stateKey {
			case vs0[0].Key():
				tTrue(avs0.Address.Equal(av.Address))
				tEqual(int64(1), av.Balance)
				tEqual([]uint64{1}, mergedOps)
			case vs1[0].Key():
				tTrue(avs1.Address.Equal(av.Address))
				tEqual(int64(2), av.Balance)
				tEqual([]uint64{2}, mergedOps)
			case vs2[0].Key():
				tTrue(avs2.Address.Equal(av.Address))
				tEqual(int64(3), av.Balance)
				tEqual([]uint64{3}, mergedOps)
			default:
				tFail("unknown state key, %q", stateKey)
			}

			tEmpty(unmergedOps)
			tEqual(uint64(3), maxUpdatedValues)

			return nil
		}

		t.NoError(dw.InsertOp(1, vs0))
		t.NoError(dw.InsertDone())

		t.NoError(dw.InsertOp(2, vs1))
		t.NoError(dw.WorkerInsertDone("w0"))

		t.NoError(dw.InsertOp(3, vs2))
		t.NoError(dw.WorkerInsertDone("w1"))

		t.NoError(dw.Done())
	})

	t.Run("with unmerge ops", func() {
		dw := t.newWorker(context.Background())
		defer dw.Cancel()

		// 3 workers, including local
		dw.args.IsEnoughWorkers = func(n uint64) bool {
			return n > 2
		}

		workerPhaseDone := func(worker string, phase int, mergedOps, unmergedOps []uint64) {
			if phase == 0 && len(unmergedOps) < 1 {
				if worker == "w0" {
					unmergedOps = []uint64{1}
				} else {
					unmergedOps = []uint64{2}
				}
			}

			if err := dw.WorkerPhaseDone(worker, phase, mergedOps, unmergedOps, len(mergedOps) < 1 && len(unmergedOps) < 1); err != nil {
				t.LockedLog("error worker phase done: phase=%d worker=%q err=%+v", phase, worker, err)

				return
			}
		}

		dw.args.BroadcastPhaseDoneFunc = func(phase int, mergedOps, unmergedOps []uint64) error {
			go func() {
				workerPhaseDone("w0", phase, mergedOps, unmergedOps)
				workerPhaseDone("w1", phase, mergedOps, unmergedOps)
			}()

			return nil
		}

		vs0 := t.randomMergeValues(1)
		vs1 := t.randomMergeValues(1)
		vs2 := t.randomMergeValues(1)

		dw.args.UpdatedValuesFunc = func(phase int, stateKey string, merger StateMerger, mergedOps, unmergedOps []uint64, maxUpdatedValues uint64) error {
			//t.LockedLog("updated values: stateKey=%q merger=%v mergedOps=%v unmergedOps=%v maxUpdatedValues=%d",
			//	stateKey, merger != nil, mergedOps, unmergedOps, maxUpdatedValues,
			//)

			if phase == 0 {
				switch stateKey {
				case vs0[0].Key():
					tEqual([]uint64{1}, mergedOps)
				case vs1[0].Key():
					tEqual([]uint64{2}, mergedOps)
				case vs2[0].Key():
					tEqual([]uint64{3}, mergedOps)
				default:
					tFail("unknown state key, %q", stateKey)
				}

				tEmpty(unmergedOps)
				tEqual(uint64(3), maxUpdatedValues)
			}

			if phase == 1 {
				tEmpty(mergedOps)

				switch stateKey {
				case vs0[0].Key():
					tEqual([]uint64{1}, unmergedOps)
				case vs1[0].Key():
					tEqual([]uint64{2}, unmergedOps)
				case vs2[0].Key():
					tEqual([]uint64{3}, unmergedOps)
				default:
					tFail("unknown state key, %q", stateKey)
				}

				tEqual(uint64(2), maxUpdatedValues)
			}

			return nil
		}

		t.NoError(dw.InsertOp(1, vs0))
		t.NoError(dw.InsertDone())

		t.NoError(dw.InsertOp(2, vs1))
		t.NoError(dw.WorkerInsertDone("w0"))

		t.NoError(dw.InsertOp(3, vs2))
		t.NoError(dw.WorkerInsertDone("w1"))

		t.NoError(dw.Done())
	})

	t.Run("with empty ops", func() {
		dw := t.newWorker(context.Background())
		defer dw.Cancel()

		// 3 workers, including local
		dw.args.IsEnoughWorkers = func(n uint64) bool {
			return n > 2
		}

		workerPhaseDone := func(worker string, phase int, mergedOps, unmergedOps []uint64) {
			if err := dw.WorkerPhaseDone(worker, phase, mergedOps, unmergedOps, len(mergedOps) < 1 && len(unmergedOps) < 1); err != nil {
				t.LockedLog("error worker phase done: phase=%d worker=%q err=%+v", phase, worker, err)

				return
			}
		}

		dw.args.BroadcastPhaseDoneFunc = func(phase int, mergedOps, unmergedOps []uint64) error {
			go func() {
				workerPhaseDone("w0", phase, mergedOps, unmergedOps)
				workerPhaseDone("w1", phase, mergedOps, unmergedOps)
			}()

			return nil
		}

		dw.args.UpdatedValuesFunc = func(phase int, stateKey string, merger StateMerger, mergedOps, unmergedOps []uint64, maxUpdatedValues uint64) error {
			return nil
		}

		t.NoError(dw.WorkerInsertDone("w0"))
		t.NoError(dw.WorkerInsertDone("w1"))
		t.NoError(dw.InsertDone())

		t.NoError(dw.Done())
	})
}

func (t *testDistributedStatesMergerWorker) testAllUnmergeNWorkers(workers, keys int) {
	stateKeys := map[string]uint64{}
	vss := make([]StateMergeValue, keys)
	allUnmergedOps := make([]uint64, len(vss))

	for i := range vss {
		vs := t.randomMergeValues(i + 1)
		vss[i] = vs[0]
		stateKeys[vs[0].Key()] = uint64(i)
		allUnmergedOps[i] = uint64(i)
	}

	ctx, cancel := context.WithCancelCause(context.Background())
	defer cancel(nil)

	dws := map[string]*DistributedStatesMergerWorker{}

	for i := 0; i < workers; i++ {
		worker := fmt.Sprintf("dw%02d", i)

		dw := t.newWorker(ctx)
		dws[worker] = dw

		dw.workerName = worker
		defer dw.Cancel()

		// dw.SetLogger(logging.TestLogging.Log().With().Str("name", worker).Logger())

		dw.args.IsEnoughWorkers = func(n uint64) bool { return n >= uint64(workers) }
	}

	workerPhaseDone := func(local string, phase int, mergedOps, unmergedOps []uint64) error {
		if phase == 0 {
			unmergedOps = allUnmergedOps
		}

		dw := dws[local]

		for worker := range dws {
			if worker == local {
				continue
			}

			worker := worker

			go func() {
				if err := dw.WorkerPhaseDone(worker, phase, mergedOps, unmergedOps, len(mergedOps) < 1 && len(unmergedOps) < 1); err != nil {
					cancel(err)

					return
				}
			}()
		}

		return nil
	}

	for worker := range dws {
		worker := worker
		dw := dws[worker]

		dw.args.BroadcastPhaseDoneFunc = func(phase int, mergedOps, unmergedOps []uint64) error {
			return workerPhaseDone(worker, phase, mergedOps, unmergedOps)
		}
	}

	tNoError := t.NoError
	tTrue := t.True
	tEqual := t.Equal
	tEmpty := t.Empty

	for worker := range dws {
		dws[worker].args.UpdatedValuesFunc = func(phase int, stateKey string, merger StateMerger, mergedOps, unmergedOps []uint64, maxUpdatedValues uint64) error {
			sv, err := merger.Close()
			tNoError(err)

			av, ok := sv.(AccountStateValue)
			tTrue(ok)

			switch phase {
			case 0:
				opindex, found := stateKeys[stateKey]
				tTrue(found)

				tEqual(int64(opindex+1), av.Balance)

				tTrue(vss[opindex].(AccountStateMergeValue).Address.Equal(av.Address))
				tEqual([]uint64{opindex}, mergedOps)

				tEmpty(unmergedOps)
				tEqual(uint64(len(vss)), maxUpdatedValues)
			case 1:
				tEqual(int64(0), av.Balance)
				tEmpty(mergedOps)

				opindex, found := stateKeys[stateKey]
				tTrue(found)

				tTrue(vss[opindex].(AccountStateMergeValue).Address.Equal(av.Address))
				tEqual([]uint64{opindex}, unmergedOps)

				tEqual(uint64(len(vss)), maxUpdatedValues)
			}

			return nil
		}
	}

	for i := range vss {
		for worker := range dws {
			t.NoError(dws[worker].InsertOp(uint64(i), []StateMergeValue{vss[i]}))
		}
	}

	for i := range dws {
		dw := dws[i]
		t.NoError(dw.InsertDone())

		for j := range dws {
			if j == i {
				continue
			}

			t.NoError(dw.WorkerInsertDone(j))
		}
	}

	go func() {
		for i := range dws {
			tNoError(dws[i].Done())
		}

		cancel(nil)
	}()

	<-ctx.Done()

	switch err := context.Cause(ctx); {
	case errors.Is(err, context.Canceled):
	default:
		t.NoError(err)
	}
}

func (t *testDistributedStatesMergerWorker) TestAllUnmergeXWorkers() {
	for i := 1; i <= 30; i += 5 {
		for j := 0; j <= 30; j += 5 {
			started := time.Now()

			t.testAllUnmergeNWorkers(i, j)

			t.T().Logf("< workers=%d keys=%d elapsed=%s", i, j, time.Since(started))
		}
	}
}

func TestDistributedStatesMergerWorker(t *testing.T) {
	defer goleak.VerifyNone(t)

	suite.Run(t, new(testDistributedStatesMergerWorker))
}
