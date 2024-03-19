package testopsworkers

import (
	"cmp"
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
	t.T().Logf("* phaseStatuses: %d", dw.phaseStatuses.Len())

	dw.phaseStatuses.Traverse(func(phase int, i uint8) bool {
		t.T().Logf("  > phase=%d status=%d", phase, i)

		return true
	})

	t.T().Logf("* phaseDoneRecords: %d", dw.phaseDoneRecords.Len())
	dw.phaseDoneRecords.Traverse(func(phase int, m map[string]bool) bool {
		t.T().Logf("  > phase=%d", phase)

		for i := range m {
			t.T().Logf("    worker=%q has_unmerged_ops=%v", i, m[i])
		}

		return true
	})

	t.T().Logf("* tempWorkerPhaseDone: %d", len(dw.tempWorkerPhaseDone))
	for phase := range dw.tempWorkerPhaseDone {
		t.T().Logf("  > phase=%d", phase)

		for worker := range dw.tempWorkerPhaseDone[phase] {
			t.T().Logf("    worker=%q unmerged_ops=%v", worker, dw.tempWorkerPhaseDone[phase][worker])
		}
	}

	t.T().Logf("* psms: %d", dw.psms.Len())
	dw.psms.Traverse(func(phase int, i *phaseStatesMerger) bool {
		t.T().Logf("  > phase=%d is_not_nil=%v", phase, i != nil)

		return true
	})

	t.T().Logf("* phaseOps: %d", dw.phaseOps.Len())
	dw.phaseOps.Traverse(func(phase int, m map[uint64]struct{}) bool {
		addops, doneops := util.CompactAppendSlice[uint64](len(m))
		for i := range m {
			addops(i)
		}

		t.T().Logf("  > phase=%d ops=%d", phase, doneops())

		return true
	})
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

		dw.args.BroadcastPhaseDoneFunc = func(phase int, _ []uint64) error {
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

	t.Run("unmerge: unknown operation", func() {
		vs := t.randomMergeValues(1)

		err := dw.unmerge(0, []uint64{3})
		t.Error(err)
		t.ErrorIs(err, util.ErrNotFound)

		dw.valuespool.SetValue(3, vs)
		t.NoError(dw.unmerge(0, []uint64{3}))
	})
}

func (t *testDistributedStatesMergerWorker) TestInsertOp() {
	t.Run("ok", func() {
		dw := t.newWorker(context.Background())
		defer dw.Cancel()

		vs := t.randomMergeValues(1)
		t.NoError(dw.InsertOp(1, vs))

		t.Equal(1, dw.psms.Len())
	})

	t.Run("insert same opindex", func() {
		dw := t.newWorker(context.Background())
		defer dw.Cancel()

		vs := t.randomMergeValues(1)
		t.NoError(dw.InsertOp(1, vs))

		err := dw.InsertOp(1, nil)
		t.ErrorIs(err, util.ErrFound)

		t.Equal(1, dw.psms.Len())
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
		t.ErrorContains(err, "already insert done")
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
		t.ErrorContains(err, "already insert done")
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
		t.ErrorContains(err, "already insert done")
	})

	t.Run("enough worker done", func() {
		dw := t.newWorker(context.Background())
		defer dw.Cancel()

		dw.args.IsEnoughWorkers = func(n uint64) bool {
			return n > 1
		}

		t.NoError(dw.InsertDone())
		t.NoError(dw.WorkerInsertDone("w1"))

		v, found := dw.phaseStatuses.Value(-1)
		t.True(found)
		t.True(v > 1)
	})
}

func (t *testDistributedStatesMergerWorker) TestDone() {
	t.Run("empty insert; only local", func() {
		dw := t.newWorker(context.Background())
		defer dw.Cancel()

		dw.args.IsEnoughWorkers = func(n uint64) bool {
			return n > 0
		}

		t.NoError(dw.InsertDone())

		v, found := dw.phaseStatuses.Value(-1)
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

		workerPhaseDone := func(phase int, worker string) {
			if err := dw.WorkerPhaseDone(phase, nil, worker); err != nil {
				dw.Log().Error().Err(err).Str("worker", worker).Msg("worker phase done")
			}
		}

		dw.args.BroadcastPhaseDoneFunc = func(phase int, unmergedOps []uint64) error {
			go func() {
				workerPhaseDone(phase, "w0")
				workerPhaseDone(phase, "w1")
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

		workerPhaseDone := func(phase int, worker string) {
			var unmergeOps []uint64

			if phase == 0 {
				if worker == "w0" {
					unmergeOps = []uint64{1}
				} else {
					unmergeOps = []uint64{2}
				}
			}

			if err := dw.WorkerPhaseDone(phase, unmergeOps, worker); err != nil {
				t.LockedLog("error worker phase done: phase=%d worker=%q err=%+v", phase, worker, err)

				return
			}
		}

		dw.args.BroadcastPhaseDoneFunc = func(phase int, unmergedOps []uint64) error {
			go func() {
				workerPhaseDone(phase, "w0")
				workerPhaseDone(phase, "w1")
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

		workerPhaseDone := func(phase int, worker string) {
			if err := dw.WorkerPhaseDone(phase, nil, worker); err != nil {
				t.LockedLog("error worker phase done: phase=%d worker=%q err=%+v", phase, worker, err)

				return
			}
		}

		dw.args.BroadcastPhaseDoneFunc = func(phase int, unmergedOps []uint64) error {
			go func() {
				workerPhaseDone(phase, "w0")
				workerPhaseDone(phase, "w1")
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

func (t *testDistributedStatesMergerWorker) TestSetPhaseDoneRecord() {
	t.Run("added", func() {
		dw := t.newWorker(context.Background())
		defer dw.Cancel()

		dw.args.IsEnoughWorkers = func(n uint64) bool {
			return n > 1
		}

		added, isEmptyEnough, isMergeDoneEnough := dw.setPhaseDoneRecord(1, "w0", true)
		t.True(added)
		t.False(isEmptyEnough)
		t.False(isMergeDoneEnough)

		added, isEmptyEnough, isMergeDoneEnough = dw.setPhaseDoneRecord(1, "w0", true)
		t.False(added)
		t.False(isEmptyEnough)
		t.False(isMergeDoneEnough)

		added, isEmptyEnough, isMergeDoneEnough = dw.setPhaseDoneRecord(1, localStatesMergerWorker, true)
		t.True(added)
		t.False(isEmptyEnough)
		t.True(isMergeDoneEnough)
	})

	t.Run("merge enough enough; before local done", func() {
		dw := t.newWorker(context.Background())
		defer dw.Cancel()

		dw.args.IsEnoughWorkers = func(n uint64) bool {
			return n > 0
		}

		added, isEmptyEnough, isMergeDoneEnough := dw.setPhaseDoneRecord(1, "w0", true)
		t.True(added)
		t.False(isEmptyEnough)
		t.False(isMergeDoneEnough)

		added, isEmptyEnough, isMergeDoneEnough = dw.setPhaseDoneRecord(1, "w1", true)
		t.True(added)
		t.False(isEmptyEnough)
		t.False(isMergeDoneEnough)
	})

	t.Run("merge enough enough; after local done", func() {
		dw := t.newWorker(context.Background())
		defer dw.Cancel()

		dw.args.IsEnoughWorkers = func(n uint64) bool {
			return n > 2
		}

		added, isEmptyEnough, isMergeDoneEnough := dw.setPhaseDoneRecord(1, localStatesMergerWorker, true)
		t.True(added)
		t.False(isEmptyEnough)
		t.False(isMergeDoneEnough)

		added, isEmptyEnough, isMergeDoneEnough = dw.setPhaseDoneRecord(1, "w0", true)
		t.True(added)
		t.False(isEmptyEnough)
		t.False(isMergeDoneEnough)

		added, isEmptyEnough, isMergeDoneEnough = dw.setPhaseDoneRecord(1, "w1", true)
		t.True(added)
		t.False(isEmptyEnough)
		t.True(isMergeDoneEnough)
	})

	t.Run("empty enough; before local done", func() {
		dw := t.newWorker(context.Background())
		defer dw.Cancel()

		dw.args.IsEnoughWorkers = func(n uint64) bool {
			return n > 0
		}

		added, isEmptyEnough, isMergeDoneEnough := dw.setPhaseDoneRecord(1, "w0", true)
		t.True(added)
		t.False(isEmptyEnough)
		t.False(isMergeDoneEnough)

		added, isEmptyEnough, isMergeDoneEnough = dw.setPhaseDoneRecord(1, "w1", true)
		t.True(added)
		t.False(isEmptyEnough)
		t.False(isMergeDoneEnough)
	})

	t.Run("empty enough; but local not empty", func() {
		dw := t.newWorker(context.Background())
		defer dw.Cancel()

		dw.args.IsEnoughWorkers = func(n uint64) bool {
			return n > 1
		}

		added, isEmptyEnough, isMergeDoneEnough := dw.setPhaseDoneRecord(1, localStatesMergerWorker, true)
		t.True(added)
		t.False(isEmptyEnough)
		t.False(isMergeDoneEnough)

		added, isEmptyEnough, isMergeDoneEnough = dw.setPhaseDoneRecord(1, "w0", false)
		t.True(added)
		t.False(isEmptyEnough)
		t.True(isMergeDoneEnough)

		added, isEmptyEnough, isMergeDoneEnough = dw.setPhaseDoneRecord(1, "w1", false)
		t.True(added)
		t.False(isEmptyEnough)
		t.True(isMergeDoneEnough)
	})

	t.Run("empty enough; local not empty", func() {
		dw := t.newWorker(context.Background())
		defer dw.Cancel()

		dw.args.IsEnoughWorkers = func(n uint64) bool {
			return n > 1
		}

		added, isEmptyEnough, isMergeDoneEnough := dw.setPhaseDoneRecord(1, localStatesMergerWorker, false)
		t.True(added)
		t.False(isEmptyEnough)
		t.False(isMergeDoneEnough)

		added, isEmptyEnough, isMergeDoneEnough = dw.setPhaseDoneRecord(1, "w0", false)
		t.True(added)
		t.True(isEmptyEnough)
		t.True(isMergeDoneEnough)

		added, isEmptyEnough, isMergeDoneEnough = dw.setPhaseDoneRecord(1, "w1", false)
		t.True(added)
		t.True(isEmptyEnough)
		t.True(isMergeDoneEnough)
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

	workerPhaseDone := func(phase int, local string, unmergedOps []uint64) error {
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
				if err := dw.WorkerPhaseDone(phase, unmergedOps, worker); err != nil {
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

		dw.args.BroadcastPhaseDoneFunc = func(phase int, unmergedOps []uint64) error {
			return workerPhaseDone(phase, worker, unmergedOps)
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

func (t *testDistributedStatesMergerWorker) TestUnmerge3Workers() {
	nworkers := 3
	nops := 9

	workers := make([]string, nworkers)
	for i := range workers {
		workers[i] = fmt.Sprintf("w%02d", i)
	}

	valueOwners := map[string]string{}
	allOps := make([][]StateMergeValue, nops)
	pubs := make([]base.Publickey, nops)
	stvs := map[string]base.StateValue{}

	for i := range pubs {
		pub := base.NewMPrivatekey().Publickey()
		pubs[i] = pub

		key := AccountStateKey(pubs[i])

		stvs[key] = NewAccountStateValue(pub, 1)

		owner := workers[i%len(workers)]
		valueOwners[key] = owner

		t.T().Logf("op=%d owner=%q key=%q", i, owner, key)
	}

	for i := range allOps {
		pub := pubs[i]
		pub0 := pubs[(i+1)%len(pubs)]
		pub1 := pubs[(i+2)%len(pubs)]

		allOps[i] = []StateMergeValue{
			t.newAccountStateMergeValue(pub, 1),
			t.newAccountStateMergeValue(pub0, -1),
			t.newAccountStateMergeValue(pub1, -1),
		}

		t.T().Logf("opindex=%d key=%q balance=%d", i, AccountStateKey(pub), 1)
		t.T().Logf("opindex=%d key=%q balance=%d", i, AccountStateKey(pub0), -1)
	}

	getStateValue := func(key string) (base.StateValue, bool, error) {
		v, found := stvs[key]

		return v, found, nil
	}

	isWorkerValue := func(worker string, key string) bool {
		i, found := valueOwners[key]
		if !found {
			panic("unknown value")
		}

		return i == worker
	}

	filterWorkerValues := func(worker string, values []StateMergeValue) []StateMergeValue {
		return util.FilterSlice(values, func(value StateMergeValue) bool {
			return isWorkerValue(worker, value.Key())
		})
	}

	filterWorkerOps := func(worker string, ops []uint64) []uint64 {
		return util.FilterSlice(ops, func(op uint64) bool {
			var found bool

			_ = util.TraverseSliceV1(allOps[op], func(_ int, v StateMergeValue) (bool, error) {
				if isWorkerValue(worker, v.Key()) {
					found = true
				}

				return !found, nil
			})

			return found
		})
	}

	ctx, cancel := context.WithCancelCause(context.Background())
	defer cancel(nil)

	tNoError := t.NoError
	tTrue := t.True

	{
		psm, err := newPhaseStatesMerger(
			ctx,
			func(string) (StateMerger, bool) { return nil, false },
			func(opindex uint64) ([]StateMergeValue, bool) {
				return allOps[opindex], true
			},
			getStateValue,
			func(ctx context.Context) (distributedWorker, error) {
				return util.NewBaseJobWorker(ctx, 1<<6)
			},
			1<<9,
		)
		t.NoError(err)

		for i := range allOps {
			t.NoError(psm.Merge(uint64(i)))
		}

		t.NoError(psm.MergeDone())

		newlyMerged, newlyUnmerged, updatef, err := psm.Done()
		t.NoError(err)
		t.Empty(newlyMerged)
		t.NotEmpty(newlyUnmerged)

		t.T().Logf("newly_merged=%v newly_unmerged=%v", newlyMerged, newlyUnmerged)

		var addUpdated func(string) bool
		var doneUpdated func() []string

		updatef(
			func(stateKey string, merger StateMerger, mergedOps, unmergedOps []uint64, maxUpdatedValues uint64) error {
				if addUpdated == nil {
					addUpdated, doneUpdated = util.SortCompactAppendSlice[string](1<<8, cmp.Less[string])
				}

				st, err := merger.Close()
				tNoError(err)

				av, ok := st.(AccountStateValue)
				tTrue(ok)

				for i := range mergedOps {
					addUpdated(fmt.Sprintf("updated: merged_op=%d key=%q balance=%d merged_ops=%v unmerged_ops=%v", mergedOps[i], stateKey, av.Balance, mergedOps, unmergedOps))
				}

				return nil
			},
			func(opindex uint64, reason string) error {
				addUpdated(fmt.Sprintf("reason: opindex=%d reason=%q", opindex, reason))

				return nil
			},
		)

		util.TraverseSliceV1(doneUpdated(), func(_ int, i string) (bool, error) {
			t.T().Log(i)

			return true, nil
		})
	}

	dws := map[string]*DistributedStatesMergerWorker{}

	for i := 0; i < nworkers; i++ {
		worker := workers[i]

		dw := t.newWorker(ctx)
		dws[worker] = dw

		dw.workerName = worker
		defer dw.Cancel()

		// dw.SetLogger(logging.TestLogging.Log().With().Str("name", worker).Logger())

		dw.args.IsEnoughWorkers = func(n uint64) bool { return n >= uint64(nworkers) }
		dw.args.GetStateValue = getStateValue
	}

	workerPhaseDone := func(local string, phase int, unmergedOps []uint64) error {
		for worker := range dws {
			worker := worker

			if worker == local {
				continue
			}

			i := filterWorkerOps(worker, unmergedOps)
			t.T().Logf("phase done, %d: %q -> %q: unmerged_ops=%v filtered=%d\n", phase, local, worker, unmergedOps, i)

			go func() {
				if err := dws[worker].WorkerPhaseDone0(phase, i, len(unmergedOps) > 0, local); err != nil {
					cancel(err)
				}
			}()
		}

		return nil
	}

	for worker := range dws {
		worker := worker
		dw := dws[worker]

		dw.args.BroadcastPhaseDoneFunc = func(phase int, unmergedOps []uint64) error {
			return workerPhaseDone(worker, phase, unmergedOps)
		}
	}

	lockedLog := t.LockedLog

	for worker := range dws {
		worker := worker

		dws[worker].args.UpdatedValuesFunc = func(phase int, stateKey string, merger StateMerger, mergedOps, unmergedOps []uint64, maxUpdatedValues uint64) error {
			sv, err := merger.Close()
			tNoError(err)

			av, ok := sv.(AccountStateValue)
			tTrue(ok)

			lockedLog(
				"updated: worker=%q phase=%d key=%s balance=%d merged_ops=%v unmerged_ops=%v",
				worker, phase, stateKey, av.Balance, mergedOps, unmergedOps,
			)

			return nil
		}
	}

	for i := range allOps {
		for j := range workers {
			worker := workers[j]

			values := filterWorkerValues(worker, allOps[i])
			if len(values) < 1 {
				continue
			}

			addKeys, doneKeys := util.CompactAppendSlice[string](len(values))
			util.TraverseSliceV1(values, func(_ int, v StateMergeValue) (bool, error) {
				addKeys(v.Key())
				return true, nil
			})

			t.T().Logf("insert: opindex=%d worker=%q keys=%v values=%d", i, worker, doneKeys(), len(values))

			t.NoError(dws[worker].InsertOp(uint64(i), values))
		}
	}

	for i := range dws {
		dw := dws[i]
		t.NoError(dw.InsertDone())
	}

	for i := range dws {
		dw := dws[i]

		for j := range dws {
			if i == j {
				continue
			}

			t.NoError(dw.WorkerInsertDone(j))
		}
	}

	errch := make(chan error, 1)

	go func() {
		for i := range dws {
			if err := dws[i].Done(); err != nil {
				errch <- err

				break
			}
		}

		cancel(nil)
	}()

	select {
	case err := <-errch:
		t.NoError(err)

		return
	case <-ctx.Done():
	}

	switch err := context.Cause(ctx); {
	case errors.Is(err, context.Canceled):
	default:
		t.NoError(err)
	}
}

func TestDistributedStatesMergerWorker(t *testing.T) {
	defer goleak.VerifyNone(t)

	suite.Run(t, new(testDistributedStatesMergerWorker))
}
