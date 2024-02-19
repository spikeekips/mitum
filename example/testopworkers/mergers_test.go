package testopsworkers

import (
	"context"
	"fmt"
	"slices"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/spikeekips/mitum/base"
	"github.com/spikeekips/mitum/util"
	"github.com/stretchr/testify/suite"
	"go.uber.org/goleak"
)

type testDistributedStatesMerger struct {
	baseTestAccountStateMerger
}

func (t *testDistributedStatesMerger) newMergers(
	ctx context.Context,
	local string,
	workersize int64,
) *DistributedStatesMergerWorker {
	args := NewDistributedStatesMergerWorkerArgs()
	args.NewDistributedWorker = func(ctx context.Context) (distributedWorker, error) {
		return util.NewErrgroupWorker(ctx, workersize)
	}

	dm, err := newDistributedStatesMergerWorker(ctx, local, args)
	t.NoError(err)

	//_ = dm.SetLogging(logging.TestLogging)

	return dm
}

func (t *testDistributedStatesMerger) testWorkersPhaseDone(
	nWorkers int,
	workersize int64,
	ops map[int] /* worker */ map[uint64][]StateMergeValue,
) ([]string, []*DistributedStatesMergerWorker, func() error, func()) {
	t.T().Log("number of workers:", nWorkers)

	ctx, cancel := context.WithCancel(context.Background())

	workernames := make([]string, nWorkers)
	workers := make([]*DistributedStatesMergerWorker, nWorkers)

	for i := range workers {
		name := fmt.Sprintf("w%d", i)
		workernames[i] = name

		dm := t.newMergers(ctx, name, workersize)

		workers[i] = dm
	}

	doneEnough := func(n uint64) bool { return n == uint64(nWorkers) }
	for i := range workers {
		workers[i].args.DoneEnough = doneEnough
	}

	phaseBroadcastedch := make(chan [3]interface{}, 1<<9)
	broadcastPhaseDoneFunc := func(worker string) func(int, []uint64) error {
		return func(phase int, unmergedOps []uint64) error {
			t.T().Logf("phase done: worker=%q phase=%d unmergedOps=%v", worker, phase, unmergedOps)

			phaseBroadcastedch <- [3]interface{}{worker, phase, unmergedOps}

			return nil
		}
	}

	for i := range workers {
		workers[i].args.BroadcastPhaseDoneFunc = broadcastPhaseDoneFunc(workernames[i])
	}

	finishedch := make(chan [2]interface{}, 1<<9)
	whenFinishedFunc := func(worker string) func(int) error {
		return func(phase int) error {
			t.T().Logf("finished: worker=%q phase=%d", worker, phase)

			finishedch <- [2]interface{}{worker, phase}

			return nil
		}
	}

	for i := range workers {
		workers[i].args.WhenDoneFunc = whenFinishedFunc(workernames[i])
	}

	return workernames, workers, func() error {
			go func() {
			end:
				for {
					select {
					case <-ctx.Done():
						break end
					case i := <-phaseBroadcastedch:
						from := i[0].(string)
						phase := i[1].(int)
						unmergeOp := i[2].([]uint64)

						for w := range workers {
							worker := workers[w]

							if from == workernames[w] {
								continue
							}

							worker.WorkerPhaseDone(from, phase, unmergeOp)
						}
					}
				}
			}()

			for w := range ops {
				worker := workers[w]

				for opindex := range ops[w] {
					if err := worker.LocalMerge(opindex, ops[w][opindex]); err != nil {
						return err
					}
				}
			}

			for i := range workers {
				if err := workers[i].LocalMerge(0, nil); err != nil {
					return err
				}
			}

			for i := range workers {
				worker := workers[i]

				for j := range workers {
					if i == j {
						continue
					}

					if err := worker.WorkerMerge(workernames[j], 0, nil); err != nil {
						return err
					}
				}
			}

			ew, err := util.NewErrgroupWorker(ctx, workersize)
			if err != nil {
				return err
			}

			go func() {
				defer ew.Done()

				for i := range workers {
					worker := workers[i]

					t.NoError(ew.NewJob(func(context.Context, uint64) error {
						if err := worker.Done(); err != nil {
							return err
						}

						return worker.SaveDone()
					}))
				}
			}()
			if err := ew.Wait(); err != nil {
				return err
			}

			var finished [][2]interface{}

			timeout := time.After(time.Second * 2)

		end:
			for {
				select {
				case <-timeout:
					t.Fail("expected broadcast phase done")
				case i := <-finishedch:
					finished = append(finished, i)

					if len(finished) == nWorkers {
						break end
					}
				}
			}

			return nil
		}, func() {
			for i := range workers {
				workers[i].Cancel()
			}

			cancel()
		}
}

func (t *testDistributedStatesMerger) TestSingleWorkerPhaseDone() {
	finishedch := make(chan int, 1)
	phaseBroadcastedch := make(chan int, 1)

	dm := t.newMergers(context.Background(), "local", 1)
	dm.args.DoneEnough = func(n uint64) bool { return n > 0 }
	dm.args.BroadcastPhaseDoneFunc = func(phase int, unmergedOps []uint64) error {
		t.T().Logf("phase done: phase=%d unmergedOps=%v", phase, unmergedOps)

		phaseBroadcastedch <- phase

		return nil
	}
	dm.args.WhenDoneFunc = func(phase int) error {
		finishedch <- phase

		return nil
	}

	defer dm.Cancel()

	pub0 := base.NewMPrivatekey().Publickey()
	pubs := make([]base.Publickey, 3)
	values := make([]AccountStateMergeValue, len(pubs))
	ops := map[uint64][]StateMergeValue{}

	for i := range pubs {
		pub, value := t.randomAccountStateMergeValue(int64(i) + 1)
		pubs[i] = pub
		values[i] = value

		value0 := t.newAccountStateMergeValue(pub0, 1)

		ops[uint64(i)] = []StateMergeValue{value, value0}
	}

	for i := range ops {
		t.NoError(dm.LocalMerge(i, ops[i]))
	}

	t.NoError(dm.LocalMerge(0, nil))

	select {
	case <-time.After(time.Second * 2):
		t.Fail("expected broadcast phase done")
	case phase := <-phaseBroadcastedch:
		t.Equal(1, phase)
	}

	select {
	case <-time.After(time.Second * 2):
		t.Fail("expected finished")
	case phase := <-finishedch:
		t.Equal(1, phase)
	}

	t.T().Log("mergers done")

	t.T().Logf("phasesState: %v", dm.phasesState.Map())
	t.T().Logf("statesMergedOps: %v", dm.statesMergedOps.Map())
	t.T().Logf("tempWorkerPhaseDone: %v", dm.tempWorkerPhaseDone)
	t.T().Logf("phaseDoneRecords: %v", dm.phaseDoneRecords)

	addMergedOps, doneMergedOps := util.SortCompactAppendSlice[uint64](len(dm.allMergedOps), func(a, b uint64) bool {
		return a < b
	})
	for i := range dm.allMergedOps {
		addMergedOps(i)
	}

	t.Equal([]uint64{0, 1, 2}, doneMergedOps())

	t.Equal(len(pubs)+1, dm.mergers.Len())

	dm.mergers.Traverse(func(key string, merger StateMerger) bool {
		value, err := merger.Close()
		t.NoError(err)

		av := value.(AccountStateValue)
		t.T().Logf("merged account: address=%v balance=%v", av.Address, av.Balance)

		switch av.Address.String() {
		case pub0.String():
			t.Equal(int64(3), av.Balance)
		case pubs[0].String():
			t.Equal(int64(1), av.Balance)
		case pubs[1].String():
			t.Equal(int64(2), av.Balance)
		case pubs[2].String():
			t.Equal(int64(3), av.Balance)
		}

		return true
	})
}

func (t *testDistributedStatesMerger) Test2WorkersPhaseDone() {
	pubs := make([]base.Publickey, 33)
	allValues := make([]AccountStateMergeValue, len(pubs))
	ops := map[uint64][]StateMergeValue{}

	expectedOps := make([]uint64, len(pubs))
	allKeys := make([]string, len(pubs))

	for i := range pubs {
		pub, value := t.randomAccountStateMergeValue(int64(i) + 1)
		pubs[i] = pub
		allValues[i] = value
		allKeys[i] = AccountStateKey(pub)

		opindex := uint64(i)
		ops[opindex] = []StateMergeValue{value}
		expectedOps[i] = opindex
	}
	sort.Strings(allKeys)

	opm := map[int]map[uint64][]StateMergeValue{0: ops, 1: ops}

	_, workers, done, cancel := t.testWorkersPhaseDone(2, 1, opm)
	defer cancel()

	workerKeysFuncs := make([]func() []string, len(workers))
	workerMergedOpsFuncs := make([]func() []uint64, len(workers))

	for i := range workers {
		workers[i].args.SyncMergedStateValuesFunc = func(_ context.Context, phase int, keys []string, values []base.StateValue) error {
			t.Equal(len(keys), len(values))
			t.Equal(1, phase)
			t.Equal(len(allKeys), len(keys))

			sort.Strings(keys)
			t.Equal(allKeys, keys)

			for j := range values {
				av := values[j].(AccountStateValue)
				k := slices.IndexFunc(pubs, func(l base.Publickey) bool {
					return l.Equal(av.Address)
				})
				t.True(k >= 0, k)

				t.Equal(allValues[k].Balance, av.Balance)
			}

			return nil
		}

		addKeys, doneKeys := util.SortCompactAppendSlice[string](len(allKeys), func(a, b string) bool {
			return strings.Compare(a, b) < 0
		})
		addMergedOps, doneMergedOps := util.SortCompactAppendSlice[uint64](len(expectedOps), func(a, b uint64) bool {
			return a < b
		})

		workerKeysFuncs[i] = doneKeys
		workerMergedOpsFuncs[i] = doneMergedOps

		workers[i].args.SaveState = func(_ context.Context, key string, value base.StateValue, ops []uint64) error {
			addKeys(key)
			for j := range ops {
				addMergedOps(ops[j])
			}

			return nil
		}
	}

	t.NoError(done())

	for i := range workers {
		worker := workers[i]

		mergedKeys := workerKeysFuncs[i]()
		t.Equal(allKeys, mergedKeys)
		t.Equal(len(allKeys), worker.mergers.Len())
		t.Equal(len(mergedKeys), worker.mergers.Len())

		t.Equal(expectedOps, workerMergedOpsFuncs[i]())
		t.Equal(len(pubs), worker.mergers.Len())
	}
}

func TestDistributedStatesMerger(t *testing.T) {
	defer goleak.VerifyNone(t)

	suite.Run(t, new(testDistributedStatesMerger))
}
