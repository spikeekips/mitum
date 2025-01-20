package main

import (
	"context"
	"fmt"
	"iter"
	"maps"
	"math"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/pkg/errors"
	"github.com/spikeekips/mitum/base"
	dutil "github.com/spikeekips/mitum/example/test364_5/util"
	"github.com/spikeekips/mitum/util"
	"github.com/spikeekips/mitum/util/logging"
	"github.com/stretchr/testify/suite"
	"go.uber.org/goleak"
)

type testWorkerUnmerger struct {
	dutil.BaseSuite
	baseTestMergeOp
	baseTestWorkers
}

func (t *testWorkerUnmerger) newFuncs() (
	vp util.LockedMap[uint64, []stateMergeValue],
	mergers util.LockedMap[string, stateValueMerger],
	states util.LockedMap[string, base.State],
	_ opindexGetStateMergeValuesFunc,
	getState getStateFunc,
	getStateValueMergers mergerGetStateValueMergersFunc,
	traverseStateValueMergers iter.Seq2[string, stateValueMerger],
) {
	t.T().Helper()

	vp, mergers, states, _, getStateMergeValues := t.baseTestMergeOp.getFuncs()

	getState = func(stateKey string) (base.State, bool, error) {
		i, found := states.Value(stateKey)

		return i, found, nil
	}

	extractKeys, _ := t.extractStateMergeValueKeys()

	switch g, t, err := newFuncStateValueMergers(
		1<<9,
		getState,
		extractKeys,
	); {
	case err != nil:
		panic(err)
	default:
		getStateValueMergers = g
		_, traverseStateValueMergers = t()
	}

	return vp, mergers, states, getStateMergeValues, getState, getStateValueMergers, traverseStateValueMergers
}

func (t *testWorkerUnmerger) randomGetStateMergeValues(uint64) ([]stateMergeValue, error) {
	return t.randomMergeValues(2), nil
}

func (t *testWorkerUnmerger) TestCancel() {
	getStateMergeValues := func(uint64) ([]stateMergeValue, error) {
		return t.randomMergeValues(2), nil
	}

	t.Run("cancel before new", func() {
		local := "w0"

		_, _, _, _, _, getStateValueMergers, _ := t.newFuncs()

		ctx, cancel := t.Ctx()
		ctx = dutil.SetContextValueLogger(ctx, logging.TestNilLogging.Log())

		cancel(nil)

		workerUnmerge, localReady, wait, err := newFuncWorkerUnmerger(
			ctx,
			cancel,
			local,
			dutil.NewFuncNewJobWorkerFunc(1<<9),
			1<<9,
			getStateMergeValues,
			getStateValueMergers,
			newFuncLocalCheckFailedWorkers(local),
		)
		t.NoError(err)

		err = workerUnmerge("w1", nil)
		t.ErrorIs(err, context.Canceled)

		err = localReady([]string{local}, nil, 1, 0)
		t.ErrorIs(err, context.Canceled)

		_, err = wait()
		t.ErrorIs(err, context.Canceled)
	})

	t.Run("cancel during worker phase done", func() {
		workers := t.workers(3)
		local := workers[0]
		failedWorker := workers[1]

		_, _, _, _, _, getStateValueMergers, _ := t.newFuncs()

		ctx, cancel := t.Ctx()

		workerUnmerge, localReady, wait, err := newFuncWorkerUnmerger(
			ctx,
			cancel,
			local,
			dutil.NewFuncNewJobWorkerFunc(1<<9),
			1<<9,
			getStateMergeValues,
			getStateValueMergers,
			newFuncLocalCheckFailedWorkers(local),
		)
		t.NoError(err)

		go func() {
			for worker := range slices.Values(workers) {
				if worker == local {
					continue
				}

				err := workerUnmerge(worker, nil)
				t.Println("worker phase done: worker=%q err=%v", worker, err)

				if slices.Index(workers, worker) > slices.Index(workers, failedWorker) {
					t.IfTruePanic(!errors.Is(err, context.Canceled), "unexpected error, %+v", err)
				}

				if worker == failedWorker {
					cancel(nil)
				}
			}
		}()

		err = localReady(workers, nil, uint64(len(workers)), 0)
		if err != nil {
			t.ErrorIs(err, context.Canceled)
		}

		_, err = wait()
		t.ErrorIs(err, context.Canceled)
	})

	t.Run("cancel before wait", func() {
		workers := t.workers(3)
		local := workers[0]

		_, _, _, _, _, getStateValueMergers, _ := t.newFuncs()

		ctx, cancel := t.Ctx()

		workerUnmerge, localReady, wait, err := newFuncWorkerUnmerger(
			ctx,
			cancel,
			local,
			dutil.NewFuncNewJobWorkerFunc(1<<9),
			1<<9,
			getStateMergeValues,
			getStateValueMergers,
			newFuncLocalCheckFailedWorkers(local),
		)
		t.NoError(err)

		donech := make(chan struct{}, 1)
		go func() {
			for worker := range slices.Values(workers) {
				var err error

				if worker == local {
					err = localReady(workers, nil, uint64(len(workers)), 0)
				} else {
					err = workerUnmerge(worker, nil)
				}

				t.Println("worker phase done: worker=%q err=%v", worker, err)
			}

			cancel(nil)
			donech <- struct{}{}
		}()

		<-donech
		_, err = wait()
		t.ErrorIs(err, context.Canceled)
	})
}

func (t *testWorkerUnmerger) TestWorkerUnmerge() {
	t.Run("worker unmerge, but local", func() {
		local := "w0"
		_, _, _, _, _, getStateValueMergers, _ := t.newFuncs()

		ctx, cancel := t.Ctx()

		workerUnmerge, localReady, wait, err := newFuncWorkerUnmerger(
			ctx,
			cancel,
			local,
			dutil.NewFuncNewJobWorkerFunc(1<<9),
			1<<9,
			t.randomGetStateMergeValues,
			getStateValueMergers,
			newFuncLocalCheckFailedWorkers(local),
		)
		t.NoError(err)

		err = workerUnmerge(local, []uint64{0, 1, 2, 3, 4})
		t.ErrorIs(err, util.ErrFound)

		t.NoError(localReady([]string{local}, nil, 1, 0))

		unmerged, err := wait()
		t.NoError(err)
		t.T().Logf("wait: unmerged=%v", unmerged)

		t.Empty(unmerged)
	})

	t.Run("local: empty unmerged and 1 min enough", func() {
		local := "w0"
		_, _, _, _, _, getStateValueMergers, _ := t.newFuncs()

		ctx, cancel := t.Ctx()

		_, localReady, wait, err := newFuncWorkerUnmerger(
			ctx,
			cancel,
			local,
			dutil.NewFuncNewJobWorkerFunc(1<<9),
			1<<9,
			t.randomGetStateMergeValues,
			getStateValueMergers,
			newFuncLocalCheckFailedWorkers(local),
		)
		t.NoError(err)

		go func() {
			err := localReady([]string{local}, nil, 1, 0)
			t.Println("local phase done: worker=%q err=%v", local, err)
		}()

		unmerged, err := wait()
		t.NoError(err)
		t.Empty(unmerged)
	})

	t.Run("local: unmerged and 1 min enough", func() {
		local := "w0"
		_, _, _, _, _, getStateValueMergers, _ := t.newFuncs()

		ctx, cancel := t.Ctx()

		workerUnmerge, localReady, wait, err := newFuncWorkerUnmerger(
			ctx,
			cancel,
			local,
			dutil.NewFuncNewJobWorkerFunc(1<<9),
			1<<9,
			t.randomGetStateMergeValues,
			getStateValueMergers,
			newFuncLocalCheckFailedWorkers(local),
		)
		t.NoError(err)

		go func() {
			err := workerUnmerge("w1", []uint64{0, 1, 2, 3, 4})
			t.Println("worker phase done: worker=%q err=%v", local, err)

			err = localReady([]string{local, "w1"}, nil, 1, 0)
			t.Println("local phase done: worker=%q err=%v", local, err)
		}()

		_, err = wait()
		t.ErrorContains(err, "unmerged found in pool")
	})

	t.Run("local: ok", func() {
		local := "w0"
		_, _, _, _, _, getStateValueMergers, _ := t.newFuncs()

		ctx, cancel := t.Ctx()

		_, localReady, wait, err := newFuncWorkerUnmerger(
			ctx,
			cancel,
			local,
			dutil.NewFuncNewJobWorkerFunc(1<<9),
			1<<9,
			t.randomGetStateMergeValues,
			getStateValueMergers,
			newFuncLocalCheckFailedWorkers(local),
		)
		t.NoError(err)

		go func() {
			err := localReady([]string{local}, nil, 1, 0)
			t.Println("local phase done: worker=%q err=%v", local, err)
		}()

		unmerged, err := wait()
		t.T().Logf("wait: unmerged=%v", unmerged)

		t.Empty(unmerged)
	})

	t.Run("local: enough", func() {
		local := "w0"
		_, _, _, _, _, getStateValueMergers, _ := t.newFuncs()

		ctx, cancel := t.Ctx()

		workerUnmerge, localReady, wait, err := newFuncWorkerUnmerger(
			ctx,
			cancel,
			local,
			dutil.NewFuncNewJobWorkerFunc(1<<9),
			1<<9,
			t.randomGetStateMergeValues,
			getStateValueMergers,
			newFuncLocalCheckFailedWorkers(local),
		)
		t.NoError(err)

		go func() {
			err := workerUnmerge("w1", []uint64{0, 1, 2, 3, 4})
			t.Println(`worker phase done: worker="w1" err=%v`, err)

			err = localReady([]string{local}, []uint64{0, 1, 2, 3, 4}, 1, 0)
			t.Println("local phase done: worker=%q err=%v", local, err)
		}()

		unmerged, err := wait()
		t.NoError(err)
		t.T().Logf("wait: unmerged=%v", unmerged)

		t.Empty(unmerged)
	})

	t.Run("3 workers: empty", func() {
		workers := t.workers(3)
		local := workers[2]
		_, _, _, _, _, getStateValueMergers, _ := t.newFuncs()

		ctx, cancel := t.Ctx()

		ctx = dutil.SetContextValueLogger(ctx, logging.TestNilLogging.Log())

		workerUnmerge, localReady, wait, err := newFuncWorkerUnmerger(
			ctx,
			cancel,
			local,
			dutil.NewFuncNewJobWorkerFunc(1<<9),
			1<<9,
			t.randomGetStateMergeValues,
			getStateValueMergers,
			newFuncLocalCheckFailedWorkers(local),
		)
		t.NoError(err)

		go func() {
			for worker := range slices.Values(workers) {
				if worker == local {
					continue
				}

				err := workerUnmerge(worker, nil)
				t.Println("worker phase done: worker=%q err=%v", worker, err)
			}

			err := localReady(workers, nil, uint64(len(workers)), 0)

			t.Println("local phase done: worker=%q err=%v", local, err)
		}()

		unmerged, err := wait()
		t.NoError(err)
		t.T().Logf("wait: unmerged=%v", unmerged)

		t.Empty(unmerged)
	})

	t.Run("3 workers: unknown worker unmerge", func() {
		workers := t.workers(3)
		local := workers[2]
		unknowns := []string{"u0", "u1", "u2", "u3"}

		_, _, _, _, _, getStateValueMergers, _ := t.newFuncs()

		ctx, cancel := t.Ctx()

		ctx = dutil.SetContextValueLogger(ctx, logging.TestNilLogging.Log())

		workerUnmerge, localReady, wait, err := newFuncWorkerUnmerger(
			ctx,
			cancel,
			local,
			dutil.NewFuncNewJobWorkerFunc(1<<9),
			1<<9,
			t.randomGetStateMergeValues,
			getStateValueMergers,
			newFuncLocalCheckFailedWorkers(local),
		)
		t.NoError(err)

		opsPerWorker := 3
		ops := make([]uint64, len(workers)*opsPerWorker)
		workerOps := map[string][]uint64{}

		for i := range ops {
			ops[i] = uint64(i)
		}

		var expectedOps []uint64

		for wi, worker := range slices.All(workers) {
			wops := ops[wi*opsPerWorker : (wi+1)*opsPerWorker]

			if worker != local {
				expectedOps = append(expectedOps, wops...)
			}

			workerOps[worker] = wops
		}

		for worker := range slices.Values(unknowns) {
			err := workerUnmerge(worker, nil)
			t.Println("unknown worker phase done: worker=%q err=%v", worker, err)
		}

		go func() {
			for worker := range slices.Values(workers) {
				if worker == local {
					continue
				}

				wops := workerOps[worker]

				err := workerUnmerge(worker, wops)
				t.Println("worker phase done: worker=%q ops=%v err=%v", worker, wops, err)
			}

			err := localReady(workers, nil, uint64(len(workers)), 0)

			t.Println("local phase done: worker=%q err=%v", local, err)
		}()

		unmerged, err := wait()
		t.NoError(err)
		slices.Sort(unmerged)

		t.T().Logf("wait: unmerged=%v", unmerged)

		t.Equal(expectedOps, unmerged)
	})

	t.Run("3 workers: ok", func() {
		workers := t.workers(3)
		local := workers[2]
		_, _, _, _, _, getStateValueMergers, _ := t.newFuncs()

		ctx, cancel := t.Ctx()

		ctx = dutil.SetContextValueLogger(ctx, logging.TestNilLogging.Log())

		workerUnmerge, localReady, wait, err := newFuncWorkerUnmerger(
			ctx,
			cancel,
			local,
			dutil.NewFuncNewJobWorkerFunc(1<<9),
			1<<9,
			t.randomGetStateMergeValues,
			getStateValueMergers,
			newFuncLocalCheckFailedWorkers(local),
		)
		t.NoError(err)

		opsPerWorker := 3
		ops := make([]uint64, len(workers)*opsPerWorker)
		workerOps := map[string][]uint64{}

		for i := range ops {
			ops[i] = uint64(i)
		}

		var expectedOps []uint64

		for wi, worker := range slices.All(workers) {
			wops := ops[wi*opsPerWorker : (wi+1)*opsPerWorker]

			if worker != local {
				expectedOps = append(expectedOps, wops...)
			}

			workerOps[worker] = wops
		}

		go func() {
			for worker := range slices.Values(workers) {
				if worker == local {
					continue
				}

				wops := workerOps[worker]

				err := workerUnmerge(worker, wops)
				t.Println("worker phase done: worker=%q ops=%v err=%v", worker, wops, err)
			}

			err := localReady(workers, nil, uint64(len(workers)), 0)

			t.Println("local phase done: worker=%q err=%v", local, err)
		}()

		unmerged, err := wait()
		t.NoError(err)
		slices.Sort(unmerged)

		t.T().Logf("wait: unmerged=%v", unmerged)

		t.Equal(expectedOps, unmerged)
	})

	t.Run("3 workers: max failed", func() {
		workers := t.workers(6)
		local := workers[4]
		failedWorkers := []string{workers[3], workers[5]}

		t.T().Logf("workers: %v local=%q failed workers=%v", workers, local, failedWorkers)

		opsPerWorker := 3
		ops := make([]uint64, len(workers)*opsPerWorker)

		for i := range ops {
			ops[i] = uint64(i)
		}

		workerOps := map[string][]uint64{}

		for wi, worker := range slices.All(workers) {
			wops := ops[wi*opsPerWorker : (wi+1)*opsPerWorker]
			workerOps[worker] = wops

			t.T().Logf("worker ops: worker=%q ops=%v", worker, wops)
		}

		getStateMergeValues := func(opindex uint64) ([]stateMergeValue, error) {
			wi := int(math.Floor(float64(opindex) / float64(opsPerWorker)))

			if slices.Index(failedWorkers, workers[wi]) >= 0 {
				return nil, errors.Errorf("findme")
			}

			return t.randomMergeValues(2), nil
		}

		do := func(
			ctx context.Context,
			cancel dutil.InErrorFunc[error],
			minEnough, maxFailed uint64,
		) workerUnmergerWaitFunc {
			_, _, _, _, _, getStateValueMergers, _ := t.newFuncs()

			workerUnmerge, localReady, wait, err := newFuncWorkerUnmerger(
				ctx,
				cancel,
				local,
				dutil.NewFuncNewJobWorkerFunc(1<<9),
				1<<9,
				getStateMergeValues,
				getStateValueMergers,
				newFuncLocalCheckFailedWorkers(local),
			)
			t.NoError(err)

			{
				err := localReady(workers, nil, minEnough, maxFailed)
				t.Println("local phase done: worker=%q err=%v", local, err)
			}

			for worker, wops := range maps.All(workerOps) {
				if worker == local {
					continue
				}

				go func() {
					err := workerUnmerge(worker, wops)
					t.Println("worker phase done: worker=%q ops=%v err=%v", worker, wops, err)

					if slices.Index(failedWorkers, worker) >= 0 {
						switch {
						case err == nil:
						case strings.Contains(err.Error(), "findme"), errors.Is(err, context.Canceled):
						default:
							panic(fmt.Sprintf(
								"unexpected error for failed worker=%q err=%+v", worker, err))
						}
					}
				}()
			}

			return wait
		}

		t.Run("under max failed", func() {
			ctx, cancel := t.Ctx()

			minEnough := uint64(len(workers) - len(failedWorkers))
			maxFailed := uint64(len(failedWorkers) + 1)

			wait := do(ctx, cancel, minEnough, maxFailed)

			unmerged, err := wait()
			t.NoError(err)
			slices.Sort(unmerged)

			t.T().Logf("wait: unmerged=%v", unmerged)

			expected := int(minEnough-1) * opsPerWorker
			t.True(len(unmerged) >= expected, "unmerged=%d >= expected=%d", len(unmerged), expected)
		})

		t.Run("over max failed", func() {
			ctx, cancel := t.Ctx()

			minEnough := uint64(len(workers) - len(failedWorkers) + 1)
			maxFailed := uint64(len(failedWorkers))

			wait := do(ctx, cancel, minEnough, maxFailed)

			_, err := wait()
			t.ErrorContains(err, "findme")
		})
	})

	t.Run("local outside sub workers", func() {
		local := "local"
		worker := "w0"
		workers := []string{worker}

		_, _, _, _, _, getStateValueMergers, _ := t.newFuncs()

		ctx, cancel := t.Ctx()

		ctx = dutil.SetContextValueLogger(ctx, logging.TestNilLogging.Log())

		workerUnmerge, localReady, wait, err := newFuncWorkerUnmerger(
			ctx,
			cancel,
			local,
			dutil.NewFuncNewJobWorkerFunc(1<<9),
			1<<9,
			t.randomGetStateMergeValues,
			getStateValueMergers,
			newFuncLocalCheckFailedWorkers(local),
		)
		t.NoError(err)

		ops := make([]uint64, 3)

		for i := range ops {
			ops[i] = uint64(i)
		}

		go func() {
			err := workerUnmerge(worker, ops)
			t.Println("worker phase done: worker=%q ops=%v err=%v", worker, ops, err)

			err = localReady(workers, nil, uint64(len(workers)), 0)

			t.Println("local phase done: worker=%q err=%v", local, err)
		}()

		unmerged, err := wait()
		t.NoError(err)
		slices.Sort(unmerged)

		t.T().Logf("wait: unmerged=%v", unmerged)

		t.Equal(ops, unmerged)
	})

	t.Run("local outside sub workers; not unmerge by worker", func() {
		local := "local"
		worker := "w0"
		workers := []string{worker}

		_, _, _, _, _, getStateValueMergers, _ := t.newFuncs()

		ctx, cancel := t.Ctx()

		ctx = dutil.SetContextValueLogger(ctx, logging.TestNilLogging.Log())

		_, localReady, wait, err := newFuncWorkerUnmerger(
			ctx,
			cancel,
			local,
			dutil.NewFuncNewJobWorkerFunc(1<<9),
			1<<9,
			t.randomGetStateMergeValues,
			getStateValueMergers,
			newFuncLocalCheckFailedWorkers(local),
		)
		t.NoError(err)

		ops := make([]uint64, 3)

		for i := range ops {
			ops[i] = uint64(i)
		}

		go func() {
			err = localReady(workers, nil, uint64(len(workers)), 0)

			t.Println("local phase done: worker=%q err=%v", local, err)
		}()

		donech := make(chan error, 1)

		go func() {
			unmerged, err := wait()
			t.Println("wait: unmerged=%v err=%v", unmerged, err)

			if err == nil {
				slices.Sort(unmerged)
				t.IfTruePanic(len(ops) != len(unmerged), "ops=%d != unmerged=%d", len(ops), len(unmerged))

				for i, v := range slices.All(unmerged) {
					if ops[i] != v {
						t.IfErrPanic(errors.Errorf("%d: ops=%d != unmerged=%d", i, ops[i], v))
					}
				}
			}

			donech <- err
		}()

		select {
		case <-time.After(time.Second * 2):
		case err := <-donech:
			t.Fail("unexpected wait done: %+v", err)
		}
	})
}

func (t *testWorkerUnmerger) TestWorkerManager() {
	t.Run("local enough and then manager", func() {
		local := "w0"
		_, _, _, _, _, getStateValueMergers, _ := t.newFuncs()

		ctx, cancel := t.Ctx()

		workerUnmerge, localReady, wait, err := newFuncWorkerUnmerger(
			ctx,
			cancel,
			local,
			dutil.NewFuncNewJobWorkerFunc(1<<9),
			1<<9,
			t.randomGetStateMergeValues,
			getStateValueMergers,
			newFuncLocalCheckFailedWorkers(local),
		)
		t.NoError(err)

		go func() {
			err := localReady([]string{local}, []uint64{0, 1, 2, 3, 4}, 1, 0)
			t.Println("local phase done: worker=%q err=%v", local, err)

			err = workerUnmerge(managerName, []uint64{0, 1, 2, 3, 4})
			t.Println("worker phase done: worker=%q err=%v", managerName, err)
		}()

		unmerged, err := wait()
		t.NoError(err)
		t.T().Logf("wait: unmerged=%v", unmerged)

		t.Empty(unmerged)
	})

	t.Run("manager before local enough", func() {
		local := "w0"
		_, _, _, _, _, getStateValueMergers, _ := t.newFuncs()

		ctx, cancel := t.Ctx()

		workerUnmerge, _, wait, err := newFuncWorkerUnmerger(
			ctx,
			cancel,
			local,
			dutil.NewFuncNewJobWorkerFunc(1<<9),
			1<<9,
			t.randomGetStateMergeValues,
			getStateValueMergers,
			newFuncLocalCheckFailedWorkers(local),
		)
		t.NoError(err)

		go func() {
			err := workerUnmerge(managerName, []uint64{0, 1, 2, 3, 4})
			t.Println("worker phase done: worker=%q err=%v", managerName, err)
		}()

		unmerged, err := wait()
		t.NoError(err)
		t.T().Logf("wait: unmerged=%v", unmerged)

		t.Empty(unmerged)
	})
}

func TestWorkerUnmerger(t *testing.T) {
	defer goleak.VerifyNone(t)

	suite.Run(t, new(testWorkerUnmerger))
}
