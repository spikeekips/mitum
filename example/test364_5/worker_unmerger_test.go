package main

import (
	"context"
	"fmt"
	"math"
	"slices"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/pkg/errors"
	"github.com/spikeekips/mitum/base"
	"github.com/spikeekips/mitum/util"
	"github.com/spikeekips/mitum/util/logging"
	"github.com/stretchr/testify/suite"
	"go.uber.org/goleak"
)

type testWorkerUnmerger struct {
	suite.Suite
	baseTestMergeOp
	baseTestWorkers
}

func (t *testWorkerUnmerger) newFuncs() (
	vp util.LockedMap[uint64, []StateMergeValue],
	mergers util.LockedMap[string, StateMerger],
	states util.LockedMap[string, base.State],
	_ opindexGetStateMergeValuesFunc,
	getState getStateFunc,
	getStateMergers mergerGetStateMergersFunc,
	traverseStateMergers traverseStateMergersFunc,
) {
	t.T().Helper()

	vp, mergers, states, _, getStateMergeValues := t.baseTestMergeOp.getFuncs()

	getState = func(stateKey string) (base.State, bool, error) {
		i, found := states.Value(stateKey)

		return i, found, nil
	}

	extractKeys, _ := t.extractStateMergeValueKeys()

	switch g, t, _, err := newFuncStateMergers(
		1<<9,
		getState,
		extractKeys,
	); {
	case err != nil:
		panic(err)
	default:
		getStateMergers = g
		traverseStateMergers = t
	}

	return vp, mergers, states, getStateMergeValues, getState, getStateMergers, traverseStateMergers
}

func (t *testWorkerUnmerger) randomGetStateMergeValues(uint64) ([]StateMergeValue, error) {
	return t.randomMergeValues(2), nil
}

func (t *testWorkerUnmerger) TestCancel() {
	getStateMergeValues := func(uint64) ([]StateMergeValue, error) {
		return t.randomMergeValues(2), nil
	}

	t.Run("cancel before new", func() {
		local := "w0"

		_, _, _, _, _, getStateMergers, _ := t.newFuncs()

		ctx, cancel := context.WithCancel(context.Background())
		ctx = setContextValueLogger(ctx, logging.TestNilLogging.Log())

		cancel()

		workerUnmerge, localReady, wait, err := newFuncWorkerUnmerger(
			ctx,
			t.toCtxCancel(cancel),
			local,
			newFuncNewJobWorkerFunc(1<<9),
			1<<9,
			getStateMergeValues,
			getStateMergers,
			newFuncLocalCheckEnoughFailedWorkers(local),
		)
		t.NoError(err)

		err = workerUnmerge("w1", nil)
		t.ErrorIs(err, context.Canceled)

		err = localReady([]string{local}, nil, 1, 0)
		t.ErrorIs(err, context.Canceled)

		err = wait(nil)
		t.ErrorIs(err, context.Canceled)
	})

	t.Run("cancel during worker phase done", func() {
		workers := t.workers(3)
		local := workers[0]
		failedWorker := workers[1]

		_, _, _, _, _, getStateMergers, _ := t.newFuncs()

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		workerUnmerge, localReady, wait, err := newFuncWorkerUnmerger(
			ctx,
			t.toCtxCancel(cancel),
			local,
			newFuncNewJobWorkerFunc(1<<9),
			1<<9,
			getStateMergeValues,
			getStateMergers,
			newFuncLocalCheckEnoughFailedWorkers(local),
		)
		t.NoError(err)

		go func() {
			traverseSlice(workers, func(_ int, worker string) (bool, error) {
				if worker == local {
					return true, nil
				}

				err := workerUnmerge(worker, nil)
				t.println("worker phase done: worker=%q err=%v", worker, err)

				if slices.Index(workers, worker) > slices.Index(workers, failedWorker) {
					if !errors.Is(err, context.Canceled) {
						panic(errors.Errorf("unexpected error, %+v", err))
					}
				}

				if worker == failedWorker {
					cancel()
				}

				return true, nil
			})
		}()

		err = localReady(workers, nil, uint64(len(workers)), 0)
		if err != nil {
			t.ErrorIs(err, context.Canceled)
		}

		err = wait(nil)
		t.ErrorIs(err, context.Canceled)
	})

	t.Run("cancel before wait", func() {
		workers := t.workers(3)
		local := workers[0]

		_, _, _, _, _, getStateMergers, _ := t.newFuncs()

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		workerUnmerge, localReady, wait, err := newFuncWorkerUnmerger(
			ctx,
			t.toCtxCancel(cancel),
			local,
			newFuncNewJobWorkerFunc(1<<9),
			1<<9,
			getStateMergeValues,
			getStateMergers,
			newFuncLocalCheckEnoughFailedWorkers(local),
		)
		t.NoError(err)

		donech := make(chan struct{}, 1)
		go func() {
			traverseSlice(workers, func(_ int, worker string) (bool, error) {
				var err error

				if worker == local {
					err = localReady(workers, nil, uint64(len(workers)), 0)
				} else {
					err = workerUnmerge(worker, nil)
				}

				t.println("worker phase done: worker=%q err=%v", worker, err)

				return true, nil
			})

			cancel()
			donech <- struct{}{}
		}()

		<-donech
		err = wait(nil)
		t.ErrorIs(err, context.Canceled)
	})
}

func (t *testWorkerUnmerger) TestWorkerUnmerge() {
	t.Run("worker unmerge, but local", func() {
		local := "w0"
		_, _, _, _, _, getStateMergers, _ := t.newFuncs()

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		workerUnmerge, localReady, wait, err := newFuncWorkerUnmerger(
			ctx,
			t.toCtxCancel(cancel),
			local,
			newFuncNewJobWorkerFunc(1<<9),
			1<<9,
			t.randomGetStateMergeValues,
			getStateMergers,
			newFuncLocalCheckEnoughFailedWorkers(local),
		)
		t.NoError(err)

		err = workerUnmerge(local, []uint64{0, 1, 2, 3, 4})
		t.ErrorIs(err, util.ErrFound)

		t.NoError(localReady([]string{local}, nil, 1, 0))

		var called int64

		t.NoError(wait(func(unmerged []uint64) error {
			atomic.AddInt64(&called, 1)

			t.T().Logf("wait: unmerged=%v", unmerged)

			t.Empty(unmerged)

			return nil
		}))

		t.Equal(int64(1), atomic.LoadInt64(&called))
	})

	t.Run("local: empty unmerged and 1 min enough", func() {
		local := "w0"
		_, _, _, _, _, getStateMergers, _ := t.newFuncs()

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		_, localReady, wait, err := newFuncWorkerUnmerger(
			ctx,
			t.toCtxCancel(cancel),
			local,
			newFuncNewJobWorkerFunc(1<<9),
			1<<9,
			t.randomGetStateMergeValues,
			getStateMergers,
			newFuncLocalCheckEnoughFailedWorkers(local),
		)
		t.NoError(err)

		go func() {
			err := localReady([]string{local}, nil, 1, 0)
			t.println("local phase done: worker=%q err=%v", local, err)
		}()

		var called int64

		t.NoError(wait(func(unmerged []uint64) error {
			atomic.AddInt64(&called, 1)

			return nil
		}))

		t.Equal(int64(1), atomic.LoadInt64(&called))
	})

	t.Run("local: unmerged and 1 min enough", func() {
		local := "w0"
		_, _, _, _, _, getStateMergers, _ := t.newFuncs()

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		workerUnmerge, localReady, wait, err := newFuncWorkerUnmerger(
			ctx,
			t.toCtxCancel(cancel),
			local,
			newFuncNewJobWorkerFunc(1<<9),
			1<<9,
			t.randomGetStateMergeValues,
			getStateMergers,
			newFuncLocalCheckEnoughFailedWorkers(local),
		)
		t.NoError(err)

		go func() {
			err := workerUnmerge("w1", []uint64{0, 1, 2, 3, 4})
			t.println("worker phase done: worker=%q err=%v", local, err)

			err = localReady([]string{local, "w1"}, nil, 1, 0)
			t.println("local phase done: worker=%q err=%v", local, err)
		}()

		err = wait(func([]uint64) error { return nil })
		t.ErrorContains(err, "unmerged found in pool")
	})

	t.Run("local: ok", func() {
		local := "w0"
		_, _, _, _, _, getStateMergers, _ := t.newFuncs()

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		_, localReady, wait, err := newFuncWorkerUnmerger(
			ctx,
			t.toCtxCancel(cancel),
			local,
			newFuncNewJobWorkerFunc(1<<9),
			1<<9,
			t.randomGetStateMergeValues,
			getStateMergers,
			newFuncLocalCheckEnoughFailedWorkers(local),
		)
		t.NoError(err)

		go func() {
			err := localReady([]string{local}, nil, 1, 0)
			t.println("local phase done: worker=%q err=%v", local, err)
		}()

		t.NoError(wait(func(unmerged []uint64) error {
			t.T().Logf("wait: unmerged=%v", unmerged)

			t.Empty(unmerged)

			return nil
		}))
	})

	t.Run("local: enough", func() {
		local := "w0"
		_, _, _, _, _, getStateMergers, _ := t.newFuncs()

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		workerUnmerge, localReady, wait, err := newFuncWorkerUnmerger(
			ctx,
			t.toCtxCancel(cancel),
			local,
			newFuncNewJobWorkerFunc(1<<9),
			1<<9,
			t.randomGetStateMergeValues,
			getStateMergers,
			newFuncLocalCheckEnoughFailedWorkers(local),
		)
		t.NoError(err)

		go func() {
			err := workerUnmerge("w1", []uint64{0, 1, 2, 3, 4})
			t.println(`worker phase done: worker="w1" err=%v`, err)

			err = localReady([]string{local}, []uint64{0, 1, 2, 3, 4}, 1, 0)
			t.println("local phase done: worker=%q err=%v", local, err)
		}()

		t.NoError(wait(func(unmerged []uint64) error {
			t.T().Logf("wait: unmerged=%v", unmerged)

			t.Empty(unmerged)

			return nil
		}))
	})

	t.Run("3 workers: empty", func() {
		workers := t.workers(3)
		local := workers[2]
		_, _, _, _, _, getStateMergers, _ := t.newFuncs()

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		ctx = setContextValueLogger(ctx, logging.TestNilLogging.Log())

		workerUnmerge, localReady, wait, err := newFuncWorkerUnmerger(
			ctx,
			t.toCtxCancel(cancel),
			local,
			newFuncNewJobWorkerFunc(1<<9),
			1<<9,
			t.randomGetStateMergeValues,
			getStateMergers,
			newFuncLocalCheckEnoughFailedWorkers(local),
		)
		t.NoError(err)

		go func() {
			traverseSlice(workers, func(wi int, worker string) (bool, error) {
				if worker == local {
					return true, nil
				}

				err := workerUnmerge(worker, nil)
				t.println("worker phase done: worker=%q err=%v", worker, err)

				return true, nil
			})

			err := localReady(workers, nil, uint64(len(workers)), 0)

			t.println("local phase done: worker=%q err=%v", local, err)
		}()

		t.NoError(wait(func(unmerged []uint64) error {
			t.T().Logf("wait: unmerged=%v", unmerged)

			t.Empty(unmerged)

			return nil
		}))
	})

	t.Run("3 workers: unknown worker unmerge", func() {
		workers := t.workers(3)
		local := workers[2]
		unknowns := []string{"u0", "u1", "u2", "u3"}

		_, _, _, _, _, getStateMergers, _ := t.newFuncs()

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		ctx = setContextValueLogger(ctx, logging.TestNilLogging.Log())

		workerUnmerge, localReady, wait, err := newFuncWorkerUnmerger(
			ctx,
			t.toCtxCancel(cancel),
			local,
			newFuncNewJobWorkerFunc(1<<9),
			1<<9,
			t.randomGetStateMergeValues,
			getStateMergers,
			newFuncLocalCheckEnoughFailedWorkers(local),
		)
		t.NoError(err)

		opsPerWorker := 3
		ops := make([]uint64, len(workers)*opsPerWorker)
		workerOps := map[string][]uint64{}

		for i := range ops {
			ops[i] = uint64(i)
		}

		var expectedOps []uint64

		traverseSlice(workers, func(wi int, worker string) (bool, error) {
			wops := ops[wi*opsPerWorker : (wi+1)*opsPerWorker]

			if worker != local {
				expectedOps = append(expectedOps, wops...)
			}

			workerOps[worker] = wops

			return true, nil
		})

		traverseSlice(unknowns, func(_ int, worker string) (bool, error) {
			err := workerUnmerge(worker, nil)
			t.println("unknown worker phase done: worker=%q err=%v", worker, err)

			return true, nil
		})

		go func() {
			traverseSlice(workers, func(_ int, worker string) (bool, error) {
				if worker == local {
					return true, nil
				}

				wops := workerOps[worker]

				err := workerUnmerge(worker, wops)
				t.println("worker phase done: worker=%q ops=%v err=%v", worker, wops, err)

				return true, nil
			})

			err := localReady(workers, nil, uint64(len(workers)), 0)

			t.println("local phase done: worker=%q err=%v", local, err)
		}()

		t.NoError(wait(func(unmerged []uint64) error {
			slices.Sort(unmerged)

			t.T().Logf("wait: unmerged=%v", unmerged)

			t.Equal(expectedOps, unmerged)

			return nil
		}))
	})

	t.Run("3 workers: ok", func() {
		workers := t.workers(3)
		local := workers[2]
		_, _, _, _, _, getStateMergers, _ := t.newFuncs()

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		ctx = setContextValueLogger(ctx, logging.TestNilLogging.Log())

		workerUnmerge, localReady, wait, err := newFuncWorkerUnmerger(
			ctx,
			t.toCtxCancel(cancel),
			local,
			newFuncNewJobWorkerFunc(1<<9),
			1<<9,
			t.randomGetStateMergeValues,
			getStateMergers,
			newFuncLocalCheckEnoughFailedWorkers(local),
		)
		t.NoError(err)

		opsPerWorker := 3
		ops := make([]uint64, len(workers)*opsPerWorker)
		workerOps := map[string][]uint64{}

		for i := range ops {
			ops[i] = uint64(i)
		}

		var expectedOps []uint64

		traverseSlice(workers, func(wi int, worker string) (bool, error) {
			wops := ops[wi*opsPerWorker : (wi+1)*opsPerWorker]

			if worker != local {
				expectedOps = append(expectedOps, wops...)
			}

			workerOps[worker] = wops

			return true, nil
		})

		go func() {
			traverseSlice(workers, func(wi int, worker string) (bool, error) {
				if worker == local {
					return true, nil
				}

				wops := workerOps[worker]

				err := workerUnmerge(worker, wops)
				t.println("worker phase done: worker=%q ops=%v err=%v", worker, wops, err)

				return true, nil
			})

			err := localReady(workers, nil, uint64(len(workers)), 0)

			t.println("local phase done: worker=%q err=%v", local, err)
		}()

		t.NoError(wait(func(unmerged []uint64) error {
			slices.Sort(unmerged)

			t.T().Logf("wait: unmerged=%v", unmerged)

			t.Equal(expectedOps, unmerged)

			return nil
		}))
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

		traverseSlice(workers, func(wi int, worker string) (bool, error) {
			wops := ops[wi*opsPerWorker : (wi+1)*opsPerWorker]
			workerOps[worker] = wops

			t.T().Logf("worker ops: worker=%q ops=%v", worker, wops)

			return true, nil
		})

		getStateMergeValues := func(opindex uint64) ([]StateMergeValue, error) {
			wi := int(math.Floor(float64(opindex) / float64(opsPerWorker)))

			if slices.Index(failedWorkers, workers[wi]) >= 0 {
				return nil, errors.Errorf("findme")
			}

			return t.randomMergeValues(2), nil
		}

		do := func(
			ctx context.Context,
			cancel context.CancelFunc,
			minEnough, maxFailed uint64,
		) inErrorFunc[unmergedFunc] {
			_, _, _, _, _, getStateMergers, _ := t.newFuncs()

			workerUnmerge, localReady, wait, err := newFuncWorkerUnmerger(
				ctx,
				t.toCtxCancel(cancel),
				local,
				newFuncNewJobWorkerFunc(1<<9),
				1<<9,
				getStateMergeValues,
				getStateMergers,
				newFuncLocalCheckEnoughFailedWorkers(local),
			)
			t.NoError(err)

			{
				err := localReady(workers, nil, minEnough, maxFailed)
				t.println("local phase done: worker=%q err=%v", local, err)
			}

			traverseMap(workerOps, func(worker string, wops []uint64) (bool, error) {
				if worker == local {
					return true, nil
				}

				go func() {
					err := workerUnmerge(worker, wops)
					t.println("worker phase done: worker=%q ops=%v err=%v", worker, wops, err)

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

				return true, nil
			})

			return wait
		}

		t.Run("under max failed", func() {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()

			minEnough := uint64(len(workers) - len(failedWorkers))
			maxFailed := uint64(len(failedWorkers) + 1)

			wait := do(ctx, cancel, minEnough, maxFailed)

			t.NoError(wait(func(unmerged []uint64) error {
				slices.Sort(unmerged)

				t.T().Logf("wait: unmerged=%v", unmerged)

				expected := int(minEnough-1) * opsPerWorker
				t.True(len(unmerged) >= expected, "unmerged=%d >= expected=%d", len(unmerged), expected)

				return nil
			}))
		})

		t.Run("over max failed", func() {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()

			minEnough := uint64(len(workers) - len(failedWorkers) + 1)
			maxFailed := uint64(len(failedWorkers))

			wait := do(ctx, cancel, minEnough, maxFailed)

			var called int64

			err := wait(func(unmerged []uint64) error {
				atomic.AddInt64(&called, 1)

				return nil
			})
			t.ErrorContains(err, "findme")
			t.True(atomic.LoadInt64(&called) < 1)
		})
	})

	t.Run("local outside sub workers", func() {
		local := "local"
		worker := "w0"
		workers := []string{worker}

		_, _, _, _, _, getStateMergers, _ := t.newFuncs()

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		ctx = setContextValueLogger(ctx, logging.TestNilLogging.Log())

		workerUnmerge, localReady, wait, err := newFuncWorkerUnmerger(
			ctx,
			t.toCtxCancel(cancel),
			local,
			newFuncNewJobWorkerFunc(1<<9),
			1<<9,
			t.randomGetStateMergeValues,
			getStateMergers,
			newFuncLocalCheckEnoughFailedWorkers(local),
		)
		t.NoError(err)

		ops := make([]uint64, 3)

		for i := range ops {
			ops[i] = uint64(i)
		}

		go func() {
			err := workerUnmerge(worker, ops)
			t.println("worker phase done: worker=%q ops=%v err=%v", worker, ops, err)

			err = localReady(workers, nil, uint64(len(workers)), 0)

			t.println("local phase done: worker=%q err=%v", local, err)
		}()

		t.NoError(wait(func(unmerged []uint64) error {
			slices.Sort(unmerged)

			t.T().Logf("wait: unmerged=%v", unmerged)

			t.Equal(ops, unmerged)

			return nil
		}))
	})

	t.Run("local outside sub workers; not unmerge by worker", func() {
		local := "local"
		worker := "w0"
		workers := []string{worker}

		_, _, _, _, _, getStateMergers, _ := t.newFuncs()

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		ctx = setContextValueLogger(ctx, logging.TestNilLogging.Log())

		_, localReady, wait, err := newFuncWorkerUnmerger(
			ctx,
			t.toCtxCancel(cancel),
			local,
			newFuncNewJobWorkerFunc(1<<9),
			1<<9,
			t.randomGetStateMergeValues,
			getStateMergers,
			newFuncLocalCheckEnoughFailedWorkers(local),
		)
		t.NoError(err)

		ops := make([]uint64, 3)

		for i := range ops {
			ops[i] = uint64(i)
		}

		go func() {
			err = localReady(workers, nil, uint64(len(workers)), 0)

			t.println("local phase done: worker=%q err=%v", local, err)
		}()

		donech := make(chan error, 1)

		go func() {
			err := wait(func(unmerged []uint64) error {
				slices.Sort(unmerged)

				t.T().Logf("wait: unmerged=%v", unmerged)

				t.Equal(ops, unmerged)

				return nil
			})

			t.println("wait: err=%v", err)

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
		_, _, _, _, _, getStateMergers, _ := t.newFuncs()

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		workerUnmerge, localReady, wait, err := newFuncWorkerUnmerger(
			ctx,
			t.toCtxCancel(cancel),
			local,
			newFuncNewJobWorkerFunc(1<<9),
			1<<9,
			t.randomGetStateMergeValues,
			getStateMergers,
			newFuncLocalCheckEnoughFailedWorkers(local),
		)
		t.NoError(err)

		go func() {
			err := localReady([]string{local}, []uint64{0, 1, 2, 3, 4}, 1, 0)
			t.println("local phase done: worker=%q err=%v", local, err)

			err = workerUnmerge(managerName, []uint64{0, 1, 2, 3, 4})
			t.println("worker phase done: worker=%q err=%v", managerName, err)
		}()

		t.NoError(wait(func(unmerged []uint64) error {
			t.T().Logf("wait: unmerged=%v", unmerged)

			t.Empty(unmerged)

			return nil
		}))
	})

	t.Run("manager before local enough", func() {
		local := "w0"
		_, _, _, _, _, getStateMergers, _ := t.newFuncs()

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		workerUnmerge, _, wait, err := newFuncWorkerUnmerger(
			ctx,
			t.toCtxCancel(cancel),
			local,
			newFuncNewJobWorkerFunc(1<<9),
			1<<9,
			t.randomGetStateMergeValues,
			getStateMergers,
			newFuncLocalCheckEnoughFailedWorkers(local),
		)
		t.NoError(err)

		go func() {
			err = workerUnmerge(managerName, []uint64{0, 1, 2, 3, 4})
			t.println("worker phase done: worker=%q err=%v", managerName, err)
		}()

		t.NoError(wait(func(unmerged []uint64) error {
			t.T().Logf("wait: unmerged=%v", unmerged)

			t.Empty(unmerged)

			return nil
		}))
	})
}

func TestWorkerUnmerger(t *testing.T) {
	defer goleak.VerifyNone(t)

	suite.Run(t, new(testWorkerUnmerger))
}
