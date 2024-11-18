package main

import (
	"context"
	"fmt"
	"math"
	"slices"
	"strings"
	"sync/atomic"
	"testing"

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
	getState createStateFunc,
	getStateMergers mergerGetStateMergersFunc,
	traverseStateMergers traverseStateMergersFunc,
) {
	t.T().Helper()

	vp, mergers, states, _, getStateMergeValues := t.baseTestMergeOp.getFuncs()

	getState = func(stateKey string) (base.State, bool, error) {
		i, found := states.Value(stateKey)

		return i, found, nil
	}

	extractKeys, _ := newFuncExtractStateMergeValueKeys(1 << 9)

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

		phaseDone, setEF, wait, err := newFuncWorkerUnmerger(
			ctx,
			local,
			newFuncNewJobWorkerFunc(1<<9),
			1<<9,
			getStateMergeValues,
			getStateMergers,
			newFuncLocalCheckEnoughFailedWorkers(local),
		)
		t.NoError(err)

		err = phaseDone("w1", nil)
		t.ErrorIs(err, context.Canceled)

		err = setEF(1, 0)
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

		phaseDone, setEF, wait, err := newFuncWorkerUnmerger(
			ctx,
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

				err := phaseDone(worker, nil)
				fmt.Printf("worker phase done: worker=%q err=%v\n", worker, err)

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

		err = setEF(uint64(len(workers)), 0)
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

		phaseDone, setEF, wait, err := newFuncWorkerUnmerger(
			ctx,
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
					err = setEF(uint64(len(workers)), 0)
				} else {
					err = phaseDone(worker, nil)
				}

				fmt.Printf("worker phase done: worker=%q err=%v\n", worker, err)

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

func (t *testWorkerUnmerger) TestWorkerPhaseDone() {
	getStateMergeValues := func(uint64) ([]StateMergeValue, error) {
		return t.randomMergeValues(2), nil
	}

	t.Run("phaseDone, but local", func() {
		local := "w0"
		_, _, _, _, _, getStateMergers, _ := t.newFuncs()

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		phaseDone, setEF, wait, err := newFuncWorkerUnmerger(
			ctx,
			local,
			newFuncNewJobWorkerFunc(1<<9),
			1<<9,
			getStateMergeValues,
			getStateMergers,
			newFuncLocalCheckEnoughFailedWorkers(local),
		)
		t.NoError(err)

		err = phaseDone(local, []uint64{0, 1, 2, 3, 4})
		t.ErrorIs(err, util.ErrFound)

		t.NoError(setEF(1, 0))

		t.NoError(wait(func(unmerged []uint64) error {
			t.T().Logf("wait: unmerged=%v", unmerged)

			t.Empty(unmerged)

			return nil
		}))
	})

	t.Run("local: ok", func() {
		local := "w0"
		_, _, _, _, _, getStateMergers, _ := t.newFuncs()

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		_, setEF, wait, err := newFuncWorkerUnmerger(
			ctx,
			local,
			newFuncNewJobWorkerFunc(1<<9),
			1<<9,
			getStateMergeValues,
			getStateMergers,
			newFuncLocalCheckEnoughFailedWorkers(local),
		)
		t.NoError(err)

		go func() {
			err := setEF(1, 0)
			fmt.Printf("local phase done: worker=%q err=%v\n", local, err)
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

		phaseDone, setEF, wait, err := newFuncWorkerUnmerger(
			ctx,
			local,
			newFuncNewJobWorkerFunc(1<<9),
			1<<9,
			getStateMergeValues,
			getStateMergers,
			newFuncLocalCheckEnoughFailedWorkers(local),
		)
		t.NoError(err)

		go func() {
			err := phaseDone("w1", []uint64{0, 1, 2, 3, 4})
			fmt.Printf("worker phase done: worker=%q err=%v\n", local, err)

			err = setEF(1, 0)
			fmt.Printf("local phase done: worker=%q err=%v\n", local, err)
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

		phaseDone, setEF, wait, err := newFuncWorkerUnmerger(
			ctx,
			local,
			newFuncNewJobWorkerFunc(1<<9),
			1<<9,
			getStateMergeValues,
			getStateMergers,
			newFuncLocalCheckEnoughFailedWorkers(local),
		)
		t.NoError(err)

		go func() {
			traverseSlice(workers, func(wi int, worker string) (bool, error) {
				if worker == local {
					return true, nil
				}

				err := phaseDone(worker, nil)
				fmt.Printf("worker phase done: worker=%q err=%v\n", worker, err)

				return true, nil
			})

			err := setEF(uint64(len(workers)), 0)

			fmt.Printf("local phase done: worker=%q err=%v\n", local, err)
		}()

		t.NoError(wait(func(unmerged []uint64) error {
			t.T().Logf("wait: unmerged=%v", unmerged)

			t.Empty(unmerged)

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

		phaseDone, setEF, wait, err := newFuncWorkerUnmerger(
			ctx,
			local,
			newFuncNewJobWorkerFunc(1<<9),
			1<<9,
			getStateMergeValues,
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

				err := phaseDone(worker, wops)
				fmt.Printf("worker phase done: worker=%q ops=%v err=%v\n", worker, wops, err)

				return true, nil
			})

			err := setEF(uint64(len(workers)), 0)

			fmt.Printf("local phase done: worker=%q err=%v\n", local, err)
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
			minEnough, maxFailed uint64,
		) workerUnmergerWaitFunc {
			_, _, _, _, _, getStateMergers, _ := t.newFuncs()

			phaseDone, setEF, wait, err := newFuncWorkerUnmerger(
				ctx,
				local,
				newFuncNewJobWorkerFunc(1<<9),
				1<<9,
				getStateMergeValues,
				getStateMergers,
				newFuncLocalCheckEnoughFailedWorkers(local),
			)
			t.NoError(err)

			{
				err := setEF(minEnough, maxFailed)
				fmt.Printf("local phase done: worker=%q err=%v\n", local, err)
			}

			traverseMap(workerOps, func(worker string, wops []uint64) (bool, error) {
				if worker == local {
					return true, nil
				}

				go func() {
					err := phaseDone(worker, wops)
					fmt.Printf("worker phase done: worker=%q ops=%v err=%v\n", worker, wops, err)

					if slices.Index(failedWorkers, worker) >= 0 {
						if !strings.Contains(err.Error(), "findme") {
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

			wait := do(ctx, minEnough, maxFailed)

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

			wait := do(ctx, minEnough, maxFailed)

			var called int64

			err := wait(func(unmerged []uint64) error {
				atomic.AddInt64(&called, 1)

				return nil
			})
			t.ErrorContains(err, "findme")
			t.True(atomic.LoadInt64(&called) < 1)
		})
	})
}

func TestWorkerUnmerger(t *testing.T) {
	defer goleak.VerifyNone(t)

	suite.Run(t, new(testWorkerUnmerger))
}
