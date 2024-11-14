package main

import (
	"context"
	"slices"
	"sync/atomic"
	"testing"

	"github.com/pkg/errors"
	"github.com/spikeekips/mitum/base"
	"github.com/spikeekips/mitum/util"
	"github.com/spikeekips/mitum/util/logging"
	"github.com/stretchr/testify/suite"
	"go.uber.org/goleak"
)

type testWorkerPhaseMerger struct {
	suite.Suite
	baseTestMergeOp
	baseTestWorkers
}

func (t *testWorkerPhaseMerger) newFuncs() (
	vp util.LockedMap[uint64, []StateMergeValue],
	mergers util.LockedMap[string, StateMerger],
	states util.LockedMap[string, base.State],
	_ mergerGetStateMergeValuesFunc,
	getState getStateFunc,
	newPhaseMergerf newPhaseMergerFunc,
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

	newPhaseMergerf = func(
		ctx context.Context,
		getStateMergeValues mergerGetStateMergeValuesFunc,
		getStateMergers mergerGetStateMergersFunc,
	) (
		merge,
		unmerge phaseMergerXMergeFunc,
		done func(),
		wait phaseMergerWaitFunc,
		_ error,
	) {
		return newFuncPhaseMerger(
			ctx,
			newFuncNewJobWorkerFunc(1<<9),
			1<<9,
			getStateMergeValues,
			getStateMergers,
		)
	}

	return vp, mergers, states, getStateMergeValues, getState, newPhaseMergerf, getStateMergers, traverseStateMergers
}

func (t *testWorkerPhaseMerger) closeStateMergers(
	traverseStateMergers traverseStateMergersFunc,
	f func(stateKey string, _ *AccountStateValue) bool,
) error {
	return traverseStateMergers(func(stateKey string, merger StateMerger) (bool, error) {
		var st base.StateValue

		switch s, err := merger.Close(); {
		case err != nil:
			return false, err
		case s == nil:
			return false, errors.Errorf("nil State")
		default:
			st = s
		}

		switch v, err := util.AssertInterfaceValue[*AccountStateValue](st); {
		case err != nil:
			return false, err
		default:
			return f(stateKey, v), nil
		}
	})
}

func (t *testWorkerPhaseMerger) TestCancel() {
	getStateMergeValues := func(uint64) ([]StateMergeValue, error) {
		return t.randomMergeValues(2), nil
	}

	t.Run("before new", func() {
		_, _, _, _, getState, newPhaseMerger, getStateMergers, _ := t.newFuncs()

		ctx, cancel := context.WithCancel(context.Background())

		cancel()

		insert, wait, err := newFuncWorkerPhaseMerger(
			ctx,
			getState,
			getStateMergeValues,
			getStateMergers,
			newFuncCheckEnoughFailedWorkers,
			newPhaseMerger,
			1, 0,
		)
		t.NoError(err)

		err = insert("w0", func(f workerPhaseMergerWorkerInsertValuesFunc) error {
			return f(0, t.randomMergeValues(1))
		})
		t.ErrorIs(err, context.Canceled)

		err = wait(nil)
		t.ErrorIs(err, context.Canceled)
	})

	t.Run("after new", func() {
		_, _, _, _, getState, newPhaseMerger, getStateMergers, _ := t.newFuncs()

		ctx, cancel := context.WithCancel(context.Background())

		insert, wait, err := newFuncWorkerPhaseMerger(
			ctx,
			getState,
			getStateMergeValues,
			getStateMergers,
			newFuncCheckEnoughFailedWorkers,
			newPhaseMerger,
			1, 0,
		)
		t.NoError(err)

		cancel()

		err = insert("w0", func(f workerPhaseMergerWorkerInsertValuesFunc) error {
			return f(0, t.randomMergeValues(1))
		})
		t.ErrorIs(err, context.Canceled)

		err = wait(nil)
		t.ErrorIs(err, context.Canceled)
	})

	t.Run("after insert", func() {
		_, _, _, _, getState, newPhaseMerger, getStateMergers, _ := t.newFuncs()

		ctx, cancel := context.WithCancel(context.Background())

		insert, wait, err := newFuncWorkerPhaseMerger(
			ctx,
			getState,
			getStateMergeValues,
			getStateMergers,
			newFuncCheckEnoughFailedWorkers,
			newPhaseMerger,
			1, 0,
		)
		t.NoError(err)

		t.NoError(insert("w0", func(f workerPhaseMergerWorkerInsertValuesFunc) error {
			return f(0, t.randomMergeValues(1))
		}))

		cancel()

		err = wait(nil)
		t.ErrorIs(err, context.Canceled)
	})

	t.Run("during insert", func() {
		_, _, _, _, getState, newPhaseMerger, getStateMergers, _ := t.newFuncs()

		ctx, cancel := context.WithCancel(context.Background())

		workers := t.workers(2)

		insert, wait, err := newFuncWorkerPhaseMerger(
			ctx,
			getState,
			getStateMergeValues,
			getStateMergers,
			newFuncCheckEnoughFailedWorkers,
			newPhaseMerger,
			2, 0,
		)
		t.NoError(err)

		traverseSlice(workers, func(_ int, worker string) (bool, error) {
			err := insert(worker, func(f workerPhaseMergerWorkerInsertValuesFunc) error {
				for i := range 3 {
					if worker == workers[1] && i == 1 {
						cancel()
					}

					err := f(uint64(i), t.randomMergeValues(2))

					t.T().Logf("worker insert error: worker=%q opindex=%d err=%v", worker, i, err)
				}

				return nil
			})
			t.T().Logf("worker error: worker=%q err=%v", worker, err)

			return true, nil
		})

		err = wait(nil)
		t.ErrorIs(err, context.Canceled)
	})
}

func (t *testWorkerPhaseMerger) TestEnoughFailed() {
	t.Run("1 worker; enough", func() {
		vp, _, _, getStateMergeValues, getState, newPhaseMerger, getStateMergers, traverseStateMergers := t.newFuncs()

		values := make([][]StateMergeValue, 33)
		var expectedStateKeys []string

		for i := range values {
			vs := t.randomMergeValues(3, 3, 3)
			values[i] = vs

			vp.SetValue(uint64(i), vs)

			traverseSlice(vs, func(_ int, v StateMergeValue) (bool, error) {
				a, err := util.AssertInterfaceValue[*AccountStateMergeValue](v)
				if err != nil {
					return false, err
				}

				expectedStateKeys = append(expectedStateKeys, AccountStateKey(a.Address))

				return true, nil
			})
		}
		slices.Sort(expectedStateKeys)

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		ctx = setContextValueLogger(ctx, logging.TestNilLogging.Log())

		insert, wait, err := newFuncWorkerPhaseMerger(
			ctx,
			getState,
			getStateMergeValues,
			getStateMergers,
			newFuncCheckEnoughFailedWorkers,
			newPhaseMerger,
			1, 0,
		)
		t.NoError(err)

		go func() {
			err := insert("w0", func(f workerPhaseMergerWorkerInsertValuesFunc) error {
				var gerr error

				vp.Traverse(func(opindex uint64, vs []StateMergeValue) bool {
					gerr = f(opindex, vs)

					return gerr == nil
				})

				return gerr
			})
			if err != nil {
				panic(err)
			}
		}()

		t.NoError(wait(func(_ context.Context, unmerged []uint64, reasons map[uint64]string) error {
			t.Empty(unmerged)
			t.Empty(reasons)

			return nil
		}))

		{
			t.T().Log("check mergers")

			var stateKeys []string

			t.NoError(t.closeStateMergers(traverseStateMergers, func(stateKey string, v *AccountStateValue) bool {
				stateKeys = append(stateKeys, stateKey)

				t.Equal(int64(3), v.Balance)

				return true
			}))

			slices.Sort(stateKeys)

			t.Equal(expectedStateKeys, stateKeys)
		}
	})

	t.Run("3 worker; 1 failed", func() {
		workers := t.workers(3)
		valuesPerWorker := 11
		failedWorker := workers[1]

		vp, _, _, getStateMergeValues, getState, newPhaseMerger, getStateMergers, _ := t.newFuncs()

		values := make([][]StateMergeValue, valuesPerWorker*len(workers))

		for i := range values {
			vs := t.randomMergeValues(3, 3, 3)
			values[i] = vs

			vp.SetValue(uint64(i), vs)
		}

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		ctx = setContextValueLogger(ctx, logging.TestNilLogging.Log())

		insert, wait, err := newFuncWorkerPhaseMerger(
			ctx,
			getState,
			getStateMergeValues,
			getStateMergers,
			newFuncCheckEnoughFailedWorkers,
			newPhaseMerger,
			3, 0,
		)
		t.NoError(err)

		t.T().Log("insert values")
		go func() {
			_ = traverseSlice(workers, func(wi int, worker string) (bool, error) {
				_ = insert(worker, func(f workerPhaseMergerWorkerInsertValuesFunc) error {
					for i := valuesPerWorker * wi; i < valuesPerWorker*(wi+1); i++ {
						if err := f(uint64(i), values[i]); err != nil {
							return err
						}
					}

					if worker == failedWorker {
						return errors.Errorf("findme")
					}

					return nil
				})

				return true, nil
			})
		}()

		t.T().Log("wait")
		{
			var called int64

			err = wait(func(context.Context, []uint64, map[uint64]string) error {
				atomic.AddInt64(&called, 1)

				return nil
			})
			t.ErrorContains(err, "findme")

			t.Equal(int64(0), atomic.LoadInt64(&called))
		}
	})

	t.Run("3 worker; enough", func() {
		workers := t.workers(3)
		valuesPerWorker := 11

		vp, _, _, getStateMergeValues, getState, newPhaseMerger, getStateMergers, traverseStateMergers := t.newFuncs()

		values := make([][]StateMergeValue, valuesPerWorker*len(workers))
		var expectedStateKeys []string

		for i := range values {
			vs := t.randomMergeValues(3, 3, 3)
			values[i] = vs

			vp.SetValue(uint64(i), vs)

			traverseSlice(vs, func(_ int, v StateMergeValue) (bool, error) {
				a, err := util.AssertInterfaceValue[*AccountStateMergeValue](v)
				if err != nil {
					return false, err
				}

				expectedStateKeys = append(expectedStateKeys, AccountStateKey(a.Address))

				return true, nil
			})
		}
		slices.Sort(expectedStateKeys)

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		ctx = setContextValueLogger(ctx, logging.TestNilLogging.Log())

		insert, wait, err := newFuncWorkerPhaseMerger(
			ctx,
			getState,
			getStateMergeValues,
			getStateMergers,
			newFuncCheckEnoughFailedWorkers,
			newPhaseMerger,
			3, 0,
		)
		t.NoError(err)

		go func() {
			err := traverseSlice(workers, func(wi int, worker string) (bool, error) {
				return true, insert(worker, func(f workerPhaseMergerWorkerInsertValuesFunc) error {
					for i := valuesPerWorker * wi; i < valuesPerWorker*(wi+1); i++ {
						if err := f(uint64(i), values[i]); err != nil {
							return err
						}
					}

					return nil
				})
			})
			if err != nil {
				panic(err)
			}
		}()

		t.NoError(wait(func(_ context.Context, unmerged []uint64, reasons map[uint64]string) error {
			t.Empty(unmerged)
			t.Empty(reasons)

			return nil
		}))

		{
			t.T().Log("check mergers")

			var stateKeys []string

			t.NoError(t.closeStateMergers(traverseStateMergers, func(stateKey string, v *AccountStateValue) bool {
				stateKeys = append(stateKeys, stateKey)

				t.Equal(int64(3), v.Balance)

				return true
			}))

			slices.Sort(stateKeys)

			t.Equal(expectedStateKeys, stateKeys)
		}
	})
}

func TestWorkerPhaseMerger(t *testing.T) {
	defer goleak.VerifyNone(t)

	suite.Run(t, new(testWorkerPhaseMerger))
}
