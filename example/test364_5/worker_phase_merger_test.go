package dop5

import (
	"context"
	"iter"
	"slices"
	"sync/atomic"
	"testing"

	"github.com/pkg/errors"
	"github.com/spikeekips/mitum/base"
	dutil "github.com/spikeekips/mitum/example/test364_5/util"
	"github.com/spikeekips/mitum/util"
	"github.com/spikeekips/mitum/util/logging"
	"github.com/stretchr/testify/suite"
	"go.uber.org/goleak"
)

type testWorkerPhaseMerger struct {
	dutil.BaseSuite
	baseTestMergeOp
	baseTestWorkers
}

func (t *testWorkerPhaseMerger) newFuncs() (
	vp dutil.LockedMap[uint64, []SMV],
	mergers dutil.LockedMap[string, SVM],
	states dutil.LockedMap[string, base.State],
	_ opindexGetSMVsFunc,
	newPhaseMergerf newPhaseMergerFunc,
	getSVMs mergerGetSVMsFunc,
	traverseSVMs iter.Seq2[string, SVM],
) {
	t.T().Helper()

	vp, mergers, states, _, getSMVs := t.baseTestMergeOp.getFuncs()

	getState := func(stateKey string) (base.State, bool, error) {
		i, found := states.Value(stateKey)

		return i, found, nil
	}

	extractKeys, _ := t.extractSMVKeys()

	getSVMs, tr, err := newFuncSVMs(
		1<<9,
		getState,
		extractKeys,
	)
	t.IfErrPanic(err)
	_, traverseSVMs = tr()

	newPhaseMergerf = func(
		ctx context.Context,
		getSMVs opindexGetSMVsFunc,
		getSVMs mergerGetSVMsFunc,
	) (
		merge,
		unmerge dutil.InErrorFunc[uint64],
		done func(),
		wait phaseMergerWaitFunc,
		_ error,
	) {
		return newFuncPhaseMerger(
			ctx,
			dutil.NewFuncNewJobWorkerFunc(1<<9),
			1<<9,
			getSMVs,
			getSVMs,
		)
	}

	return vp, mergers, states, getSMVs, newPhaseMergerf, getSVMs, traverseSVMs
}

func (t *testWorkerPhaseMerger) closeSVMs(
	traverseSVMs iter.Seq2[string, SVM],
	f func(stateKey string, _ *AccountSV) bool,
) error {
end:
	for stateKey, merger := range traverseSVMs {
		var st base.StateValue

		switch s, err := merger.Close(); {
		case err != nil:
			return err
		case s == nil:
			return errors.New("nil State")
		default:
			st = s
		}

		switch v, err := util.AssertInterfaceValue[*AccountSV](st); {
		case err != nil:
			return err
		default:
			if !f(stateKey, v) {
				break end
			}
		}
	}

	return nil
}

func (t *testWorkerPhaseMerger) TestCancel() {
	getSMVs := func(uint64) ([]SMV, error) {
		return t.randomMergeValues(2), nil
	}

	t.Run("before new", func() {
		_, _, _, _, newPhaseMerger, getSVMs, _ := t.newFuncs()

		ctx, cancel := t.Ctx()

		cancel(nil)

		insertf, wait, err := newFuncWorkerPhaseMerger(
			ctx,
			"local",
			cancel,
			getSMVs,
			getSVMs,
			newPhaseMerger,
			1, 1,
		)
		t.NoError(err)

		insert, insertDone, err := insertf("w0")
		t.NoError(err)

		err = insert(0)
		t.ErrorIs(err, context.Canceled)
		err = insertDone(nil)
		t.ErrorIs(err, context.Canceled)

		_, _, err = wait()
		t.ErrorIs(err, context.Canceled)
	})

	t.Run("after new", func() {
		_, _, _, _, newPhaseMerger, getSVMs, _ := t.newFuncs()

		ctx, cancel := t.Ctx()

		insertf, wait, err := newFuncWorkerPhaseMerger(
			ctx,
			"local",
			cancel,
			getSMVs,
			getSVMs,
			newPhaseMerger,
			1, 1,
		)
		t.NoError(err)

		cancel(nil)

		insert, insertDone, err := insertf("w0")
		t.NoError(err)
		err = insert(0)
		t.ErrorIs(err, context.Canceled)
		err = insertDone(nil)
		t.ErrorIs(err, context.Canceled)

		_, _, err = wait()
		t.ErrorIs(err, context.Canceled)
	})

	t.Run("after insert", func() {
		t.Run("after insert", func() {
			_, _, _, _, newPhaseMerger, getSVMs, _ := t.newFuncs()

			ctx, cancel := t.Ctx()

			insertf, wait, err := newFuncWorkerPhaseMerger(
				ctx,
				"local",
				cancel,
				getSMVs,
				getSVMs,
				newPhaseMerger,
				1, 1,
			)
			t.NoError(err)

			insert, insertDone, err := insertf("w0")
			t.NoError(err)

			t.NoError(insert(0))

			cancel(nil)

			err = insertDone(nil)
			t.ErrorIs(err, context.Canceled)

			_, _, err = wait()
			t.ErrorIs(err, context.Canceled)
		})

		t.Run("after insert done", func() {
			_, _, _, _, newPhaseMerger, getSVMs, _ := t.newFuncs()

			ctx, cancel := t.Ctx()

			insertf, wait, err := newFuncWorkerPhaseMerger(
				ctx,
				"local",
				cancel,
				getSMVs,
				getSVMs,
				newPhaseMerger,
				1, 1,
			)
			t.NoError(err)

			insert, insertDone, err := insertf("w0")
			t.NoError(err)

			t.NoError(insert(0))
			t.NoError(insertDone(nil))

			cancel(nil)

			_, _, err = wait()
			t.ErrorIs(err, context.Canceled)
		})
	})

	t.Run("during insert", func() {
		_, _, _, _, newPhaseMerger, getSVMs, _ := t.newFuncs()

		ctx, cancel := t.Ctx()

		workers := t.workers(2)

		insertf, wait, err := newFuncWorkerPhaseMerger(
			ctx,
			"local",
			cancel,
			getSMVs,
			getSVMs,
			newPhaseMerger,
			2, 1,
		)
		t.NoError(err)

		for worker := range slices.Values(workers) {
			insert, insertDone, err := insertf(worker)
			t.NoError(err)

			for i := range 3 {
				if worker == workers[1] && i == 1 {
					cancel(nil)
				}

				err := insert(uint64(i))

				t.T().Logf("worker insert error: worker=%q opindex=%d err=%v", worker, i, err)
			}

			err = insertDone(nil)

			t.T().Logf("worker error: worker=%q err=%v", worker, err)
		}

		_, _, err = wait()
		t.ErrorIs(err, context.Canceled)
	})
}

func (t *testWorkerPhaseMerger) TestEnoughFailed() {
	t.Run("1 worker; empty", func() {
		_, _, _, getSMVs, newPhaseMerger, getSVMs, traverseSVMs := t.newFuncs()

		ctx, cancel := t.Ctx()

		ctx = dutil.SetContextValueLogger(ctx, logging.TestNilLogging.Log())

		insertf, wait, err := newFuncWorkerPhaseMerger(
			ctx,
			"w0",
			cancel,
			getSMVs,
			getSVMs,
			newPhaseMerger,
			1, 1,
		)
		t.NoError(err)

		go func() {
			_, insertDone, err := insertf("w0")
			t.IfErrPanic(err)

			t.IfErrPanic(insertDone(nil))
		}()

		unmerged, reasons, err := wait()
		t.NoError(err)
		t.Empty(unmerged)
		t.Empty(reasons)

		{
			t.T().Log("check mergers")

			var called int64

			t.NoError(t.closeSVMs(traverseSVMs, func(stateKey string, v *AccountSV) bool {
				atomic.AddInt64(&called, 1)

				return true
			}))

			t.Equal(int64(0), atomic.LoadInt64(&called))
		}
	})

	t.Run("1 worker; enough", func() {
		vp, _, _, getSMVs, newPhaseMerger, getSVMs, traverseSVMs := t.newFuncs()

		values := make([][]SMV, 33)
		var expectedStateKeys []string

		for i := range values {
			vs := t.randomMergeValues(3, 3, 3)
			values[i] = vs

			vp.SetValue(uint64(i), vs)

			for v := range slices.Values(vs) {
				a, err := util.AssertInterfaceValue[*AccountSMV](v)
				t.NoError(err)

				expectedStateKeys = append(expectedStateKeys, accountStateKey(a.Address))
			}
		}
		slices.Sort(expectedStateKeys)

		ctx, cancel := t.Ctx()

		ctx = dutil.SetContextValueLogger(ctx, logging.TestNilLogging.Log())

		insertf, wait, err := newFuncWorkerPhaseMerger(
			ctx,
			"w0",
			cancel,
			getSMVs,
			getSVMs,
			newPhaseMerger,
			1, 1,
		)
		t.NoError(err)

		go func() {
			insert, insertDone, err := insertf("w0")
			t.IfErrPanic(err)

			for opindex := range vp.Seq2 {
				if insert(opindex) != nil {
					break
				}
			}

			t.IfErrPanic(insertDone(nil))
		}()

		unmerged, reasons, err := wait()
		t.NoError(err)
		t.Empty(unmerged)
		t.Empty(reasons)

		{
			t.T().Log("check mergers")

			var stateKeys []string

			t.NoError(t.closeSVMs(traverseSVMs, func(stateKey string, v *AccountSV) bool {
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

		vp, _, _, getSMVs, newPhaseMerger, getSVMs, _ := t.newFuncs()

		for i := range valuesPerWorker * len(workers) {
			vs := t.randomMergeValues(3, 3, 3)

			vp.SetValue(uint64(i), vs)
		}

		ctx, cancel := t.Ctx()

		ctx = dutil.SetContextValueLogger(ctx, logging.TestNilLogging.Log())

		insertf, wait, err := newFuncWorkerPhaseMerger(
			ctx,
			workers[0],
			cancel,
			getSMVs,
			getSVMs,
			newPhaseMerger,
			3, 1,
		)
		t.NoError(err)

		t.T().Log("insert values")
		go func() {
			for wi, worker := range slices.All(workers) {
				insert, insertDone, err := insertf(worker)
				t.IfErrPanic(err)

				for i := valuesPerWorker * wi; i < valuesPerWorker*(wi+1); i++ {
					_ = insert(uint64(i))
				}

				if worker == failedWorker {
					_ = insertDone(errors.New("findme"))
				} else {
					_ = insertDone(nil)
				}
			}
		}()

		t.T().Log("wait")
		_, _, err = wait()
		t.ErrorContains(err, "findme")
	})

	t.Run("3 worker; empty", func() {
		workers := t.workers(3)

		_, _, _, getSMVs, newPhaseMerger, getSVMs, traverseSVMs := t.newFuncs()

		ctx, cancel := t.Ctx()

		ctx = dutil.SetContextValueLogger(ctx, logging.TestNilLogging.Log())

		insertf, wait, err := newFuncWorkerPhaseMerger(
			ctx,
			workers[0],
			cancel,
			getSMVs,
			getSVMs,
			newPhaseMerger,
			3, 1,
		)
		t.NoError(err)

		go func() {
			for worker := range slices.Values(workers) {
				_, insertDone, err := insertf(worker)
				t.IfErrPanic(err)

				t.IfErrPanic(insertDone(nil))
			}
		}()

		unmerged, reasons, err := wait()
		t.NoError(err)
		t.Empty(unmerged)
		t.Empty(reasons)

		{
			t.T().Log("check mergers")

			var called int64

			t.NoError(t.closeSVMs(traverseSVMs, func(stateKey string, v *AccountSV) bool {
				atomic.AddInt64(&called, 1)

				return true
			}))

			t.Equal(int64(0), atomic.LoadInt64(&called))
		}
	})

	t.Run("3 worker; enough", func() {
		workers := t.workers(3)
		valuesPerWorker := 11

		vp, _, _, getSMVs, newPhaseMerger, getSVMs, traverseSVMs := t.newFuncs()

		var expectedStateKeys []string

		for i := range valuesPerWorker * len(workers) {
			vs := t.randomMergeValues(3, 3, 3)

			vp.SetValue(uint64(i), vs)

			for v := range slices.Values(vs) {
				a, err := util.AssertInterfaceValue[*AccountSMV](v)
				t.NoError(err)

				expectedStateKeys = append(expectedStateKeys, accountStateKey(a.Address))
			}
		}
		slices.Sort(expectedStateKeys)

		ctx, cancel := t.Ctx()

		ctx = dutil.SetContextValueLogger(ctx, logging.TestNilLogging.Log())

		insertf, wait, err := newFuncWorkerPhaseMerger(
			ctx,
			workers[0],
			cancel,
			getSMVs,
			getSVMs,
			newPhaseMerger,
			3, 1,
		)
		t.NoError(err)

		go func() {
			for wi, worker := range slices.All(workers) {
				insert, insertDone, err := insertf(worker)
				t.IfErrPanic(err)

				for i := valuesPerWorker * wi; i < valuesPerWorker*(wi+1); i++ {
					if err := insert(uint64(i)); err != nil {
						t.IfErrPanic(insertDone(err))
					}
				}

				t.IfErrPanic(insertDone(nil))
			}
		}()

		unmerged, reasons, err := wait()
		t.NoError(err)
		t.Empty(unmerged)
		t.Empty(reasons)

		{
			t.T().Log("check mergers")

			var stateKeys []string

			t.NoError(t.closeSVMs(traverseSVMs, func(stateKey string, v *AccountSV) bool {
				stateKeys = append(stateKeys, stateKey)

				t.Equal(int64(3), v.Balance)

				return true
			}))

			slices.Sort(stateKeys)

			t.Equal(expectedStateKeys, stateKeys)
		}
	})
}

func (t *testWorkerPhaseMerger) TestManagerWorker() {
	t.Run("only manager", func() {
		_, _, _, getSMVs, newPhaseMerger, getSVMs, traverseSVMs := t.newFuncs()

		ctx, cancel := t.Ctx()

		ctx = dutil.SetContextValueLogger(ctx, logging.TestNilLogging.Log())

		insertf, wait, err := newFuncWorkerPhaseMerger(
			ctx,
			"local",
			cancel,
			getSMVs,
			getSVMs,
			newPhaseMerger,
			1, 1,
		)
		t.NoError(err)

		go func() {
			_, insertDone, err := insertf(ManagerName)
			t.IfErrPanic(err)

			t.IfErrPanic(insertDone(nil))
		}()

		unmerged, reasons, err := wait()
		t.NoError(err)
		t.Empty(unmerged)
		t.Empty(reasons)

		{
			t.T().Log("check mergers")

			var called int64

			t.NoError(t.closeSVMs(traverseSVMs, func(stateKey string, v *AccountSV) bool {
				atomic.AddInt64(&called, 1)

				return true
			}))

			t.Equal(int64(0), atomic.LoadInt64(&called))
		}
	})

	t.Run("1 worker enough and then manager", func() {
		vp, _, _, getSMVs, newPhaseMerger, getSVMs, traverseSVMs := t.newFuncs()

		values := make([][]SMV, 33)
		var expectedStateKeys []string

		for i := range values {
			vs := t.randomMergeValues(3, 3, 3)
			values[i] = vs

			vp.SetValue(uint64(i), vs)

			for v := range slices.Values(vs) {
				a, err := util.AssertInterfaceValue[*AccountSMV](v)
				t.NoError(err)

				expectedStateKeys = append(expectedStateKeys, accountStateKey(a.Address))
			}
		}
		slices.Sort(expectedStateKeys)

		ctx, cancel := t.Ctx()

		ctx = dutil.SetContextValueLogger(ctx, logging.TestNilLogging.Log())

		insertf, wait, err := newFuncWorkerPhaseMerger(
			ctx,
			"w0",
			cancel,
			getSMVs,
			getSVMs,
			newPhaseMerger,
			2, 1,
		)
		t.NoError(err)

		go func() {
			insert, insertDone, err := insertf("w0")
			t.IfErrPanic(err)

			for opindex := range vp.Seq2 {
				if insert(opindex) != nil {
					break
				}
			}

			t.IfErrPanic(insertDone(nil))

			_, insertDone, err = insertf(ManagerName)
			t.IfErrPanic(err)

			t.IfErrPanic(insertDone(nil))
		}()

		unmerged, reasons, err := wait()
		t.NoError(err)
		t.Empty(unmerged)
		t.Empty(reasons)

		{
			t.T().Log("check mergers")

			var stateKeys []string

			t.NoError(t.closeSVMs(traverseSVMs, func(stateKey string, v *AccountSV) bool {
				stateKeys = append(stateKeys, stateKey)

				t.Equal(int64(3), v.Balance)

				return true
			}))

			slices.Sort(stateKeys)

			t.Equal(expectedStateKeys, stateKeys)
		}
	})

	t.Run("1 worker not enough and then manager", func() {
		vp, _, _, getSMVs, newPhaseMerger, getSVMs, traverseSVMs := t.newFuncs()

		values := make([][]SMV, 33)
		var expectedStateKeys []string

		for i := range values {
			vs := t.randomMergeValues(3, 3, 3)
			values[i] = vs

			vp.SetValue(uint64(i), vs)

			for v := range slices.Values(vs) {
				a, err := util.AssertInterfaceValue[*AccountSMV](v)
				t.NoError(err)

				expectedStateKeys = append(expectedStateKeys, accountStateKey(a.Address))
			}
		}
		slices.Sort(expectedStateKeys)

		ctx, cancel := t.Ctx()

		ctx = dutil.SetContextValueLogger(ctx, logging.TestNilLogging.Log())

		insertf, wait, err := newFuncWorkerPhaseMerger(
			ctx,
			"w0",
			cancel,
			getSMVs,
			getSVMs,
			newPhaseMerger,
			2, 1,
		)
		t.NoError(err)

		go func() {
			insert, insertDone, err := insertf("w0")
			t.IfErrPanic(err)

			for opindex := range vp.Seq2 {
				if insert(opindex) != nil {
					break
				}
			}

			t.IfErrPanic(insertDone(nil))

			_, insertDone, err = insertf(ManagerName)
			t.IfErrPanic(err)

			t.IfErrPanic(insertDone(nil))
		}()

		unmerged, reasons, err := wait()
		t.NoError(err)
		t.Empty(unmerged)
		t.Empty(reasons)

		{
			t.T().Log("check mergers")

			var stateKeys []string

			t.NoError(t.closeSVMs(traverseSVMs, func(stateKey string, v *AccountSV) bool {
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
	defer goleak.VerifyNone(t,
		goleak.IgnoreTopFunction("github.com/spikeekips/mitum/example/test364_5/util.NewUnlimitedChan[...].func2"),
		goleak.IgnoreTopFunction("github.com/spikeekips/mitum/example/test364_5/util.NewUnlimitedChan[...].func4"),
	)

	suite.Run(t, new(testWorkerPhaseMerger))
}
