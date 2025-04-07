package dop5

import (
	"context"
	"slices"
	"sync"
	"testing"

	"github.com/pkg/errors"
	dutil "github.com/spikeekips/mitum/example/test364_5/util"
	"github.com/spikeekips/mitum/util"
	"github.com/spikeekips/mitum/util/valuehash"
	"github.com/stretchr/testify/suite"
	"go.uber.org/goleak"
)

type testManagerCollectResult struct {
	dutil.BaseSuite
	baseTestWorkers
	baseTestManager
}

func (t *testManagerCollectResult) TestCancel() {
	t.RunWithGoLeak("before new", func() {
		ctx, cancel := t.Ctx()

		cancel(nil)

		wr, or, w, err := newFuncManagerCollectResult(ctx, 1<<9, t.randomOphs(3))
		t.NoError(err)

		_, _, _, err = wr("w0")
		t.ErrorIs(err, context.Canceled)

		err = or([]string{util.UUID().String(), util.UUID().String()}, 1)
		t.ErrorIs(err, context.Canceled)
		_, _, err = w(t.baseGenerateNodesHashFunc())
		t.ErrorIs(err, context.Canceled)
	})

	t.RunWithGoLeak("before worker result", func() {
		ctx, cancel := t.Ctx()

		wr, or, w, err := newFuncManagerCollectResult(ctx, 1<<9, t.randomOphs(3))
		t.NoError(err)

		cancel(nil)

		_, _, _, err = wr("w0")
		t.ErrorIs(err, context.Canceled)

		err = or([]string{util.UUID().String(), util.UUID().String()}, 1)
		t.ErrorIs(err, context.Canceled)
		_, _, err = w(t.baseGenerateNodesHashFunc())
		t.ErrorIs(err, context.Canceled)
	})

	t.RunWithGoLeak("during worker result", func() {
		ctx, cancel := t.Ctx()

		wr, or, w, err := newFuncManagerCollectResult(ctx, 1<<9, t.randomOphs(3))
		t.NoError(err)

		var wg sync.WaitGroup

		checkError := func(err error) {
			switch {
			case err == nil,
				errors.Is(err, context.Canceled):
			default:
				t.IfErrPanic(err)
			}
		}

		for i, worker := range slices.All(t.workers(3)) {
			wg.Add(1)

			go func() {
				defer wg.Done()

				r, s, d, err := wr(worker)
				if err != nil {
					return
				}

				if i == 1 {
					cancel(errors.Errorf("showme"))
				}

				if err = r(nil); err != nil {
					checkError(err)
				}

				if err = s(util.UUID().String(), valuehash.RandomSHA256()); err != nil {
					checkError(err)
				}
				if err = d(nil); err != nil {
					checkError(err)
				}
			}()
		}

		wg.Wait()

		err = or([]string{util.UUID().String(), util.UUID().String()}, 1)
		t.ErrorIs(err, context.Canceled)
		_, _, err = w(t.baseGenerateNodesHashFunc())
		t.ErrorIs(err, context.Canceled)
	})

	t.RunWithGoLeak("before owners", func() {
		ctx, cancel := t.Ctx()

		wr, or, w, err := newFuncManagerCollectResult(ctx, 1<<9, t.randomOphs(3))
		t.NoError(err)

		r, s, d, err := wr("w0")
		t.NoError(err)
		t.NoError(r(nil))
		t.NoError(s(util.UUID().String(), valuehash.RandomSHA256()))
		t.NoError(d(nil))

		cancel(nil)

		err = or([]string{util.UUID().String(), util.UUID().String()}, 1)
		t.ErrorIs(err, context.Canceled)
		_, _, err = w(t.baseGenerateNodesHashFunc())
		t.ErrorIs(err, context.Canceled)
	})

	t.RunWithGoLeak("before wait", func() {
		ctx, cancel := t.Ctx()

		wr, or, w, err := newFuncManagerCollectResult(ctx, 1<<9, t.randomOphs(3))
		t.NoError(err)

		r, s, d, err := wr("w0")
		t.NoError(err)
		t.NoError(r(nil))
		t.NoError(s(util.UUID().String(), valuehash.RandomSHA256()))
		t.NoError(d(nil))

		t.NoError(or([]string{"w0"}, 1))

		cancel(nil)

		_, _, err = w(t.baseGenerateNodesHashFunc())
		t.ErrorIs(err, context.Canceled)
	})
}

func (t *testManagerCollectResult) TestWorkerResult() {
	t.RunWithGoLeak("after workers done", func() {
		ctx, cancel := t.Ctx()
		defer cancel(nil)

		ophs := t.randomOphs(3)
		workers := []string{"w0", "w1"}

		wr, or, w, err := newFuncManagerCollectResult(ctx, 1<<9, ophs)
		t.NoError(err)

		_, _, d, err := wr("w0")
		t.NoError(err)
		t.NoError(d(nil))

		t.NoError(or(workers, 2))

		opsTree, _, err := w(t.baseGenerateNodesHashFunc())
		t.NoError(err)
		t.NoError(t.checkOpsTree(ophs, nil, opsTree))

		// NOTE already done
		_, _, d, err = wr("w1")
		t.NoError(err)
		t.Nil(d)
	})
}

func (t *testManagerCollectResult) TestMaxFailed() {
	t.RunWithGoLeak("under max failed", func() {
		ctx, cancel := t.Ctx()
		defer cancel(nil)

		workers := t.workers(3)
		var statesShare uint64 = 2
		ophs := t.randomOphs(3)

		{
			minEnough, maxFailed, err := minEnoughMaxFailed(uint64(len(workers)), statesShare)
			t.NoError(err)
			t.T().Logf("workers=%d states share=%d min enough=%d max failed=%d", len(workers), statesShare, minEnough, maxFailed)
		}

		wr, or, w, err := newFuncManagerCollectResult(ctx, 1<<9, ophs)
		t.NoError(err)

		t.NoError(or(workers, statesShare))

		_, _, d, err := wr("w0") // NOTE w0 ok
		t.NoError(err)
		t.NoError(d(nil))

		_, _, d, err = wr("w1") // NOTE w1 error
		t.NoError(err)

		err = d(errors.Errorf("showme"))
		t.NoError(err)

		_, _, d, err = wr("w2") // NOTE w2 ok
		t.NoError(err)
		t.NoError(d(nil))

		opsTree, _, err := w(t.baseGenerateNodesHashFunc())
		t.NoError(err)
		t.NoError(t.checkOpsTree(ophs, nil, opsTree))
	})

	t.RunWithGoLeak("over max failed", func() {
		ctx, cancel := t.Ctx()
		defer cancel(nil)

		workers := t.workers(3)
		var statesShare uint64 = 1
		ophs := t.randomOphs(3)

		{
			minEnough, maxFailed, err := minEnoughMaxFailed(uint64(len(workers)), statesShare)
			t.NoError(err)
			t.T().Logf("workers=%d states share=%d min enough=%d max failed=%d", len(workers), statesShare, minEnough, maxFailed)
		}

		wr, or, w, err := newFuncManagerCollectResult(ctx, 1<<9, ophs)
		t.NoError(err)

		t.NoError(or(workers, statesShare))

		_, _, d, err := wr("w0") // NOTE w0 ok
		t.NoError(err)
		t.NoError(d(nil))

		_, _, d, err = wr("w1") // NOTE w1 error
		t.NoError(err)

		err = d(errors.Errorf("showme"))
		t.ErrorIs(err, errWorkersFailed)
		t.ErrorContains(err, "showme")

		_, _, err = w(t.baseGenerateNodesHashFunc())
		t.ErrorIs(err, errWorkersFailed)

		switch {
		case errors.Is(err, context.Canceled):
		default:
			t.ErrorContains(err, "showme")
		}
	})
}

func (t *testManagerCollectResult) TestStatesOwners() {
	t.RunWithGoLeak("empty states owners", func() {
		ctx, cancel := t.Ctx()
		defer cancel(nil)

		ophs := t.randomOphs(3)

		_, or, w, err := newFuncManagerCollectResult(ctx, 1<<9, ophs)
		t.NoError(err)

		t.NoError(or(nil, 1))

		opsTree, statesTree, err := w(t.baseGenerateNodesHashFunc())
		t.NoError(err)
		t.NoError(t.checkOpsTree(ophs, nil, opsTree))
		t.NoError(t.checkStatesTree(nil, statesTree))
	})
}

func (t *testManagerCollectResult) TestOpsTree() {
	t.RunWithGoLeak("empty ophs", func() {
		ctx, cancel := t.Ctx()
		defer cancel(nil)

		_, _, _, err := newFuncManagerCollectResult(ctx, 1<<9, nil)
		t.ErrorContains(err, "empty ophs")
	})

	t.RunWithGoLeak("ok", func() {
		ctx, cancel := t.Ctx()
		defer cancel(nil)

		ophs := t.randomOphs(3)
		workers := []string{"w0"}

		wr, or, w, err := newFuncManagerCollectResult(ctx, 1<<9, ophs)
		t.NoError(err)

		_, _, d, err := wr("w0")
		t.NoError(err)
		t.NoError(d(nil))

		t.NoError(or(workers, 1))

		opsTree, _, err := w(t.baseGenerateNodesHashFunc())
		t.NoError(err)
		t.NoError(t.checkOpsTree(ophs, nil, opsTree))
	})

	t.RunWithGoLeak("with reasons", func() {
		ctx, cancel := t.Ctx()
		defer cancel(nil)

		ophs := t.randomOphs(3)
		expectedReasons := map[uint64]string{}
		for i, oph := range slices.All(ophs) {
			expectedReasons[uint64(i)] = oph[1].String()
		}

		workers := []string{"w0"}

		wr, or, w, err := newFuncManagerCollectResult(ctx, 1<<9, ophs)
		t.NoError(err)

		r, _, d, err := wr("w0")
		t.NoError(err)
		t.NoError(r(expectedReasons))
		t.NoError(d(nil))

		t.NoError(or(workers, 1))

		opsTree, _, err := w(t.baseGenerateNodesHashFunc())
		t.NoError(err)
		t.NoError(t.checkOpsTree(ophs, expectedReasons, opsTree))
	})
}

func (t *testManagerCollectResult) TestStatesTree() {
	t.RunWithGoLeak("empty states", func() {
		ctx, cancel := t.Ctx()
		defer cancel(nil)

		ophs := t.randomOphs(3)

		workers := []string{"w0"}
		wr, or, w, err := newFuncManagerCollectResult(ctx, 1<<9, ophs)
		t.NoError(err)

		_, _, d, err := wr("w0")
		t.NoError(err)
		t.NoError(d(nil))

		t.NoError(or(workers, 1))

		opsTree, statesTree, err := w(t.baseGenerateNodesHashFunc())
		t.NoError(err)
		t.NoError(t.checkOpsTree(ophs, nil, opsTree))
		t.NoError(t.checkStatesTree(nil, statesTree))
	})

	t.RunWithGoLeak("ok", func() {
		ctx, cancel := t.Ctx()
		defer cancel(nil)

		ophs := t.randomOphs(3)

		workers := []string{"w0"}
		wr, or, w, err := newFuncManagerCollectResult(ctx, 1<<9, ophs)
		t.NoError(err)

		expectedStates := make([][2]interface{}, 3)
		for i := range expectedStates {
			expectedStates[i] = [2]interface{}{util.UUID().String(), valuehash.RandomSHA256()}
		}

		_, s, d, err := wr("w0")
		for i := range slices.Values(expectedStates) {
			t.NoError(s(i[0].(string), i[1].(util.Hash)))
		}
		t.NoError(err)
		t.NoError(d(nil))

		t.NoError(or(workers, 1))

		opsTree, statesTree, err := w(t.baseGenerateNodesHashFunc())
		t.NoError(err)
		t.NoError(t.checkOpsTree(ophs, nil, opsTree))
		t.NoError(t.checkStatesTree(expectedStates, statesTree))
	})
}

func TestManagerCollectResult(t *testing.T) {
	defer goleak.VerifyNone(t)

	suite.Run(t, new(testManagerCollectResult))
}
