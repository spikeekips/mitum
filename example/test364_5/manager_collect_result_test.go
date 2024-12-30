package main

import (
	"context"
	"slices"
	"strings"
	"sync"
	"testing"

	"github.com/pkg/errors"
	"github.com/spikeekips/mitum/example/test364_5/fixedtree"
	dutil "github.com/spikeekips/mitum/example/test364_5/util"
	"github.com/spikeekips/mitum/util"
	"github.com/spikeekips/mitum/util/valuehash"
	"github.com/stretchr/testify/suite"
	"go.uber.org/goleak"
)

type testManagerCollectResult struct {
	dutil.BaseSuite
	baseTestWorkers
}

func (t *testManagerCollectResult) checkOpsTree(ophs [][2]util.Hash, expectedReasons map[uint64]string, tr *fixedtree.Tree) {
	t.NoError(tr.IsValid(nil))

	t.Equal(len(ophs), tr.Len())

	tr.Traverse(func(index uint64, n fixedtree.Node) (bool, error) {
		t.NoError(n.IsValid(nil))

		op := ophs[index]
		b, ok := n.(*OperationTreeNode)
		t.True(ok)

		t.NotNil(b.Hash())
		t.Equal(op[0].String(), b.Key())
		t.True(op[1].Equal(b.Fact()))

		if e, found := expectedReasons[index]; found {
			t.Equal(e, b.Reason())
		} else {
			t.Empty(b.Reason())
		}

		return true, nil
	})
}

func (t *testManagerCollectResult) checkStatesTree(expectedStates [][2]interface{}, tr *fixedtree.Tree) {
	t.NoError(tr.IsValid(nil))

	t.Equal(len(expectedStates), tr.Len())

	slices.SortFunc(expectedStates, func(a, b [2]interface{}) int {
		return strings.Compare(a[0].(string), b[0].(string))
	})

	t.NoError(tr.Traverse(func(index uint64, n fixedtree.Node) (bool, error) {
		a := expectedStates[index]
		b, ok := n.(*StateTreeNode)
		t.True(ok)

		t.Equal(a[0].(string), b.Key())
		t.True(a[1].(util.Hash).Equal(b.StateHash()))

		return true, nil
	}))
}

func (t *testManagerCollectResult) TestCancel() {
	t.RunWithGoLeak("before new", func() {
		ctx, cancel := t.Ctx()

		cancel(nil)

		wr, or, w, err := newFuncManagerCollectResult(ctx, 1<<9, t.randomOphs(3))
		t.NoError(err)

		{
			r, s, d, err := wr("w0")
			t.NoError(err)
			err = r(nil)
			t.ErrorIs(err, context.Canceled)
			err = s(util.UUID().String(), valuehash.RandomSHA256())
			t.ErrorIs(err, context.Canceled)
			err = d(nil)
			t.ErrorIs(err, context.Canceled)
		}

		err = or([]string{util.UUID().String(), util.UUID().String()}, 1)
		t.ErrorIs(err, context.Canceled)
		err = w(nil, nil, nil)
		t.ErrorIs(err, context.Canceled)
	})

	t.RunWithGoLeak("before worker result", func() {
		ctx, cancel := t.Ctx()

		wr, or, w, err := newFuncManagerCollectResult(ctx, 1<<9, t.randomOphs(3))
		t.NoError(err)

		cancel(nil)

		{
			r, s, d, err := wr("w0")
			t.NoError(err)
			err = r(nil)
			t.ErrorIs(err, context.Canceled)
			err = s(util.UUID().String(), valuehash.RandomSHA256())
			t.ErrorIs(err, context.Canceled)
			err = d(nil)
			t.ErrorIs(err, context.Canceled)
		}

		err = or([]string{util.UUID().String(), util.UUID().String()}, 1)
		t.ErrorIs(err, context.Canceled)
		err = w(nil, nil, nil)
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
				panic(err)
			}
		}

		_ = dutil.TraverseSlice(t.workers(3), func(i int, worker string) (bool, error) {
			wg.Add(1)

			go func() {
				defer wg.Done()

				r, s, d, err := wr(worker)
				if err != nil {
					panic(err)
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

			return true, nil
		})

		wg.Wait()

		err = or([]string{util.UUID().String(), util.UUID().String()}, 1)
		t.ErrorIs(err, context.Canceled)
		err = w(nil, nil, nil)
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
		err = w(nil, nil, nil)
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

		err = w(nil, nil, nil)
		t.ErrorIs(err, context.Canceled)
	})
}

func (t *testManagerCollectResult) TestWorkerResult() {
	t.RunWithGoLeak("unknown worker", func() {
		ctx, cancel := t.Ctx()
		defer cancel(nil)

		ophs := t.randomOphs(3)

		wr, or, w, err := newFuncManagerCollectResult(ctx, 1<<9, ophs)
		t.NoError(err)

		workers := []string{"w0"}

		t.NoError(or(workers, 1))

		_, _, d, err := wr("w1")
		t.ErrorContains(d(nil), "unknown worker")

		_, _, d, err = wr("w0")
		t.NoError(err)
		t.NoError(d(nil))

		var called bool

		t.NoError(w(nil, func(tr *fixedtree.Tree) error {
			t.checkOpsTree(ophs, nil, tr)

			called = true

			return nil
		}, nil))

		t.True(called)

		// NOTE already done
		_, _, d, err = wr("w1")
		t.NoError(err)
		t.NoError(d(nil))
	})

	t.RunWithGoLeak("after workers done", func() {
		ctx, cancel := t.Ctx()
		defer cancel(nil)

		ophs := t.randomOphs(3)

		wr, or, w, err := newFuncManagerCollectResult(ctx, 1<<9, ophs)
		t.NoError(err)

		workers := []string{"w0", "w1"}

		_, _, d, err := wr("w0")
		t.NoError(err)
		t.NoError(d(nil))

		t.NoError(or(workers, 2))

		var called bool

		t.NoError(w(nil, func(tr *fixedtree.Tree) error {
			t.checkOpsTree(ophs, nil, tr)

			called = true

			return nil
		}, nil))

		t.True(called)

		// NOTE already done
		_, _, d, err = wr("w1")
		t.NoError(err)
		t.NoError(d(nil))
	})
}

func (t *testManagerCollectResult) TestStateOwners() {
	t.RunWithGoLeak("empty state owners", func() {
		ctx, cancel := t.Ctx()
		defer cancel(nil)

		ophs := t.randomOphs(3)

		_, or, w, err := newFuncManagerCollectResult(ctx, 1<<9, ophs)
		t.NoError(err)

		t.NoError(or(nil, 1))

		var opsCalled, statesCalled bool

		t.NoError(w(nil,
			func(tr *fixedtree.Tree) error {
				t.checkOpsTree(ophs, nil, tr)

				opsCalled = true

				return nil
			},
			func(tr *fixedtree.Tree) error {
				t.checkStatesTree(nil, tr)

				statesCalled = true

				return nil
			},
		))

		t.True(opsCalled)
		t.True(statesCalled)
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

		wr, or, w, err := newFuncManagerCollectResult(ctx, 1<<9, ophs)
		t.NoError(err)

		workers := []string{"w0"}

		_, _, d, err := wr("w0")
		t.NoError(err)
		t.NoError(d(nil))

		t.NoError(or(workers, 1))

		var called bool

		t.NoError(w(nil, func(tr *fixedtree.Tree) error {
			t.checkOpsTree(ophs, nil, tr)

			called = true

			return nil
		}, nil))

		t.True(called)
	})

	t.RunWithGoLeak("with reasons", func() {
		ctx, cancel := t.Ctx()
		defer cancel(nil)

		ophs := t.randomOphs(3)
		expectedReasons := map[uint64]string{}
		dutil.TraverseSlice(ophs, func(i int, oph [2]util.Hash) (bool, error) {
			expectedReasons[uint64(i)] = oph[1].String()

			return true, nil
		})

		wr, or, w, err := newFuncManagerCollectResult(ctx, 1<<9, ophs)
		t.NoError(err)

		workers := []string{"w0"}

		r, _, d, err := wr("w0")
		t.NoError(err)
		t.NoError(r(expectedReasons))
		t.NoError(d(nil))

		t.NoError(or(workers, 1))

		var called bool

		t.NoError(w(nil, func(tr *fixedtree.Tree) error {
			t.checkOpsTree(ophs, expectedReasons, tr)

			called = true

			return nil
		}, nil))

		t.True(called)
	})
}

func (t *testManagerCollectResult) TestStatesTree() {
	t.RunWithGoLeak("empty states", func() {
		ctx, cancel := t.Ctx()
		defer cancel(nil)

		ophs := t.randomOphs(3)

		wr, or, w, err := newFuncManagerCollectResult(ctx, 1<<9, ophs)
		t.NoError(err)

		workers := []string{"w0"}

		_, _, d, err := wr("w0")
		t.NoError(err)
		t.NoError(d(nil))

		t.NoError(or(workers, 1))

		var opsCalled, statesCalled bool

		t.NoError(w(nil,
			func(tr *fixedtree.Tree) error {
				t.checkOpsTree(ophs, nil, tr)

				opsCalled = true

				return nil
			},
			func(tr *fixedtree.Tree) error {
				t.checkStatesTree(nil, tr)

				statesCalled = true

				return nil
			},
		))

		t.True(opsCalled)
		t.True(statesCalled)
	})

	t.RunWithGoLeak("ok", func() {
		ctx, cancel := t.Ctx()
		defer cancel(nil)

		ophs := t.randomOphs(3)

		wr, or, w, err := newFuncManagerCollectResult(ctx, 1<<9, ophs)
		t.NoError(err)

		workers := []string{"w0"}

		expectedStates := make([][2]interface{}, 3)
		for i := range expectedStates {
			expectedStates[i] = [2]interface{}{util.UUID().String(), valuehash.RandomSHA256()}
		}

		_, s, d, err := wr("w0")
		t.NoError(dutil.TraverseSlice(expectedStates, func(_ int, i [2]interface{}) (bool, error) {
			return true, s(i[0].(string), i[1].(util.Hash))
		}))
		t.NoError(err)
		t.NoError(d(nil))

		t.NoError(or(workers, 1))

		var opsCalled, statesCalled bool

		t.NoError(w(nil,
			func(tr *fixedtree.Tree) error {
				t.checkOpsTree(ophs, nil, tr)

				opsCalled = true

				return nil
			},
			func(tr *fixedtree.Tree) error {
				t.checkStatesTree(expectedStates, tr)

				statesCalled = true

				return nil
			},
		))

		t.True(opsCalled)
		t.True(statesCalled)
	})
}

func TestManagerCollectResult(t *testing.T) {
	defer goleak.VerifyNone(t)

	suite.Run(t, new(testManagerCollectResult))
}
