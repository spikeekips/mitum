package main

import (
	"context"
	"fmt"
	"slices"
	"testing"

	dutil "github.com/spikeekips/mitum/example/test364_5/util"
	"github.com/stretchr/testify/suite"
	"go.uber.org/goleak"
)

type testManagerCollectOwners struct {
	dutil.BaseSuite
	baseTestWorkers
}

func (t *testManagerCollectOwners) TestCancel() {
	t.Run("before new", func() {
		ctx, cancel := t.Ctx()
		cancel(nil)

		add, done, err := newFuncManagerCollectOwners(ctx, []string{"w0"}, 2, 1)
		t.NoError(err)

		err = add("w0", []string{"w1"})
		t.ErrorIs(err, context.Canceled)

		_, err = done()
		t.ErrorIs(err, context.Canceled)
	})

	t.Run("before add", func() {
		ctx, cancel := t.Ctx()

		add, done, err := newFuncManagerCollectOwners(ctx, []string{"w0"}, 2, 1)
		t.NoError(err)

		cancel(nil)

		err = add("w0", []string{"w1"})
		t.ErrorIs(err, context.Canceled)

		_, err = done()
		t.ErrorIs(err, context.Canceled)
	})

	t.Run("before done", func() {
		ctx, cancel := t.Ctx()

		add, done, err := newFuncManagerCollectOwners(ctx, []string{"w0", "w1"}, 2, 2)
		t.NoError(err)

		t.NoError(add("w0", []string{"w1"}))

		cancel(nil)

		_, err = done()
		t.ErrorIs(err, context.Canceled)
	})
}

func (t *testManagerCollectOwners) TestAdd() {
	t.Run("only one", func() {
		ctx, _ := t.Ctx()

		add, done, err := newFuncManagerCollectOwners(ctx, []string{"w0"}, 2, 1)
		t.NoError(err)

		owners := []string{"w1"}
		t.NoError(add("w0", owners))

		s, err := done()
		t.NoError(err)
		t.Equal(owners, s)
	})

	t.Run("duplicated worker", func() {
		ctx, _ := t.Ctx()

		add, done, err := newFuncManagerCollectOwners(ctx, []string{"w0", "w1"}, 9, 1)
		t.NoError(err)

		expectedOwners := []string{"w3", "w4", "w5", "w6", "w7"}

		t.NoError(add("w1", []string{"w4", "w5"}))
		t.NoError(add("w1", []string{"w6", "w7"}))
		t.NoError(add("w0", []string{"w3", "w4"}))

		s, err := done()
		t.NoError(err)
		slices.Sort(s)

		t.Equal(expectedOwners, s)
	})

	t.Run("duplicated owners", func() {
		ctx, _ := t.Ctx()

		add, done, err := newFuncManagerCollectOwners(ctx, []string{"w0", "w1"}, 9, 1)
		t.NoError(err)

		expectedOwners := []string{"w3", "w4", "w5"}
		t.NoError(add("w1", []string{"w4", "w5"}))
		t.NoError(add("w0", []string{"w3", "w4"}))

		s, err := done()
		t.NoError(err)
		slices.Sort(s)

		t.Equal(expectedOwners, s)
	})

	t.Run("over enough", func() {
		ctx, _ := t.Ctx()

		add, done, err := newFuncManagerCollectOwners(ctx, []string{"w0", "w1", "w2"}, 9, 2)
		t.NoError(err)

		expectedOwners := []string{"w3", "w4", "w5"}
		t.NoError(add("w1", []string{"w4", "w5"}))
		t.NoError(add("w0", []string{"w3", "w4"}))
		t.NoError(add("w2", []string{"w5", "w6"}))

		s, err := done()
		t.NoError(err)
		slices.Sort(s)

		t.Equal(expectedOwners, s)
	})

	t.Run("async", func() {
		ctx, _ := t.Ctx()

		workers := make([]string, 99)

		for i := 0; i < len(workers); i++ {
			workers[i] = fmt.Sprintf("w%02d", i)
		}

		ownersPerWorker := 9

		add, done, err := newFuncManagerCollectOwners(ctx, workers, uint64(len(workers)), 1)
		t.NoError(err)

		for i, worker := range slices.All(workers) {
			go func() {
				e := i + ownersPerWorker
				if f := len(workers); e > f {
					e = f
				}

				owners := workers[i:e]
				if len(owners) < ownersPerWorker {
					owners = append(owners, workers[:ownersPerWorker-len(owners)]...)
				}

				t.IfErrPanic(add(worker, owners))
			}()
		}

		s, err := done()
		t.NoError(err)
		slices.Sort(s)

		t.Equal(workers, s)
	})
}

func TestManagerCollectOwners(t *testing.T) {
	defer goleak.VerifyNone(t)

	suite.Run(t, new(testManagerCollectOwners))
}
