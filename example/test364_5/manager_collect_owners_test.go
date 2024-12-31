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

		add, done, err := newFuncManagerCollectOwners(ctx, 1, 1)
		t.NoError(err)

		err = add("w0", []string{"w1"})
		t.ErrorIs(err, context.Canceled)

		err = done(nil)
		t.ErrorIs(err, context.Canceled)
	})

	t.Run("before add", func() {
		ctx, cancel := t.Ctx()

		add, done, err := newFuncManagerCollectOwners(ctx, 1, 1)
		t.NoError(err)

		cancel(nil)

		err = add("w0", []string{"w1"})
		t.ErrorIs(err, context.Canceled)

		err = done(nil)
		t.ErrorIs(err, context.Canceled)
	})

	t.Run("before done", func() {
		ctx, cancel := t.Ctx()

		add, done, err := newFuncManagerCollectOwners(ctx, 2, 2)
		t.NoError(err)

		t.NoError(add("w0", []string{"w1"}))

		cancel(nil)

		err = done(nil)
		t.ErrorIs(err, context.Canceled)
	})
}

func (t *testManagerCollectOwners) TestAdd() {
	t.Run("only one", func() {
		ctx, _ := t.Ctx()

		add, done, err := newFuncManagerCollectOwners(ctx, 1, 1)
		t.NoError(err)

		owners := []string{"w1"}
		t.NoError(add("w0", owners))

		t.NoError(done(func(s []string) error {
			t.Equal(owners, s)

			return nil
		}))
	})

	t.Run("duplicated worker", func() {
		ctx, _ := t.Ctx()

		add, done, err := newFuncManagerCollectOwners(ctx, 4, 2)
		t.NoError(err)

		expectedOwners := []string{"w3", "w4", "w5"}
		t.NoError(add("w1", []string{"w4", "w5"}))
		t.NoError(add("w0", []string{"w3", "w4"}))

		t.NoError(add("w1", []string{"w6", "w7"}))

		t.NoError(done(func(s []string) error {
			slices.Sort(s)

			t.Equal(expectedOwners, s)

			return nil
		}))
	})

	t.Run("duplicated owners", func() {
		ctx, _ := t.Ctx()

		add, done, err := newFuncManagerCollectOwners(ctx, 4, 2)
		t.NoError(err)

		expectedOwners := []string{"w3", "w4", "w5"}
		t.NoError(add("w1", []string{"w4", "w5"}))
		t.NoError(add("w0", []string{"w3", "w4"}))

		t.NoError(done(func(s []string) error {
			slices.Sort(s)

			t.Equal(expectedOwners, s)

			return nil
		}))
	})

	t.Run("over enough", func() {
		ctx, _ := t.Ctx()

		add, done, err := newFuncManagerCollectOwners(ctx, 4, 2)
		t.NoError(err)

		expectedOwners := []string{"w3", "w4", "w5"}
		t.NoError(add("w1", []string{"w4", "w5"}))
		t.NoError(add("w0", []string{"w3", "w4"}))
		t.NoError(add("w2", []string{"w5", "w6"}))

		t.NoError(done(func(s []string) error {
			slices.Sort(s)

			t.Equal(expectedOwners, s)

			return nil
		}))
	})

	t.Run("async", func() {
		ctx, _ := t.Ctx()

		workers := make([]string, 99)

		for i := 0; i < len(workers); i++ {
			workers[i] = fmt.Sprintf("w%02d", i)
		}

		ownersPerWorker := 9

		add, done, err := newFuncManagerCollectOwners(ctx, uint64(len(workers)), uint64(len(workers)))
		t.NoError(err)

		dutil.TraverseSlice(workers, func(i int, worker string) (bool, error) {
			j := i

			go func() {
				e := j + ownersPerWorker
				if f := len(workers); e > f {
					e = f
				}

				owners := workers[j:e]
				if len(owners) < ownersPerWorker {
					owners = append(owners, workers[:ownersPerWorker-len(owners)]...)
				}

				if err := add(worker, owners); err != nil {
					panic(err)
				}
			}()

			return true, nil
		})

		t.NoError(done(func(s []string) error {
			slices.Sort(s)

			t.Equal(workers, s)

			return nil
		}))
	})
}

func TestManagerCollectOwners(t *testing.T) {
	defer goleak.VerifyNone(t)

	suite.Run(t, new(testManagerCollectOwners))
}
