package main

import (
	"context"
	"fmt"
	"slices"
	"sync"
	"testing"

	"github.com/stretchr/testify/suite"
	"go.uber.org/goleak"
)

type testWorkerStateOwners struct {
	suite.Suite
	baseTestMergeOp
	baseTestWorkers
}

func (t *testWorkerStateOwners) owners(n int, prefix string) []string {
	s := make([]string, n)

	for i := range n {
		s[i] = fmt.Sprintf("%s%d", prefix, i)
	}

	return s
}

func (t *testWorkerStateOwners) randomGetStateOwners() getStateOwnersFunc {
	return func(stateKey string) []string {
		return []string{stateKey}
	}
}

func (t *testWorkerStateOwners) TestCancel() {
	local := "local"

	t.Run("before local", func() {
		ctx, cancel := context.WithCancel(context.Background())

		cancel()

		insertf, add, done, err := newFuncWorkerStateOwners(ctx, local, 1<<9)
		t.NoError(err)

		{
			insert, traverse, err := insertf(t.randomGetStateOwners())
			t.NoError(err)

			_, err = insert(0, t.randomMergeValues(1)[0], t.randomMergeValues(1, 2))
			t.ErrorIs(err, context.Canceled)

			err = traverse(nil)
			t.ErrorIs(err, context.Canceled)
		}

		err = add("w0", t.owners(3, "a"), t.owners(3, "b"))
		t.ErrorIs(err, context.Canceled)

		err = done(nil)
		t.ErrorIs(err, context.Canceled)
	})

	t.Run("before local insert", func() {
		ctx, cancel := context.WithCancel(context.Background())

		insertf, add, done, err := newFuncWorkerStateOwners(ctx, local, 1<<9)
		t.NoError(err)

		{
			insert, traverse, err := insertf(t.randomGetStateOwners())
			t.NoError(err)

			cancel()

			_, err = insert(0, t.randomMergeValues(1)[0], t.randomMergeValues(1, 2))
			t.ErrorIs(err, context.Canceled)

			err = traverse(nil)
			t.ErrorIs(err, context.Canceled)
		}

		err = add("w0", t.owners(3, "a"), t.owners(3, "b"))
		t.ErrorIs(err, context.Canceled)

		err = done(nil)
		t.ErrorIs(err, context.Canceled)
	})

	t.Run("before local travese", func() {
		ctx, cancel := context.WithCancel(context.Background())

		insertf, add, done, err := newFuncWorkerStateOwners(ctx, local, 1<<9)
		t.NoError(err)

		{
			insert, traverse, err := insertf(t.randomGetStateOwners())
			t.NoError(err)

			_, err = insert(0, t.randomMergeValues(1)[0], t.randomMergeValues(1, 2))
			t.NoError(err)

			cancel()

			err = traverse(nil)
			t.ErrorIs(err, context.Canceled)
		}

		err = add("w0", t.owners(3, "a"), t.owners(3, "b"))
		t.ErrorIs(err, context.Canceled)

		err = done(nil)
		t.ErrorIs(err, context.Canceled)
	})

	t.Run("before add", func() {
		ctx, cancel := context.WithCancel(context.Background())

		cancel()

		_, add, done, err := newFuncWorkerStateOwners(ctx, local, 1<<9)
		t.NoError(err)

		err = add("w0", t.owners(3, "a"), t.owners(3, "b"))
		t.ErrorIs(err, context.Canceled)

		err = done(nil)
		t.ErrorIs(err, context.Canceled)
	})

	t.Run("during add", func() {
		ctx, cancel := context.WithCancel(context.Background())

		_, add, done, err := newFuncWorkerStateOwners(ctx, local, 1<<9)
		t.NoError(err)

		t.NoError(add("w0", t.owners(3, "a"), t.owners(3, "b")))

		cancel()

		err = add("w1", t.owners(3, "a"), t.owners(3, "b"))
		t.ErrorIs(err, context.Canceled)

		err = done(nil)
		t.ErrorIs(err, context.Canceled)
	})

	t.Run("after add", func() {
		ctx, cancel := context.WithCancel(context.Background())

		_, add, done, err := newFuncWorkerStateOwners(ctx, local, 1<<9)
		t.NoError(err)

		t.NoError(add("w0", t.owners(3, "a"), t.owners(3, "b")))

		cancel()

		err = done(nil)
		t.ErrorIs(err, context.Canceled)
	})
}

func (t *testWorkerStateOwners) TestLocalInsert() {
	local := "local"

	t.Run("no insert", func() {
		workers := t.workers(9)

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		insertf, _, done, err := newFuncWorkerStateOwners(ctx, local, 1<<9)
		t.NoError(err)
		defer t.NoError(done(nil))

		getStateOwner, stateOwnersClose, err := stateOwnersFuncByCount(workers, 2)
		t.NoError(err)
		defer stateOwnersClose()

		_, traverse, err := insertf(getStateOwner)
		t.NoError(err)

		err = traverse(func(worker string, subws, affected []string) error {
			t.Empty(subws, worker)
			t.Empty(affected, worker)

			return nil
		})
		t.NoError(err)

		t.NoError(done(func(subws, affected []string) error {
			t.Empty(subws)
			t.Empty(affected)

			return nil
		}))
	})

	t.Run("only local insert", func() {
		workers := t.workers(9)

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		insertf, _, done, err := newFuncWorkerStateOwners(ctx, local, 1<<9)
		t.NoError(err)
		defer t.NoError(done(nil))

		values := make([][2]interface{}, len(workers))
		for i := range values {
			values[i] = [2]interface{}{t.randomMergeValues(-1)[0], t.randomMergeValues(1, 2)}
		}

		getStateOwner, stateOwnersClose, err := stateOwnersFuncByCount(workers, 2)
		t.NoError(err)
		defer stateOwnersClose()

		// NOTE preload
		traverseSlice(values, func(i int, v [2]interface{}) (bool, error) {
			_ = getStateOwner(v[0].(StateMergeValue).Key())

			traverseSlice(v[1].([]StateMergeValue), func(i int, value StateMergeValue) (bool, error) {
				_ = getStateOwner(value.Key())

				return true, nil
			})
			return true, nil
		})

		expectedSubws := map[string][]string{
			"w0": {"w6", "w7"},
			"w1": {"w6", "w7"},
			"w3": {"w0", "w1"},
			"w4": {"w0", "w1"},
			"w6": {"w3", "w4"},
			"w7": {"w3", "w4"},
		}

		expectedAffected := map[string][]string{
			"w0": {"w2", "w3", "w4", "w5"},
			"w1": {"w2", "w3", "w4", "w5"},
			"w3": {"w5", "w6", "w7", "w8"},
			"w4": {"w5", "w6", "w7", "w8"},
			"w6": {"w0", "w1", "w2", "w8"},
			"w7": {"w0", "w1", "w2", "w8"},
		}

		insert, traverse, err := insertf(getStateOwner)
		t.NoError(err)

		t.NoError(traverseSlice(values, func(i int, v [2]interface{}) (bool, error) {
			_, err = insert(
				uint64(i),
				v[0].(StateMergeValue),
				v[1].([]StateMergeValue),
			)

			return err == nil, err
		}))

		err = traverse(func(worker string, subws, affected []string) error {
			slices.Sort(subws)
			slices.Sort(affected)
			t.Equal(expectedSubws[worker], subws, worker)
			t.Equal(expectedAffected[worker], affected, worker)

			return nil
		})
		t.NoError(err)
	})

	t.Run("local insert with other workers", func() {
		workers := t.workers(9)
		local := workers[0]

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		insertf, add, done, err := newFuncWorkerStateOwners(ctx, local, 1<<9)
		t.NoError(err)
		defer t.NoError(done(nil))

		values := make([][2]interface{}, len(workers))
		for i := range values {
			values[i] = [2]interface{}{t.randomMergeValues(-1)[0], t.randomMergeValues(1, 2)}
		}

		getStateOwner, stateOwnersClose, err := stateOwnersFuncByCount(workers, 2)
		t.NoError(err)
		defer stateOwnersClose()

		// NOTE preload
		traverseSlice(values, func(i int, v [2]interface{}) (bool, error) {
			_ = getStateOwner(v[0].(StateMergeValue).Key())

			traverseSlice(v[1].([]StateMergeValue), func(i int, value StateMergeValue) (bool, error) {
				_ = getStateOwner(value.Key())

				return true, nil
			})
			return true, nil
		})

		expectedSubws := map[string][]string{
			"w0": {"w6", "w7"},
			"w1": {"w6", "w7"},
			"w3": {"w0", "w1"},
			"w4": {"w0", "w1"},
			"w6": {"w3", "w4"},
			"w7": {"w3", "w4"},
		}

		expectedAffected := map[string][]string{
			"w0": {"w2", "w3", "w4", "w5"},
			"w1": {"w2", "w3", "w4", "w5"},
			"w3": {"w5", "w6", "w7", "w8"},
			"w4": {"w5", "w6", "w7", "w8"},
			"w6": {"w0", "w1", "w2", "w8"},
			"w7": {"w0", "w1", "w2", "w8"},
		}

		insert, traverse, err := insertf(getStateOwner)
		t.NoError(err)

		t.NoError(traverseSlice(values, func(i int, v [2]interface{}) (bool, error) {
			_, err = insert(
				uint64(i),
				v[0].(StateMergeValue),
				v[1].([]StateMergeValue),
			)

			return err == nil, err
		}))

		err = traverse(func(worker string, subws, affected []string) error {
			slices.Sort(subws)
			slices.Sort(affected)
			t.Equal(expectedSubws[worker], subws, worker)
			t.Equal(expectedAffected[worker], affected, worker)

			return nil
		})
		t.NoError(err)

		otherSubws := func() []string {
			o := make([]string, len(workers)-1)

			for i := range workers[1:] {
				o[i] = fmt.Sprintf("s%d", i)
			}

			return o
		}()

		otherAffected := func() []string {
			o := make([]string, len(workers)-1)

			for i := range workers[1:] {
				o[i] = fmt.Sprintf("a%d", i)
			}

			return o
		}()

		traverseSlice(workers, func(i int, worker string) (bool, error) {
			if worker == local {
				return true, nil
			}

			t.NoError(add(worker, []string{otherSubws[i-1]}, []string{otherAffected[i-1]}))

			return true, nil
		})

		t.NoError(done(func(subws, affected []string) error {
			slices.Sort(subws)
			slices.Sort(affected)

			localExpectedSubws := slices.Concat(expectedSubws[local], otherSubws)
			slices.Sort(localExpectedSubws)
			t.Equal(localExpectedSubws, subws)

			localExpectedAffected := slices.Concat(expectedAffected[local], otherAffected)
			slices.Sort(localExpectedAffected)
			t.Equal(localExpectedAffected, affected)

			return nil
		}))
	})
}

func (t *testWorkerStateOwners) TestAdd() {
	local := "local"

	t.Run("empty", func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		_, _, done, err := newFuncWorkerStateOwners(ctx, local, 1<<9)
		t.NoError(err)

		t.NoError(done(func(subs, affected []string) error {
			t.Empty(subs)
			t.Empty(affected)

			return nil
		}))
	})

	t.Run("add async", func() {
		expectedSubs := t.owners(9, "s")
		expectedAffected := t.owners(9, "a")

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		_, add, done, err := newFuncWorkerStateOwners(ctx, local, 1<<9)
		t.NoError(err)

		var wg sync.WaitGroup

		for i := range 3 {
			wg.Add(1)

			go func() {
				defer wg.Done()

				t.println("add: %d", i)

				if err := add(
					fmt.Sprintf("w%d", i),
					expectedSubs[i*3:(i+1)*3],
					expectedAffected[i*3:(i+1)*3],
				); err != nil {
					panic(err)
				}
			}()
		}

		wg.Wait()

		t.NoError(done(func(subs, affected []string) error {
			slices.Sort(subs)
			t.Equal(expectedSubs, subs)

			slices.Sort(affected)
			t.Equal(expectedAffected, affected)

			return nil
		}))
	})

	t.Run("over max size", func() {
		expectedSubs := t.owners(9, "s")
		expectedAffected := t.owners(9, "a")

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		_, add, done, err := newFuncWorkerStateOwners(ctx, local, 1)
		t.NoError(err)

		var wg sync.WaitGroup

		for i := range 3 {
			wg.Add(1)

			go func() {
				defer wg.Done()

				if err := add(
					fmt.Sprintf("w%d", i),
					expectedSubs[i*3:(i+1)*3],
					expectedAffected[i*3:(i+1)*3],
				); err != nil {
					panic(err)
				}
			}()
		}

		wg.Wait()

		t.NoError(done(func(subs, affected []string) error {
			slices.Sort(subs)
			t.Equal(expectedSubs, subs)

			slices.Sort(affected)
			t.Equal(expectedAffected, affected)

			return nil
		}))
	})

	t.Run("duplicated", func() {
		expectedSubs := t.owners(9, "s")
		expectedAffected := t.owners(9, "a")

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		_, add, done, err := newFuncWorkerStateOwners(ctx, local, 1<<9)
		t.NoError(err)

		var wg sync.WaitGroup

		for i := range 3 {
			wg.Add(1)

			go func() {
				defer wg.Done()

				if err := add(fmt.Sprintf("w%d", i), expectedSubs, expectedAffected); err != nil {
					panic(err)
				}
			}()
		}

		wg.Wait()

		t.NoError(done(func(subs, affected []string) error {
			slices.Sort(subs)
			t.Equal(expectedSubs, subs)

			slices.Sort(affected)
			t.Equal(expectedAffected, affected)

			return nil
		}))
	})
}

func TestWorkerStateOwners(t *testing.T) {
	defer goleak.VerifyNone(t)

	suite.Run(t, new(testWorkerStateOwners))
}
