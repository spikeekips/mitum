package util

import (
	"slices"
	"sync"
	"testing"

	"github.com/stretchr/testify/suite"
	"go.uber.org/goleak"
)

type testAppendSlice struct {
	suite.Suite
}

func (t *testAppendSlice) TestAppendSlice() {
	t.Run("error, size 0", func() {
		_, _, err := AppendSlice[int](0, 0)
		t.ErrorContains(err, "size and grow under 1")
	})

	t.Run("error, size, grow 0", func() {
		_, _, err := AppendSlice[int](0, 0)
		t.ErrorContains(err, "size and grow under 1")
	})

	t.Run("no error, size 0, but grow over 0", func() {
		_, _, err := AppendSlice[int](0, 1)
		t.NoError(err)
	})

	t.Run("size < 1 and grow > 0", func() {
		var grow uint64 = 1

		add, done, err := AppendSlice[int](0, grow)
		t.NoError(err)

		added, left := add(1)
		t.EqualValues(1, added)
		t.EqualValues(grow, left)

		added, left = add(2)
		t.EqualValues(1, added)
		t.EqualValues(grow, left)

		added, left = add(3)
		t.EqualValues(1, added)
		t.EqualValues(grow, left)

		added, left = add(4)
		t.EqualValues(1, added)
		t.EqualValues(grow, left)

		t.Equal([]int{1, 2, 3, 4}, done())
	})

	t.Run("ok", func() {
		add, done, err := AppendSlice[int](3, 0)
		t.NoError(err)

		added, left := add(1)
		t.EqualValues(1, added)
		t.EqualValues(2, left)

		added, left = add(2)
		t.EqualValues(1, added)
		t.EqualValues(1, left)

		added, left = add(3)
		t.EqualValues(1, added)
		t.EqualValues(0, left)

		added, left = add(4)
		t.EqualValues(0, added)
		t.EqualValues(0, left)

		t.Equal([]int{1, 2, 3}, done())
	})

	t.Run("add after done", func() {
		add, done, err := AppendSlice[int](3, 0)
		t.NoError(err)

		added, left := add(1)
		t.EqualValues(1, added)
		t.EqualValues(2, left)

		added, left = add(2)
		t.EqualValues(1, added)
		t.EqualValues(1, left)

		t.Equal([]int{1, 2}, done())

		added, left = add(3)
		t.EqualValues(1, added)
		t.EqualValues(0, left)

		t.Equal([]int{1, 2, 3}, done())
	})

	t.Run("variadic, less", func() {
		add, done, err := AppendSlice[int](3, 0)
		t.NoError(err)

		added, left := add(1, 2)
		t.EqualValues(2, added)
		t.EqualValues(1, left)

		t.Equal([]int{1, 2}, done())
	})

	t.Run("variadic, full", func() {
		add, done, err := AppendSlice[int](3, 0)
		t.NoError(err)

		added, left := add(1, 2, 3)
		t.EqualValues(3, added)
		t.EqualValues(0, left)

		t.Equal([]int{1, 2, 3}, done())
	})

	t.Run("variadic, over", func() {
		add, done, err := AppendSlice[int](3, 0)
		t.NoError(err)

		added, left := add(1, 2, 3, 4)
		t.EqualValues(3, added)
		t.EqualValues(0, left)

		t.Equal([]int{1, 2, 3}, done())
	})

	t.Run("none", func() {
		_, done, err := AppendSlice[int](3, 0)
		t.NoError(err)

		t.Nil(done())
	})

	t.Run("less", func() {
		add, done, err := AppendSlice[int](3, 0)
		t.NoError(err)

		added, left := add(1)
		t.EqualValues(1, added)
		t.EqualValues(2, left)

		added, left = add(2)
		t.EqualValues(1, added)
		t.EqualValues(1, left)

		t.Equal(2, len(done()))
		t.Equal([]int{1, 2}, done())
	})

	t.Run("over size", func() {
		add, done, err := AppendSlice[int](3, 0)
		t.NoError(err)

		added, left := add(1)
		t.EqualValues(1, added)
		t.EqualValues(2, left)

		added, left = add(2)
		t.EqualValues(1, added)
		t.EqualValues(1, left)

		added, left = add(3)
		t.EqualValues(1, added)
		t.EqualValues(0, left)

		added, left = add(4)
		t.EqualValues(0, added)
		t.EqualValues(0, left)

		added, left = add(6)
		t.EqualValues(0, added)
		t.EqualValues(0, left)

		t.Equal([]int{1, 2, 3}, done())
	})

	t.Run("grow", func() {
		var grow uint64 = 1

		add, done, err := AppendSlice[int](3, grow)
		t.NoError(err)

		added, left := add(1)
		t.EqualValues(1, added)
		t.EqualValues(2, left)

		added, left = add(2)
		t.EqualValues(1, added)
		t.EqualValues(1, left)

		added, left = add(3)
		t.EqualValues(1, added)
		t.EqualValues(grow, left)

		added, left = add(4)
		t.EqualValues(1, added)
		t.EqualValues(grow, left)

		added, left = add(5)
		t.EqualValues(1, added)
		t.EqualValues(grow, left)

		added, left = add(6)
		t.EqualValues(1, added)
		t.EqualValues(grow, left)

		added, left = add(7)
		t.EqualValues(1, added)
		t.EqualValues(grow, left)

		t.Equal([]int{1, 2, 3, 4, 5, 6, 7}, done())
	})

	t.Run("async", func() {
		var size uint64 = 3333
		add, done, err := AppendSlice[uint64](size, 0)
		t.NoError(err)

		var wg sync.WaitGroup

		expected := make([]uint64, size)
		for i := uint64(0); i < size; i++ {
			i := i

			expected[i] = i

			wg.Add(1)

			go func() {
				defer wg.Done()

				_, _ = add(i)
			}()
		}

		wg.Wait()

		s := done()
		t.EqualValues(size, len(s))
		slices.Sort(s)

		t.Equal(expected, s)
	})

	t.Run("async, grow", func() {
		var size uint64 = 3333
		add, done, err := AppendSlice[uint64](3, 3)
		t.NoError(err)

		var wg sync.WaitGroup

		expected := make([]uint64, size)
		for i := uint64(0); i < size; i++ {
			i := i

			expected[i] = i

			wg.Add(1)

			go func() {
				defer wg.Done()

				_, _ = add(i)
			}()
		}

		wg.Wait()

		s := done()
		t.EqualValues(size, len(s))
		slices.Sort(s)

		t.Equal(expected, s)
	})
}

func (t *testAppendSlice) TestRAppendSlice() {
	t.Run("ok", func() {
		var add AppendSliceAddFunc[int]
		var done OutFunc[[]int]

		t.NoError(RAppendSlice[int](3, 0, &add, &done))

		added, left := add(1)
		t.EqualValues(1, added)
		t.EqualValues(2, left)

		t.Equal([]int{1}, done())
	})

	t.Run("wrong type", func() {
		var add AppendSliceAddFunc[int]

		err := RAppendSlice[int](3, 0, &add, &add)
		t.ErrorContains(err, "AppendSliceAddFunc[int]")
	})
}

func TestAppendSlice(t *testing.T) {
	defer goleak.VerifyNone(t)

	suite.Run(t, new(testAppendSlice))
}
