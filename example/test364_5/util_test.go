package main

import (
	"slices"
	"sync"
	"testing"

	"github.com/stretchr/testify/suite"
	"go.uber.org/goleak"
)

type testCompactAppendSlice struct {
	suite.Suite
}

func (t *testCompactAppendSlice) TestCompactAppendSlice() {
	t.Run("error, size 0", func() {
		_, _, err := compactAppendSlice[int](0, 0)
		t.ErrorContains(err, "size and grow under 1")
	})

	t.Run("error, size, grow 0", func() {
		_, _, err := compactAppendSlice[int](0, 0)
		t.ErrorContains(err, "size and grow under 1")
	})

	t.Run("no error, size 0, but grow over 0", func() {
		_, _, err := compactAppendSlice[int](0, 1)
		t.NoError(err)
	})

	t.Run("size < 1 and grow > 0", func() {
		var grow uint64 = 1

		add, done, err := compactAppendSlice[int](0, grow)
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

		t.NoError(done(func(s []int) error {
			t.Equal([]int{1, 2, 3, 4}, s)

			return nil
		}))
	})

	t.Run("ok", func() {
		add, done, err := compactAppendSlice[int](3, 0)
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

		t.NoError(done(func(s []int) error {
			t.Equal([]int{1, 2, 3}, s)

			return nil
		}))
	})

	t.Run("add after done", func() {
		add, done, err := compactAppendSlice[int](3, 0)
		t.NoError(err)

		added, left := add(1)
		t.EqualValues(1, added)
		t.EqualValues(2, left)

		added, left = add(2)
		t.EqualValues(1, added)
		t.EqualValues(1, left)

		t.NoError(done(func(s []int) error {
			t.Equal([]int{1, 2}, s)

			return nil
		}))

		added, left = add(3)
		t.EqualValues(0, added)
		t.EqualValues(0, left)

		t.NoError(done(func(s []int) error {
			t.Equal(0, len(s))

			return nil
		}))
	})

	t.Run("nil done", func() {
		add, done, err := compactAppendSlice[int](3, 0)
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

		t.NoError(done(nil))
	})

	t.Run("variadic, less", func() {
		add, done, err := compactAppendSlice[int](3, 0)
		t.NoError(err)

		added, left := add(1, 2)
		t.EqualValues(2, added)
		t.EqualValues(1, left)

		t.NoError(done(func(s []int) error {
			t.Equal([]int{1, 2}, s)

			return nil
		}))
	})

	t.Run("variadic, full", func() {
		add, done, err := compactAppendSlice[int](3, 0)
		t.NoError(err)

		added, left := add(1, 2, 3)
		t.EqualValues(3, added)
		t.EqualValues(0, left)

		t.NoError(done(func(s []int) error {
			t.Equal([]int{1, 2, 3}, s)

			return nil
		}))
	})

	t.Run("variadic, over", func() {
		add, done, err := compactAppendSlice[int](3, 0)
		t.NoError(err)

		added, left := add(1, 2, 3, 4)
		t.EqualValues(3, added)
		t.EqualValues(0, left)

		t.NoError(done(func(s []int) error {
			t.Equal([]int{1, 2, 3}, s)

			return nil
		}))
	})

	t.Run("none", func() {
		_, done, err := compactAppendSlice[int](3, 0)
		t.NoError(err)

		t.NoError(done(func(s []int) error {
			t.Nil(s)

			return nil
		}))
	})

	t.Run("less", func() {
		add, done, err := compactAppendSlice[int](3, 0)
		t.NoError(err)

		added, left := add(1)
		t.EqualValues(1, added)
		t.EqualValues(2, left)

		added, left = add(2)
		t.EqualValues(1, added)
		t.EqualValues(1, left)

		t.NoError(done(func(s []int) error {
			t.Equal(2, len(s))
			t.Equal([]int{1, 2}, s)

			return nil
		}))
	})

	t.Run("over size", func() {
		add, done, err := compactAppendSlice[int](3, 0)
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

		t.NoError(done(func(s []int) error {
			t.Equal([]int{1, 2, 3}, s)

			return nil
		}))
	})

	t.Run("grow", func() {
		var grow uint64 = 1

		add, done, err := compactAppendSlice[int](3, grow)
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

		t.NoError(done(func(s []int) error {
			t.Equal([]int{1, 2, 3, 4, 5, 6, 7}, s)

			return nil
		}))
	})

	t.Run("async", func() {
		var size uint64 = 3333
		add, done, err := compactAppendSlice[uint64](size, 0)
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

		t.NoError(done(func(s []uint64) error {
			t.EqualValues(size, len(s))
			slices.Sort(s)

			t.Equal(expected, s)

			return nil
		}))
	})

	t.Run("async, grow", func() {
		var size uint64 = 3333
		add, done, err := compactAppendSlice[uint64](3, 3)
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

		t.NoError(done(func(s []uint64) error {
			t.EqualValues(size, len(s))
			slices.Sort(s)

			t.Equal(expected, s)

			return nil
		}))
	})
}

func (t *testCompactAppendSlice) TestRCompactAppendSlice() {
	t.Run("ok", func() {
		var add compactAppendSliceAddFunc[int]
		var done compactAppendSliceDoneFunc[int]

		t.NoError(rCompactAppendSlice[int](3, 0, &add, &done))

		added, left := add(1)
		t.EqualValues(1, added)
		t.EqualValues(2, left)

		t.NoError(done(func(s []int) error {
			t.Equal([]int{1}, s)

			return nil
		}))
	})

	t.Run("wrong type", func() {
		var add compactAppendSliceAddFunc[int]

		err := rCompactAppendSlice[int](3, 0, &add, &add)
		t.ErrorContains(err, "compactAppendSliceAddFunc[int]")
	})
}

func TestCompactAppendSlice(t *testing.T) {
	defer goleak.VerifyNone(t)

	suite.Run(t, new(testCompactAppendSlice))
}
