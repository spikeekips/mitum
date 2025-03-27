package util

import (
	"fmt"
	"slices"
	"sync"
	"testing"

	"github.com/stretchr/testify/suite"
	"go.uber.org/goleak"
)

type testBaseAppendSlice struct {
	BaseSuite
}

func (t *testBaseAppendSlice) TestError() {
	t.Run("error, size 0", func() {
		_, _, err := newBaseAppendSlice[int](0, 0, nil)
		t.ErrorContains(err, "size and grow under 1")
	})

	t.Run("error, size, grow 0", func() {
		_, _, err := newBaseAppendSlice[int](0, 0, nil)
		t.ErrorContains(err, "size and grow under 1")
	})

	t.Run("no error, size 0, but grow over 0", func() {
		_, _, err := newBaseAppendSlice[int](0, 1, nil)
		t.NoError(err)
	})
}

func (t *testBaseAppendSlice) TestGrowOver0() {
	t.Run("size < 1 and grow = 1", func() {
		add, done, err := newBaseAppendSlice[int](0, 1, TrueFilter)
		t.NoError(err)

		added, left := add(1)
		t.EqualValues(1, added)
		t.EqualValues(0, left)

		added, left = add(2)
		t.EqualValues(1, added)
		t.EqualValues(0, left)

		added, left = add(3)
		t.EqualValues(1, added)
		t.EqualValues(0, left)

		added, left = add(4)
		t.EqualValues(1, added)
		t.EqualValues(0, left)

		t.Equal([]int{1, 2, 3, 4}, done())
	})

	t.Run("size < 1 and grow = 2", func() {
		add, done, err := newBaseAppendSlice[int](0, 2, TrueFilter)
		t.NoError(err)

		added, left := add(1)
		t.EqualValues(1, added)
		t.EqualValues(1, left)

		added, left = add(2)
		t.EqualValues(1, added)
		t.EqualValues(0, left)

		added, left = add(3)
		t.EqualValues(1, added)
		t.EqualValues(1, left)

		added, left = add(4)
		t.EqualValues(1, added)
		t.EqualValues(0, left)

		t.Equal([]int{1, 2, 3, 4}, done())
	})
}

func (t *testBaseAppendSlice) TestAdd() {
	t.Run("add none", func() {
		_, done, err := newBaseAppendSlice[int](3, 0, TrueFilter)
		t.NoError(err)

		t.Equal(0, len(done()))
	})

	t.Run("add after done", func() {
		add, done, err := newBaseAppendSlice[int](3, 0, TrueFilter)
		t.NoError(err)

		added, left := add(1)
		t.EqualValues(1, added)
		t.EqualValues(2, left)

		t.Equal([]int{1}, done())

		added, left = add(2)
		t.EqualValues(0, added)
		t.EqualValues(0, left)

		added, left = add(3)
		t.EqualValues(0, added)
		t.EqualValues(0, left)

		t.Equal([]int{1}, done())
	})

	t.Run("add one", func() {
		add, done, err := newBaseAppendSlice[int](3, 0, TrueFilter)
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

		// add more
		added, left = add(4)
		t.EqualValues(0, added)
		t.EqualValues(0, left)

		t.Equal([]int{1, 2, 3}, done())
	})

	t.Run("add multiple", func() {
		add, done, err := newBaseAppendSlice[int](3, 0, TrueFilter)
		t.NoError(err)

		added, left := add(1, 2, 3)
		t.EqualValues(3, added)
		t.EqualValues(0, left)

		// add more
		added, left = add(4, 5, 6)
		t.EqualValues(0, added)
		t.EqualValues(0, left)

		t.Equal([]int{1, 2, 3}, done())
	})
}

func (t *testBaseAppendSlice) TestFilter() {
	t.Run("grow = 0", func() {
		t.Run("add one", func() {
			add, done, err := newBaseAppendSlice[int](3, 0, func(i int) bool {
				switch {
				case i%2 == 0:
					return false
				default:
					return true
				}
			})
			t.NoError(err)

			added, left := add(0)
			t.EqualValues(0, added)
			t.EqualValues(3, left)

			added, left = add(1)
			t.EqualValues(1, added)
			t.EqualValues(2, left)

			added, left = add(2)
			t.EqualValues(0, added)
			t.EqualValues(2, left)

			added, left = add(3)
			t.EqualValues(1, added)
			t.EqualValues(1, left)

			added, left = add(4)
			t.EqualValues(0, added)
			t.EqualValues(1, left)

			added, left = add(5)
			t.EqualValues(1, added)
			t.EqualValues(0, left)

			t.Equal([]int{1, 3, 5}, done())
		})

		t.Run("add multiple", func() {
			add, done, err := newBaseAppendSlice[int](3, 0, func(i int) bool {
				switch {
				case i%2 == 0:
					return false
				default:
					return true
				}
			})
			t.NoError(err)

			added, left := add(0, 2, 4)
			t.EqualValues(0, added)
			t.EqualValues(3, left)

			added, left = add(1, 3, 5)
			t.EqualValues(3, added)
			t.EqualValues(0, left)

			t.Equal([]int{1, 3, 5}, done())
		})
	})

	t.Run("grow > 0", func() {
		t.Run("add one", func() {
			add, done, err := newBaseAppendSlice[int](3, 1, func(i int) bool {
				switch {
				case i%2 == 0:
					return false
				default:
					return true
				}
			})
			t.NoError(err)

			added, left := add(0)
			t.EqualValues(0, added)
			t.EqualValues(3, left)

			added, left = add(1)
			t.EqualValues(1, added)
			t.EqualValues(2, left)

			added, left = add(2)
			t.EqualValues(0, added)
			t.EqualValues(2, left)

			added, left = add(3)
			t.EqualValues(1, added)
			t.EqualValues(1, left)

			added, left = add(4)
			t.EqualValues(0, added)
			t.EqualValues(1, left)

			added, left = add(5)
			t.EqualValues(1, added)
			t.EqualValues(0, left)

			t.Equal([]int{1, 3, 5}, done())
		})

		t.Run("add multiple", func() {
			add, done, err := newBaseAppendSlice[int](3, 0, func(i int) bool {
				switch {
				case i%2 == 0:
					return false
				default:
					return true
				}
			})
			t.NoError(err)

			added, left := add(0, 2, 4)
			t.EqualValues(0, added)
			t.EqualValues(3, left)

			added, left = add(1, 3, 5)
			t.EqualValues(3, added)
			t.EqualValues(0, left)

			t.Equal([]int{1, 3, 5}, done())
		})
	})
}

func TestBaseAppendSlice(t *testing.T) {
	defer goleak.VerifyNone(t)

	suite.Run(t, new(testBaseAppendSlice))
}

type testLockAppendSlice struct {
	BaseSuite
}

func (t *testLockAppendSlice) TestAdd() {
	t.Run("async", func() {
		var size uint64 = 3333
		add, done, err := NewLockAppendSlice[uint64](size, 0)
		t.NoError(err)

		var wg sync.WaitGroup

		expected := make([]uint64, size)
		for i := uint64(0); i < size; i++ {
			expected[i] = i

			wg.Add(1)

			go func() {
				defer wg.Done()

				added, _ := add(i)
				if added < 1 {
					t.Panicf(nil, "not added, %d", i)
				}
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
		add, done, err := NewLockAppendSlice[uint64](3, 33)
		t.NoError(err)

		var wg sync.WaitGroup

		expected := make([]uint64, size)
		for i := uint64(0); i < size; i++ {
			expected[i] = i

			wg.Add(1)

			go func() {
				defer wg.Done()

				added, _ := add(i)
				if added < 1 {
					t.Panicf(nil, "not added, %d", i)
				}
			}()
		}

		wg.Wait()

		s := done()
		t.EqualValues(size, len(s))
		slices.Sort(s)

		t.Equal(expected, s)
	})

	t.Run("async; multiple add", func() {
		owners := [][]string{
			{"w0", "w3", "w7"},
			{"w0", "w4", "w7"},
			{"w0", "w4", "w8"},
			{"w1", "w4", "w8"},
			{"w1", "w5", "w8"},
			{"w1", "w5", "w9"},
			{"w2", "w5", "w9"},
			{"w2", "w6", "w9"},
			{"w2", "w6", "w10"},
			{"w3", "w6", "w10"},
			{"w3", "w7", "w10"},
		}
		expected := []string{"w0", "w1", "w10", "w2", "w3", "w4", "w5", "w6", "w7", "w8", "w9"}

		add, done, err := NewLockAppendSlice[string](uint64(len(expected)), 1<<9)
		t.NoError(err)

		var wg sync.WaitGroup

		for i := range owners {
			wg.Add(1)

			go func() {
				defer wg.Done()

				v := owners[i]

				added, left := add(v...)
				if added < 1 {
					t.Panicf(nil, "not added, %d", i)
				}

				t.Print("add: %d owners=%v added=%d left=%d", i, v, added, left)
			}()
		}

		wg.Wait()

		s := done()
		t.Print("done: %v", s)

		slices.Sort(s)
		s = slices.Compact(s)

		t.True(equalStringSlice(expected, s), "expected=%v actual=%v", expected, s)
	})
}

func TestLockAppendSlice(t *testing.T) {
	defer goleak.VerifyNone(t)

	suite.Run(t, new(testLockAppendSlice))
}

type testUniqueAppendSlice struct {
	BaseSuite
}

func (t *testUniqueAppendSlice) TestAdd() {
	t.Run("non-comparable", func() {
		add, done, err := NewUniqueAppendSlice[int, string](3, 0, func(i int) string {
			return fmt.Sprintf("%d", i)
		})
		t.NoError(err)

		var added, left uint64

		added, left = add(1)
		t.EqualValues(1, added)
		t.EqualValues(2, left)

		added, left = add(1)
		t.EqualValues(0, added)
		t.EqualValues(2, left)

		added, left = add(3)
		t.EqualValues(1, added)
		t.EqualValues(1, left)

		t.Equal([]int{1, 3}, done())
	})

	t.Run("comparable", func() {
		t.Run("ok", func() {
			add, done, err := NewComparableUniqueAppendSlice[int](3, 0)
			t.NoError(err)

			var added, left uint64

			added, left = add(1)
			t.EqualValues(1, added)
			t.EqualValues(2, left)

			added, left = add(1)
			t.EqualValues(0, added)
			t.EqualValues(2, left)

			added, left = add(3)
			t.EqualValues(1, added)
			t.EqualValues(1, left)

			t.Equal([]int{1, 3}, done())
		})

		t.Run("done again", func() {
			add, done, err := NewComparableUniqueAppendSlice[int](3, 0)
			t.NoError(err)

			var added, left uint64

			added, left = add(1)
			t.EqualValues(1, added)
			t.EqualValues(2, left)

			added, left = add(1)
			t.EqualValues(0, added)
			t.EqualValues(2, left)

			t.Equal([]int{1}, done())

			added, left = add(3)
			t.EqualValues(0, added)
			t.EqualValues(0, left)

			t.Equal([]int{1}, done())
		})
	})
}

func TestUniqueAppendSlice(t *testing.T) {
	defer goleak.VerifyNone(t)

	suite.Run(t, new(testUniqueAppendSlice))
}

type testLockUniqueAppendSlice struct {
	BaseSuite
}

func (t *testLockUniqueAppendSlice) TestAdd() {
	t.Run("async", func() {
		var size uint64 = 3333
		add, done, err := NewLockComparableUniqueAppendSlice[uint64](size, 0)
		t.NoError(err)

		var wg sync.WaitGroup

		expected := make([]uint64, size)
		for i := uint64(0); i < size; i++ {
			expected[i] = i

			wg.Add(2)

			go func() {
				defer wg.Done()

				_, _ = add(i)
			}()

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

		s = done()
		t.EqualValues(size, len(s))
		slices.Sort(s)

		t.Equal(expected, s)
	})

	t.Run("async, grow", func() {
		var size uint64 = 3333
		add, done, err := NewLockComparableUniqueAppendSlice[uint64](3, 33)
		t.NoError(err)

		var wg sync.WaitGroup

		expected := make([]uint64, size)
		for i := uint64(0); i < size; i++ {
			expected[i] = i

			wg.Add(1)

			go func() {
				defer wg.Done()

				added, _ := add(i)
				if added < 1 {
					t.Panicf(nil, "not added, %d", i)
				}
			}()
		}

		wg.Wait()

		s := done()
		t.EqualValues(size, len(s))
		slices.Sort(s)

		t.Equal(expected, s)
	})

	t.Run("async; multiple add", func() {
		owners := [][]string{
			{"w0", "w3", "w7"},
			{"w0", "w4", "w7"},
			{"w0", "w4", "w8"},
			{"w1", "w4", "w8"},
			{"w1", "w5", "w8"},
			{"w1", "w5", "w9"},
			{"w2", "w5", "w9"},
			{"w2", "w6", "w9"},
			{"w2", "w6", "w10"},
			{"w3", "w6", "w10"},
			{"w3", "w7", "w10"},
		}
		expected := []string{"w0", "w1", "w10", "w2", "w3", "w4", "w5", "w6", "w7", "w8", "w9"}

		add, done, err := NewLockComparableUniqueAppendSlice[string](uint64(len(expected)), 1<<9)
		t.NoError(err)

		var wg sync.WaitGroup

		for i := range owners {
			wg.Add(1)

			go func() {
				defer wg.Done()

				v := owners[i]

				added, left := add(v...)
				t.Print("add: %d owners=%v added=%d left=%d", i, v, added, left)
			}()
		}

		wg.Wait()

		s := done()
		t.Print("done: %v", s)

		slices.Sort(s)
		s = slices.Compact(s)

		t.True(equalStringSlice(expected, s), "expected=%v actual=%v", expected, s)

		s = done()
		t.Print("done: %v", s)

		slices.Sort(s)
		s = slices.Compact(s)

		t.True(equalStringSlice(expected, s), "expected=%v actual=%v", expected, s)
	})
}

func TestLockUniqueAppendSlice(t *testing.T) {
	defer goleak.VerifyNone(t)

	suite.Run(t, new(testLockUniqueAppendSlice))
}

func equalStringSlice[T comparable](a, b []T) bool {
	if len(a) != len(b) {
		return false
	}

	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}

	return true
}

func newFuncRunBenchmarkAppendSlice(bb *testing.B) func(string, func(uint64, *testing.B)) {
	return func(name string, f func(uint64, *testing.B)) {
		for n := uint64(10); n <= 10000; n *= 10 {
			bb.Run(fmt.Sprintf(name+"%d", n), func(b *testing.B) {
				for b.Loop() {
					f(n, b)
				}
			})
		}
	}
}

func BenchmarkSliceAppend(bb *testing.B) {
	run := newFuncRunBenchmarkAppendSlice(bb)

	run("", func(n uint64, b *testing.B) {
		s := make([]uint64, n)

		for i := range n {
			s[i] = i
		}
	})
}

func BenchmarkBaseAppendSliceFullInitialSize(bb *testing.B) {
	run := newFuncRunBenchmarkAppendSlice(bb)

	run("", func(n uint64, b *testing.B) {
		b.StopTimer()
		b.StartTimer()

		add, _, _ := newBaseAppendSlice[uint64](n, 0, TrueFilter)

		for i := range n {
			if added, _ := add(i); added < 1 {
				panic("not added")
			}
		}
	})

	run("after new: ", func(n uint64, b *testing.B) {
		b.StopTimer()
		add, _, _ := newBaseAppendSlice[uint64](n, 0, TrueFilter)
		b.StartTimer()

		for i := range n {
			if added, _ := add(i); added < 1 {
				panic("not added")
			}
		}
	})

	run("done: ", func(n uint64, b *testing.B) {
		b.StopTimer()
		add, done, _ := newBaseAppendSlice[uint64](n, 0, TrueFilter)

		for i := range n {
			if added, _ := add(i); added < 1 {
				panic("not added")
			}
		}

		b.StartTimer()

		_ = done()
	})
}

func BenchmarkBaseAppendSliceGrow(bb *testing.B) {
	run := newFuncRunBenchmarkAppendSlice(bb)

	run("", func(n uint64, b *testing.B) {
		b.StopTimer()
		b.StartTimer()

		add, _, _ := newBaseAppendSlice[uint64](n/2, n/3, TrueFilter)

		for i := range n {
			if added, _ := add(i); added < 1 {
				panic("not added")
			}
		}
	})

	run("after new: ", func(n uint64, b *testing.B) {
		b.StopTimer()
		add, _, _ := newBaseAppendSlice[uint64](n/2, n/3, TrueFilter)
		b.StartTimer()

		for i := range n {
			if added, _ := add(i); added < 1 {
				panic("not added")
			}
		}
	})

	run("done: ", func(n uint64, b *testing.B) {
		b.StopTimer()
		add, done, _ := newBaseAppendSlice[uint64](n/2, n/3, TrueFilter)

		for i := range n {
			if added, _ := add(i); added < 1 {
				panic("not added")
			}
		}

		b.StartTimer()

		_ = done()
	})
}

func newAppendSliceNoAlloc[T any](grow uint64, filter func(T) bool) (
	add AppendSliceAddFunc[T],
	done OutFunc[[]T],
) {
	s := make([]T, 0, grow)

	var next uint64

	return func(ts ...T) (added, _ uint64) {
			for t := range slices.Values(ts) {
				if !filter(t) {
					continue
				}

				s = append(s, t)
				added++
				next++
			}

			return added, 0
		},
		func() []T {
			if next < 1 {
				return nil
			}

			s = s[:next]

			return s
		}
}

func BenchmarkAppendSliceNoAlloc(bb *testing.B) {
	run := newFuncRunBenchmarkAppendSlice(bb)

	run("", func(n uint64, b *testing.B) {
		b.StopTimer()
		b.StartTimer()

		add, _ := newAppendSliceNoAlloc[uint64](n, TrueFilter)

		for i := range n {
			if added, _ := add(i); added < 1 {
				panic("not added")
			}
		}
	})

	run("after new: ", func(n uint64, b *testing.B) {
		b.StopTimer()
		add, _ := newAppendSliceNoAlloc[uint64](n, TrueFilter)
		b.StartTimer()

		for i := range n {
			if added, _ := add(i); added < 1 {
				panic("not added")
			}
		}
	})

	run("done: ", func(n uint64, b *testing.B) {
		b.StopTimer()
		add, done := newAppendSliceNoAlloc[uint64](n, TrueFilter)

		for i := range n {
			if added, _ := add(i); added < 1 {
				panic("not added")
			}
		}
		b.StartTimer()

		_ = done()
	})
}
