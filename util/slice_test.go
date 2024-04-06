package util

import (
	"fmt"
	"reflect"
	"sync"
	"testing"

	"github.com/gofrs/uuid"
	"github.com/pkg/errors"
	"github.com/stretchr/testify/suite"
	"go.uber.org/goleak"
	"golang.org/x/exp/slices"
)

func TestIsDuplicatedSlice(tt *testing.T) {
	t := new(suite.Suite)
	t.SetT(tt)

	var nilslice []int

	cases := []struct {
		s        interface{}
		k        func(interface{}) (bool, string)
		name     string
		expected bool
	}{
		{name: "invalid: nil", s: nil, expected: false},
		{name: "invalid: nil slice", s: nilslice, expected: false},
		{name: "slice: not duplicated", s: []int{5, 4, 3, 2, 1}, expected: false, k: func(i interface{}) (bool, string) {
			return true, fmt.Sprintf("%d", i)
		}},
		{name: "slice: duplicated", s: []int{4, 4, 3, 2, 1}, expected: true, k: func(i interface{}) (bool, string) {
			return true, fmt.Sprintf("%d", i)
		}},
		{name: "array: not duplicated", s: [5]int{5, 4, 3, 2, 1}, expected: false, k: func(i interface{}) (bool, string) {
			return true, fmt.Sprintf("%d", i)
		}},
		{name: "array: duplicated", s: [5]int{4, 4, 3, 2, 1}, expected: true, k: func(i interface{}) (bool, string) {
			return true, fmt.Sprintf("%d", i)
		}},
		{name: "slice: nil", s: []interface{}{4, nil, 3, 2, 1}, expected: false, k: func(i interface{}) (bool, string) {
			return true, fmt.Sprintf("%d", i)
		}},
		{name: "slice: nils", s: []interface{}{nil, nil, 3, 2, 1}, expected: true, k: func(i interface{}) (bool, string) {
			return true, fmt.Sprintf("%d", i)
		}},
	}

	for i, c := range cases {
		i := i
		c := c
		t.Run(c.name, func() {
			sl := makeInterfaceSlice(c.s)

			isduplicated := IsDuplicatedSlice(sl, c.k)
			t.Equal(c.expected, isduplicated, "%d(%q): %v", i, c.name, c.s)
		})
	}

	t.Run("stop", func() {
		sl := []string{"a", "b", "c"}

		var n int

		isduplicated := IsDuplicatedSlice(sl, func(i string) (bool, string) {
			if n == 2 {
				return false, i
			}

			n++

			return true, i
		})

		t.False(isduplicated)
		t.Equal(2, n)
	})
}

func TestFilter2Slices(tt *testing.T) {
	t := new(suite.Suite)
	t.SetT(tt)

	t.Run("nil & nil", func() {
		o := Filter2Slices(
			nil,
			nil,
			func(a, b int) bool {
				return a == b
			},
		)

		t.Equal(0, len(o))
	})

	t.Run("[1,2,3], [2,3]", func() {
		o := Filter2Slices(
			[]int{1, 2, 3},
			[]int{2, 3},
			func(a, b int) bool {
				return a == b
			},
		)

		t.Equal(1, len(o))
		t.Equal([]int{1}, o)
	})

	t.Run("[1,2,3], [2,3,4]", func() {
		o := Filter2Slices(
			[]int{1, 2, 3},
			[]int{2, 3, 4},
			func(a, b int) bool {
				return a == b
			},
		)

		t.Equal(1, len(o))
		t.Equal([]int{1}, o)
	})
}

func makeInterfaceSlice(s interface{}) []interface{} {
	v := reflect.ValueOf(s)

	switch v.Kind() {
	case reflect.Slice, reflect.Array:
		if v.Len() < 1 {
			return nil
		}

		l := make([]interface{}, v.Len())
		for i := 0; i < v.Len(); i++ {
			l[i] = v.Index(i).Interface()
		}

		return l
	default:
		return nil
	}
}

type testRandomChoiceSlice struct {
	suite.Suite
}

func (t *testRandomChoiceSlice) newslice(size int) []int {
	n := make([]int, size)

	for i := range n {
		n[i] = i
	}

	return n
}

func (t *testRandomChoiceSlice) isInside(a []int, b []int) bool {
	for i := range b {
		if slices.Index(a, b[i]) < 0 {
			return false
		}
	}

	return true
}

func (t *testRandomChoiceSlice) isDuplicated(a []int) bool {
	checked := map[int]struct{}{}

	for i := range a {
		if _, found := checked[a[i]]; found {
			return true
		}

		checked[a[i]] = struct{}{}
	}

	return false
}

func (t *testRandomChoiceSlice) TestZeroSize() {
	a := t.newslice(33)

	_, err := RandomChoiceSlice(a, 0)
	t.Error(err)
}

func (t *testRandomChoiceSlice) TestSameSize() {
	for i := range make([]int, 33) {
		a := t.newslice(33)

		b, err := RandomChoiceSlice(a, int64(len(a)))
		t.NoError(err)
		t.Equal(len(a), len(b))
		t.False(t.isDuplicated(b))
		t.True(t.isInside(a, b))

		t.T().Logf("%d input: %v", i, a)
		t.T().Logf("%d output: %v", i, b)
	}
}

func (t *testRandomChoiceSlice) TestLessSize() {
	for i := range make([]int, 33) {
		a := t.newslice(33)

		var size int64
	inside:
		for {
			size, _ = randInt64(int64(len(a)) - 1)
			if size < 1 {
				continue inside
			}

			break inside
		}

		b, err := RandomChoiceSlice(a, size)
		t.NoError(err)
		t.Equal(size, int64(len(b)))
		t.False(t.isDuplicated(b))
		t.True(t.isInside(a, b))

		t.T().Logf("%d input: %v", i, a)
		t.T().Logf("%d output: %v", i, b)
	}
}

func (t *testRandomChoiceSlice) TestOverSize() {
	for i := range make([]int, 33) {
		a := t.newslice(33)

		b, err := RandomChoiceSlice(a, 66)
		t.NoError(err)
		t.Equal(len(a), len(b))
		t.False(t.isDuplicated(b))
		t.True(t.isInside(a, b))

		t.T().Logf("%d input: %v", i, a)
		t.T().Logf("%d output: %v", i, b)
	}
}

func TestRandomChoiceSlice(t *testing.T) {
	suite.Run(t, new(testRandomChoiceSlice))
}

func TestRemoveDuplicated(tt *testing.T) {
	t := new(suite.Suite)
	t.SetT(tt)

	uuids := []interface{}{UUID(), UUID(), UUID(), UUID(), UUID()}
	uuids_dup := make([]interface{}, len(uuids)+1)
	copy(uuids_dup, uuids)

	uuids_dup[len(uuids)] = uuids[len(uuids)-1]

	cases := []struct {
		name     string
		s        interface{}
		f        func(interface{}) (string, error)
		expected interface{}
		err      string
	}{
		{
			name: "not duplicated",
			s:    []int{5, 4, 3, 2, 1},
			f: func(i interface{}) (string, error) {
				return fmt.Sprintf("%d", i), nil
			},
			expected: []interface{}{5, 4, 3, 2, 1},
		},
		{
			name: "duplicated: int",
			s:    []int{5, 4, 3, 2, 1, 5},
			f: func(i interface{}) (string, error) {
				return fmt.Sprintf("%d", i), nil
			},
			expected: []interface{}{5, 4, 3, 2, 1},
		},
		{
			name: "duplicated: UUID",
			s:    uuids_dup,
			f: func(i interface{}) (string, error) {
				return i.(uuid.UUID).String(), nil
			},
			expected: uuids,
		},
		{name: "empty", s: []int{}, expected: ([]interface{})(nil)},
		{
			name: "error",
			s:    []int{5, 4, 3, 2, 1, 5},
			f: func(i interface{}) (string, error) {
				if fmt.Sprintf("%d", i) == "3" {
					return "", errors.Errorf("findme")
				}

				return fmt.Sprintf("%d", i), nil
			},
			err: "findme",
		},
	}

	for i, c := range cases {
		i := i
		c := c
		t.Run(c.name, func() {
			sl := makeInterfaceSlice(c.s)

			r, err := RemoveDuplicatedSlice(sl, c.f)

			if len(c.err) > 0 {
				t.Error(err)
				t.ErrorContains(err, c.err, "%d, %s: %v", i, c.name)

				return
			}

			t.Equal(r, c.expected, "%d, %s", i, c.name)
		})
	}
}

type testCompactAppendSliceV1 struct {
	suite.Suite
}

func (t *testCompactAppendSliceV1) TestCompactAppendSliceV1() {
	t.Run("error, size 0", func() {
		_, _, err := CompactAppendSliceV1[int](0)
		t.ErrorContains(err, "size and grow under 1")
	})

	t.Run("error, size, grow 0", func() {
		_, _, err := CompactAppendSliceV1[int](0, 0)
		t.ErrorContains(err, "size and grow under 1")
	})

	t.Run("no error, size 0, but grow over 0", func() {
		_, _, err := CompactAppendSliceV1[int](0, 1)
		t.NoError(err)
	})

	t.Run("size < 1 and grow > 0", func() {
		var grow uint64 = 1

		add, done, err := CompactAppendSliceV1[int](0, grow)
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
		add, done, err := CompactAppendSliceV1[int](3)
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

	t.Run("nil done", func() {
		add, done, err := CompactAppendSliceV1[int](3)
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
		add, done, err := CompactAppendSliceV1[int](3)
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
		add, done, err := CompactAppendSliceV1[int](3)
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
		add, done, err := CompactAppendSliceV1[int](3)
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
		_, done, err := CompactAppendSliceV1[int](3)
		t.NoError(err)

		t.NoError(done(func(s []int) error {
			t.Nil(s)

			return nil
		}))
	})

	t.Run("less", func() {
		add, done, err := CompactAppendSliceV1[int](3)
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
		add, done, err := CompactAppendSliceV1[int](3)
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

		add, done, err := CompactAppendSliceV1[int](3, grow)
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
		add, done, err := CompactAppendSliceV1[uint64](size)
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
		add, done, err := CompactAppendSliceV1[uint64](3, 3)
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

func TestCompactAppendSliceV1(t *testing.T) {
	defer goleak.VerifyNone(t)

	suite.Run(t, new(testCompactAppendSliceV1))
}
