package util

import (
	"fmt"
	"reflect"
	"testing"

	"github.com/gofrs/uuid"
	"github.com/pkg/errors"
	"github.com/stretchr/testify/suite"
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

			_, isduplicated := IsDuplicatedSlice(sl, c.k)
			t.Equal(c.expected, isduplicated, "%d(%q): %v", i, c.name, c.s)
		})
	}

	t.Run("stop", func() {
		sl := []string{"a", "b", "c"}

		var n int

		_, isduplicated := IsDuplicatedSlice(sl, func(i string) (bool, string) {
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
		if slices.Index[int](a, b[i]) < 0 {
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
