package util

import (
	"fmt"
	"reflect"
	"testing"

	"github.com/stretchr/testify/suite"
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
		l := make([]interface{}, v.Len())
		for i := 0; i < v.Len(); i++ {
			l[i] = v.Index(i).Interface()
		}

		return l
	default:
		return nil
	}
}
