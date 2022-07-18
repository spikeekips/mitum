package util

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/suite"
)

func TestCheckSliceDuplicated(tt *testing.T) {
	t := new(suite.Suite)
	t.SetT(tt)

	var nilslice []int

	cases := []struct {
		s        interface{}
		k        func(interface{}, int) string
		name     string
		expected bool
	}{
		{name: "invalid: string", s: "string", expected: false},
		{name: "invalid: int", s: 333, expected: false},
		{name: "invalid: nil", s: nil, expected: false},
		{name: "invalid: nil slice", s: nilslice, expected: false},
		{name: "slice: not duplicated", s: []int{5, 4, 3, 2, 1}, expected: false, k: func(i interface{}, _ int) string {
			return fmt.Sprintf("%d", i)
		}},
		{name: "slice: duplicated", s: []int{4, 4, 3, 2, 1}, expected: true, k: func(i interface{}, _ int) string {
			return fmt.Sprintf("%d", i)
		}},
		{name: "array: not duplicated", s: [5]int{5, 4, 3, 2, 1}, expected: false, k: func(i interface{}, _ int) string {
			return fmt.Sprintf("%d", i)
		}},
		{name: "array: duplicated", s: [5]int{4, 4, 3, 2, 1}, expected: true, k: func(i interface{}, _ int) string {
			return fmt.Sprintf("%d", i)
		}},
		{name: "slice: nil", s: []interface{}{4, nil, 3, 2, 1}, expected: false, k: func(i interface{}, _ int) string {
			return fmt.Sprintf("%d", i)
		}},
		{name: "slice: nils", s: []interface{}{nil, nil, 3, 2, 1}, expected: true, k: func(i interface{}, _ int) string {
			return fmt.Sprintf("%d", i)
		}},
	}

	for i, c := range cases {
		i := i
		c := c
		t.Run(c.name, func() {
			_, isduplicated := CheckSliceDuplicated(c.s, c.k)
			t.Equal(c.expected, isduplicated, "%d(%q): %v", i, c.name, c.s)
		})
	}
}
