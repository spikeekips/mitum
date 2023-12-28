package launch

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/suite"
)

func TestRangeFlag(tt *testing.T) {
	t := new(suite.Suite)
	t.SetT(tt)

	var zero *uint64
	var three *uint64
	{
		var i uint64 = 0
		var j uint64 = 3

		zero = &i
		three = &j
	}

	cases := []struct {
		name     string
		s        string
		expected []*uint64
		err      string
	}{
		{name: "empty", s: "", expected: nil},
		{name: "blank", s: "           \t", expected: nil},
		{name: "ok", s: "0-3", expected: []*uint64{zero, three}},
		{name: "ok: missing to", s: "0-", expected: []*uint64{zero, nil}},
		{name: "ok: missing from", s: "-3", expected: []*uint64{nil, three}},
		{name: "multiple hyphen", s: "0---4", err: "invalid syntax"},
		{name: "not digit", s: "a", err: "invalid syntax"},
	}

	for i, c := range cases {
		i := i
		c := c
		t.Run(c.name, func() {
			var r RangeFlag
			err := r.UnmarshalText([]byte(c.s))

			if len(c.err) > 0 {
				if err == nil {
					t.Fail(fmt.Sprintf("expected error, but nil error: %+v", c.err), "%d: %v", i, c.name)

					return
				}

				t.ErrorContains(err, c.err, "%d: %v", i, c.name)

				return
			}

			if err != nil {
				t.Fail(fmt.Sprintf("expected nil error, but %+v", err), "%d: %v", i, c.name)

				return
			}

			var from, to *uint64

			if c.expected != nil {
				if len(c.expected) > 0 {
					from = c.expected[0]
				}

				if len(c.expected) > 1 {
					to = c.expected[1]
				}
			}

			t.Equal(from, r.from, "%d: %v; %v != %v", i, c.name, from, r.from)
			t.Equal(to, r.to, "%d: %v; %v != %v", i, c.name, to, r.to)
		})
	}
}
