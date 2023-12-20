package hint

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/pkg/errors"
	"github.com/spikeekips/mitum/util"
	"github.com/stretchr/testify/suite"
)

type testHint struct {
	suite.Suite
}

func (t *testHint) version(s string) util.Version {
	return util.EnsureParseVersion(s)
}

func (t *testHint) TestNew() {
	ht := NewHint("showme", t.version("v1.2.3+compatible"))
	t.NoError(ht.IsValid(nil))
	t.Equal("showme-v1.2.3+compatible", ht.String())
}

func (t *testHint) TestParse() {
	cases := []struct {
		name     string
		s        string
		expected string
		err      string
	}{
		{name: "valid", s: "showme-v1.2.3+incompatible"},
		{name: "ended with whitespace", s: "showme-v1.2.3 "},
		{name: "started with whitespace", s: " showme-v1.2.3 "},
		{name: "empty version #0", s: "showme", err: "empty version"},
		{name: "empty version #1", s: "showme-", err: "empty version"},
		{name: "inside v+hyphen", s: "sho-vwme-v1.2.3+incompatible", expected: "sho-vwme-v1.2.3+incompatible"},
	}

	for i, c := range cases {
		i := i
		c := c
		t.Run(
			c.name,
			func() {
				ht, err := ParseHint(c.s)
				if len(c.err) > 0 {
					if err == nil {
						t.NoError(errors.Errorf("expected %q, but nil error", c.err), "%d(%q): %v", i, c.s, c.name)

						return
					}

					t.ErrorContains(err, c.err, "%d(%q): %v", i, c.s, c.name)

					return
				} else if err != nil {
					t.NoError(errors.Errorf("expected nil error, but %+v", err), "%d(%q): %v", i, c.s, c.name)

					return
				}

				if len(c.expected) > 0 {
					t.Equal(c.expected, ht.String(), "%d(%q): %v", i, c.s, c.name)
				}
			},
		)
	}
}

func (t *testHint) TestIsValid() {
	cases := []struct {
		name     string
		s        string
		expected string
		err      string
	}{
		{name: "hyphen-v", s: "sho-v1.2.3wme-v1.2.3+incompatible", expected: "sho-v1.2.3wme-v1.2.3+incompatible", err: "empty version"},
		{name: "too long version", s: "showme-v1.2.3+" + strings.Repeat("a", MaxVersionLength-6), err: "too long version"},
	}

	for i, c := range cases {
		i := i
		c := c
		t.Run(
			c.name,
			func() {
				ht, err := ParseHint(c.s)
				t.NoError(err)

				err = ht.IsValid(nil)
				if len(c.err) > 0 {
					if err == nil {
						t.NoError(errors.Errorf("expected %q, but nil error", c.err), "%d(%q): %v", i, c.s, c.name)

						return
					}

					t.ErrorContains(err, c.err, "%d(%q): %v", i, c.s, c.name)

					return
				} else if err != nil {
					t.NoError(errors.Errorf("expected nil error, but %+v", err), "%d(%q): %v", i, c.s, c.name)

					return
				}

				if len(c.expected) > 0 {
					t.Equal(c.expected, ht.String(), "%d(%q): %v", i, c.s, c.name)
				}
			},
		)
	}
}

func (t *testHint) TestCompatible() {
	cases := []struct {
		name string
		a    string
		b    string
		r    bool
	}{
		{name: "upper patch version", a: "showme-v1.2.3", b: "showme-v1.2.4", r: true},
		{name: "same patch version", a: "showme-v1.2.3", b: "showme-v1.2.3", r: true},
		{name: "lower patch version", a: "showme-v1.2.3", b: "showme-v1.2.2", r: true},
		{name: "upper minor version", a: "showme-v1.2.3", b: "showme-v1.3.4", r: true},
		{name: "same minor version", a: "showme-v1.2.3", b: "showme-v1.2.9", r: true},
		{name: "lower minor version", a: "showme-v1.2.3", b: "showme-v1.1.2", r: true},
		{name: "upper major version", a: "showme-v1.2.3", b: "showme-v9.3.4", r: false},
		{name: "same major version", a: "showme-v1.2.3", b: "showme-v10.2.9", r: false},
		{name: "lower major version", a: "showme-v1.2.3", b: "showme-v2.1.2", r: false},
	}

	for i, c := range cases {
		i := i
		c := c
		t.Run(
			c.name,
			func() {
				a, _ := ParseHint(c.a)
				b, _ := ParseHint(c.b)

				r := a.IsCompatible(b)

				t.Equal(c.r, r, "%d: %v; %q ~~ %q", i, c.name, c.a, c.b)
			},
		)
	}
}

func (t *testHint) TestEncodeJSON() {
	ht, err := ParseHint("showme-v1.2.3+compatible")
	t.NoError(err)

	b, err := util.MarshalJSON(map[string]interface{}{
		"_hint": ht,
	})
	t.NoError(err)

	var m map[string]Hint
	t.NoError(json.Unmarshal(b, &m))

	uht := m["_hint"]
	t.True(ht.Equal(uht))
}

func TestHint(t *testing.T) {
	suite.Run(t, new(testHint))
}
