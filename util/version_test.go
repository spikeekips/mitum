package util

import (
	"encoding/json"
	"testing"

	"github.com/pkg/errors"
	"github.com/stretchr/testify/suite"
)

type testVersion struct {
	suite.Suite
}

func (t *testVersion) TestParseError() {
	o := "va.b.c"
	_, err := ParseVersion(o)
	t.Error(err)
	t.ErrorContains(err, "Invalid Semantic Version")
	t.True(errors.Is(err, ErrInvalid))
}

func (t *testVersion) TestEnsureParseVersion() {
	{ // invalid version string
		o := "a.b.c"
		v := EnsureParseVersion(o)
		err := v.IsValid(nil)
		t.Error(err)
		t.ErrorContains(err, "empty version string")
		t.True(errors.Is(err, ErrInvalid))
	}

	{ // valid version string
		o := "v1.2.33"
		v := EnsureParseVersion(o)
		t.NoError(v.IsValid(nil))
	}
}

func (t *testVersion) TestWithoutPrefix() {
	o := "v1.2.3-alpha.4+1ca74f7ba1ce143efec26ae161dbb4de"
	v, err := ParseVersion(o)
	t.NoError(err)

	t.Equal(o, v.String())
	t.Equal(v.major, uint64(1))
	t.Equal(v.minor, uint64(2))
	t.Equal(v.patch, uint64(3))
	t.Equal(v.prerelease, "alpha.4")
}

func (t *testVersion) TestParse() {
	cases := []struct {
		name       string
		s          string
		prerelease string
		err        string
		major      uint64
		minor      uint64
		patch      uint64
	}{
		{name: "simple", s: "v1.2.3", major: 1, minor: 2, patch: 3, prerelease: ""},
		{name: "not v prefix", s: "1.2.3", err: "version"},
		{name: "go style", s: "v1.2.3", major: 1, minor: 2, patch: 3, prerelease: ""},
		{name: "+prerelease", s: "v1.2.3-alpha", major: 1, minor: 2, patch: 3, prerelease: "alpha"},
		{name: "+prerelease+version", s: "v1.2.3-alpha.4", major: 1, minor: 2, patch: 3, prerelease: "alpha.4"},
		{name: "+numeric prerelease", s: "v1.2.3-4", major: 1, minor: 2, patch: 3, prerelease: "4"},
		{name: "+prerelease+hypen+version", s: "v1.2.3-alpha-4", major: 1, minor: 2, patch: 3, prerelease: "alpha-4"},
		{name: "not numeric", s: "va.2.3", err: "invalid"},
	}

	for i, c := range cases {
		i := i
		c := c
		t.Run(
			c.name,
			func() {
				v, err := ParseVersion(c.s)
				if len(c.err) > 0 {
					if err == nil {
						t.NoError(errors.Errorf("error expected, %q, but nil error", c.err), "%d(%q): %v", i, c.s, c.name)

						return
					}

					t.ErrorContains(err, c.err, "%d(%q): %v; %+v", i, c.s, c.name, err)

					return
				}

				if err != nil {
					t.NoError(errors.Errorf("expected nil error, but %+v", err), "%d(%q): %v", i, c.s, c.name)

					return
				}

				t.Equal(c.major, v.major, "major mismatch: %d(%q): %v; %d != %d", i, c.s, c.name, c.major, v.major)
				t.Equal(c.minor, v.minor, "minor mismatch: %d(%q): %v; %d != %d", i, c.s, c.name, c.minor, v.minor)
				t.Equal(c.patch, v.patch, "patch mismatch: %d(%q): %v; %d != %d", i, c.s, c.name, c.patch, v.patch)
				t.Equal(c.prerelease, v.prerelease, "prerelease mismatch: %d(%q): %v; %d != %d", i, c.s, c.name, c.prerelease, v.prerelease)
			},
		)
	}
}

func (t *testVersion) TestString() {
	cases := []struct {
		name string
		s    string
		o    string
	}{
		{name: "with v", s: "v1.2.3", o: "v1.2.3"},
		{name: "major", s: "v1", o: "v1.0.0"},
		{name: "minor", s: "v1.2", o: "v1.2.0"},
		{name: "patch", s: "v1.2.3", o: "v1.2.3"},
		{name: "prerelease#0", s: "v1.2.3-beta", o: "v1.2.3-beta"},
		{name: "prerelease#1", s: "v1.2.3-beta0", o: "v1.2.3-beta0"},
		{name: "prerelease#2", s: "v1.2.3-beta.0", o: "v1.2.3-beta.0"},
		{name: "with build", s: "v1.2.3-beta.0+1ca74f7ba1ce143efec26ae161dbb4de", o: "v1.2.3-beta.0+1ca74f7ba1ce143efec26ae161dbb4de"},
	}

	for i, c := range cases {
		i := i
		c := c
		t.Run(
			c.name,
			func() {
				v, err := ParseVersion(c.s)
				t.NoError(err)

				t.Equal(v.String(), c.o, "mismatch: %d(%q): %v; %q != %q", i, c.s, c.name, v.String(), c.o)
			},
		)
	}
}

func (t *testVersion) TestIsValid() {
	cases := []struct {
		name string
		s    string
		err  string
	}{
		{name: "valid#0", s: "v1.2.3"},
		{name: "valid#1", s: "v1"},
		{name: "empty", s: "", err: "empty version string"},
		{name: "empty with blank#0", s: "  ", err: "empty version string"},
		{name: "empty with blank#1", s: "\t\t", err: "empty version string"},
		{name: "without v", s: "1.2.3", err: "version"},
		{name: "invalid semver#0", s: "va.2.3", err: "invalid semver"},
		{name: "invalid semver#1", s: "v1=beta", err: "invalid semver"},
	}

	for i, c := range cases {
		i := i
		c := c
		t.Run(
			c.name,
			func() {
				v := Version{s: c.s}
				err := v.IsValid(nil)
				if len(c.err) > 0 {
					if err == nil {
						t.NoError(errors.Errorf("error expected, %q, but nil error", c.err), "%d(%q): %v", i, c.s, c.name)

						return
					}

					t.True(errors.Is(err, ErrInvalid))
					t.ErrorContains(err, c.err, "%d(%q): %v; %+v", i, c.s, c.name, err)

					return
				}

				if err != nil {
					t.NoError(errors.Errorf("expected nil error, but %+v", err), "%d(%q): %v", i, c.s, c.name)

					return
				}
			},
		)
	}
}

func (t *testVersion) TestCompare() {
	cases := []struct {
		name string
		a    string
		b    string
		r    int
	}{
		{name: "same", a: "v1.2.3", b: "v1.2.3", r: 0},
		{name: "major mismatch", a: "v1.2.3", b: "v2.2.3", r: -1},
		{name: "minor lower", a: "v1.2.3", b: "v1.0.3", r: 1},
		{name: "minor higher", a: "v1.2.3", b: "v1.3.3", r: -1},
		{name: "patch lower", a: "v1.2.3", b: "v1.2.0", r: 1},
		{name: "patch higher", a: "v1.2.3", b: "v1.2.4", r: -1},
		{name: "prerelease#0", a: "v1.2.3-beta0", b: "v1.2.3-beta.0", r: 1},
		{name: "prerelease lower", a: "v1.2.3-beta.0", b: "v1.2.0-beta", r: 1},
		{name: "prerelease higher", a: "v1.2.3-beta", b: "v1.2.4-beta.0", r: -1},
		{name: "with build", a: "v1.2.3-beta.0+1ca74f7ba1ce143efec26ae161dbb4de", b: "v1.2.3-beta.0", r: 0},
	}

	for i, c := range cases {
		i := i
		c := c
		t.Run(
			c.name,
			func() {
				a, err := ParseVersion(c.a)
				if err != nil {
					t.NoError(err)

					return
				}

				b, err := ParseVersion(c.b)
				if err != nil {
					t.NoError(err)

					return
				}

				r := a.Compare(b)

				t.Equal(c.r, r, "compare: %d(%q): %v; %q vs %q; %d != %d", i, c.a, c.name, c.a, c.b, c.r, r)
			},
		)
	}
}

func (t *testVersion) TestIsCompatible() {
	cases := []struct {
		name string
		a    string
		b    string
		r    bool
	}{
		{name: "same", a: "v1.2.3", b: "v1.2.3", r: true},
		{name: "major mismatch", a: "v1.2.3", b: "v2.2.3", r: false},
		{name: "minor lower", a: "v1.2.3", b: "v1.0.3", r: true},
		{name: "minor higher", a: "v1.2.3", b: "v1.3.3", r: true},
		{name: "patch lower", a: "v1.2.3", b: "v1.2.0", r: true},
		{name: "patch higher", a: "v1.2.3", b: "v1.2.4", r: true},
		{name: "prerelease#0", a: "v1.2.3-beta0", b: "v1.2.0-beta.0", r: true},
		{name: "prerelease lower", a: "v1.2.3-beta.0", b: "v1.2.0-beta", r: true},
		{name: "prerelease higher", a: "v1.2.3-beta", b: "v1.2.4-beta.0", r: true},
		{name: "with build", a: "v1.2.3-beta.0+1ca74f7ba1ce143efec26ae161dbb4de", b: "v1.2.3-beta.0", r: true},
	}

	for i, c := range cases {
		i := i
		c := c
		t.Run(
			c.name,
			func() {
				a, err := ParseVersion(c.a)
				if err != nil {
					t.NoError(err)

					return
				}

				b, err := ParseVersion(c.b)
				if err != nil {
					t.NoError(err)

					return
				}

				r := a.IsCompatible(b)

				t.Equal(c.r, r, "is compatible: %d(%q): %v; %q vs %q; %v != %v", i, c.a, c.name, c.a, c.b, c.r, r)
			},
		)
	}
}

func (t *testVersion) TestMarshalJSON() {
	v, err := ParseVersion("v1.2.3-beta.0+1ca74f7ba1ce143efec26ae161dbb4de")
	t.NoError(err)

	b, err := MarshalJSON(v)
	t.NoError(err)

	var ub Version
	t.NoError(json.Unmarshal(b, &ub))

	t.Equal(v.String(), ub.String())
	t.True(v.Compare(ub) == 0)
}

func TestVersion(t *testing.T) {
	suite.Run(t, new(testVersion))
}
