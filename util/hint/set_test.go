package hint

import (
	"testing"

	"github.com/spikeekips/mitum/util"
	"github.com/stretchr/testify/suite"
)

type hinter struct {
	h Hint
}

func newHint(t string, v string) Hint {
	return NewHint(Type(t), util.EnsureParseVersion(v))
}

func newHinter(t string, v string) hinter {
	return hinter{h: newHint(t, v)}
}

func (sh hinter) Hint() Hint {
	return sh.h
}

type testCompatibleSet struct {
	suite.Suite
}

func (t *testCompatibleSet) TestNilCache() {
	hs := NewCompatibleSet(0)

	hr := newHinter("showme", "v2019.10")
	t.NoError(hs.AddHinter(hr))

	hr0 := newHinter("findme", "v2019.10")
	t.NoError(hs.AddHinter(hr0))

	hr1 := newHinter("findme", "v2020.10")
	t.NoError(hs.AddHinter(hr1))

	ehr0 := hs.Find(EnsureParseHint("findme-v2019.9"))
	t.Equal(hr0, ehr0)
	ehr1 := hs.Find(EnsureParseHint("findme-v2020.9"))
	t.Equal(hr1, ehr1)
}

func (t *testCompatibleSet) TestAddHinter() {
	hs := NewCompatibleSet(3)

	hr := newHinter("showme", "v2019.10")
	t.NoError(hs.AddHinter(hr))

	hr0 := newHinter("findme", "v2019.10")
	t.NoError(hs.AddHinter(hr0))

	hr1 := newHinter("findme", "v2020.10")
	t.NoError(hs.AddHinter(hr1))

	ehr0 := hs.Find(EnsureParseHint("findme-v2019.9"))
	t.Equal(hr0, ehr0)
	ehr1 := hs.Find(EnsureParseHint("findme-v2020.9"))
	t.Equal(hr1, ehr1)
}

func (t *testCompatibleSet) TestAdd() {
	hs := NewCompatibleSet(3)

	ht := newHint("showme", "v2019.10")
	t.NoError(hs.Add(ht, 1))

	ht0 := newHint("findme", "v2019.10")
	t.NoError(hs.Add(ht0, 2))

	ht1 := newHint("findme", "v2020.10")
	t.NoError(hs.Add(ht1, 3))

	v0 := hs.Find(EnsureParseHint("findme-v2019.9"))
	t.Equal(2, v0)
	v1 := hs.Find(EnsureParseHint("findme-v2020.9"))
	t.Equal(3, v1)
}

func (t *testCompatibleSet) TestAlreadyAdded() {
	hs := NewCompatibleSet(3)

	hr := newHinter("showme", "v2019.10")
	t.NoError(hs.AddHinter(hr))
	err := hs.AddHinter(hr)
	t.Error(err)
	t.ErrorContains(err, "already added")
}

func (t *testCompatibleSet) TestFind() {
	hs := NewCompatibleSet(3)

	hr := newHinter("showme", "v2019.10")
	t.NoError(hs.AddHinter(hr))

	v := hs.Find(hr.Hint())
	t.NotNil(v)

	uhr, ok := v.(Hinter)
	t.True(ok)
	t.True(hr.Hint().Equal(uhr.Hint()))
}

func (t *testCompatibleSet) TestFindHead() {
	hs := NewCompatibleSet(3)

	hr0 := newHinter("showme", "v2019.10")
	t.NoError(hs.AddHinter(hr0))

	hr1 := newHinter("showme", "v2019.11")
	t.NoError(hs.AddHinter(hr1))

	v0 := hs.Find(hr0.Hint())
	t.NotNil(v0)

	uhr, ok := v0.(Hinter)
	t.True(ok)
	t.True(hr1.Hint().Equal(uhr.Hint()))

	v1 := hs.Find(hr1.Hint())
	t.NotNil(v1)

	uhr, ok = v1.(Hinter)
	t.True(ok)
	t.True(hr1.Hint().Equal(uhr.Hint()))
}

func (t *testCompatibleSet) TestFindByType() {
	hs := NewCompatibleSet(3)

	hr0 := newHinter("showme", "v2019.10")
	t.NoError(hs.AddHinter(hr0))

	uht, v := hs.FindBytType(hr0.Hint().Type())

	uhr, ok := v.(Hinter)
	t.True(ok)
	t.True(hr0.Hint().Equal(uht))
	t.True(hr0.Hint().Equal(uhr.Hint()))
}

func (t *testCompatibleSet) TestFindByTypeSameMajor() {
	hs := NewCompatibleSet(3)

	hr0 := newHinter("showme", "v2019.10")
	t.NoError(hs.AddHinter(hr0))
	hr1 := newHinter("showme", "v2019.11")
	t.NoError(hs.AddHinter(hr1))

	uht, v := hs.FindBytType(hr0.Hint().Type())

	uhr, ok := v.(Hinter)
	t.True(ok)
	t.True(hr1.Hint().Equal(uht))
	t.True(hr1.Hint().Equal(uhr.Hint()))
}

func (t *testCompatibleSet) TestFindByTypeLowerMinor() {
	hs := NewCompatibleSet(3)

	hr0 := newHinter("showme", "v2019.10")
	t.NoError(hs.AddHinter(hr0))
	hr1 := newHinter("showme", "v2019.9")
	t.NoError(hs.AddHinter(hr1))

	uht, v := hs.FindBytType(hr0.Hint().Type())

	uhr, ok := v.(Hinter)
	t.True(ok)
	t.True(hr0.Hint().Equal(uht))
	t.True(hr0.Hint().Equal(uhr.Hint()))
}

func (t *testCompatibleSet) TestFindByTypeHigerMajor() {
	hs := NewCompatibleSet(3)

	hr0 := newHinter("showme", "v2019.10")
	t.NoError(hs.AddHinter(hr0))
	hr1 := newHinter("showme", "v2020.01")
	t.NoError(hs.AddHinter(hr1))

	uht, v := hs.FindBytType(hr0.Hint().Type())

	uhr, ok := v.(Hinter)
	t.True(ok)
	t.True(hr1.Hint().Equal(uht))
	t.True(hr1.Hint().Equal(uhr.Hint()))
}

func (t *testCompatibleSet) TestFindByTypeLowerMajor() {
	hs := NewCompatibleSet(3)

	hr0 := newHinter("showme", "v2019.10")
	t.NoError(hs.AddHinter(hr0))
	hr1 := newHinter("showme", "v2018.11")
	t.NoError(hs.AddHinter(hr1))

	uht, v := hs.FindBytType(hr0.Hint().Type())

	uhr, ok := v.(Hinter)
	t.True(ok)
	t.True(hr0.Hint().Equal(uht))
	t.True(hr0.Hint().Equal(uhr.Hint()))
}

func (t *testCompatibleSet) TestFindCompatible() {
	hs := NewCompatibleSet(3)

	ts := "showme"
	vs := "v2019.10.11"
	hr := newHinter(ts, vs)
	t.NoError(hs.AddHinter(hr))

	cases := []struct {
		name  string
		s     string
		found bool
	}{
		{name: "equal", s: hr.Hint().String(), found: true},
		{name: "different type", s: "findme-" + vs, found: false},
		{name: "higher major", s: ts + "-v2020.01", found: false},
		{name: "lower major", s: ts + "-v2018.12", found: false},
		{name: "higher minor", s: ts + "-v2019.12", found: true},
		{name: "lower minor", s: ts + "-v2019.9", found: true},
		{name: "higher patch", s: ts + "-v2019.10.12", found: true},
		{name: "lower patch", s: ts + "-v2019.10.10", found: true},
	}

	for i, c := range cases {
		i := i
		c := c
		t.Run(
			c.name,
			func() {
				ht, _ := ParseHint(c.s)

				ehr := hs.Find(ht)
				if c.found {
					t.NotNil(ehr, "%d(%q): %v", i, c.s, c.name)
					t.Equal(hr, ehr)
				} else {
					t.Nil(ehr, "%d(%q): %v", i, c.s, c.name)
				}
			},
		)
	}
}

func TestCompatibleSet(t *testing.T) {
	suite.Run(t, new(testCompatibleSet))
}
