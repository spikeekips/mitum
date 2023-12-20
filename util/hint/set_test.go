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
	hs := NewCompatibleSet[Hinter](0)

	hr := newHinter("showme", "v2019.10")
	t.NoError(hs.AddHinter(hr))

	hr0 := newHinter("findme", "v2019.10")
	t.NoError(hs.Add(hr0.Hint(), hr0))

	hr1 := newHinter("findme", "v2020.10")
	t.NoError(hs.Add(hr1.Hint(), hr1))

	ehr0, found := hs.Find(EnsureParseHint("findme-v2019.9"))
	t.True(found)
	t.Equal(hr0, ehr0)
	ehr1, found := hs.Find(EnsureParseHint("findme-v2020.9"))
	t.True(found)
	t.Equal(hr1, ehr1)
}

func (t *testCompatibleSet) TestAddHinter() {
	hs := NewCompatibleSet[Hinter](3)

	hr := newHinter("showme", "v2019.10")
	t.NoError(hs.Add(hr.Hint(), hr))

	hr0 := newHinter("findme", "v2019.10")
	t.NoError(hs.AddHinter(hr0))

	hr1 := newHinter("findme", "v2020.10")
	t.NoError(hs.AddHinter(hr1))

	ehr0, found := hs.Find(EnsureParseHint("findme-v2019.9"))
	t.True(found)
	t.Equal(hr0, ehr0)
	ehr1, found := hs.Find(EnsureParseHint("findme-v2020.9"))
	t.True(found)
	t.Equal(hr1, ehr1)

	t.Run("unexpected", func() {
		hs := NewCompatibleSet[int](3)

		hr := newHinter("showme", "v2019.10")
		err := hs.AddHinter(hr)
		t.Error(err)
		t.ErrorContains(err, "expected int")
	})
}

func (t *testCompatibleSet) TestAdd() {
	hs := NewCompatibleSet[int](3)

	ht := newHint("showme", "v2019.10")
	t.NoError(hs.Add(ht, 1))

	ht0 := newHint("findme", "v2019.10")
	t.NoError(hs.Add(ht0, 2))

	ht1 := newHint("findme", "v2020.10")
	t.NoError(hs.Add(ht1, 3))

	v0, found := hs.Find(EnsureParseHint("findme-v2019.9"))
	t.True(found)
	t.Equal(2, v0)
	v1, found := hs.Find(EnsureParseHint("findme-v2020.9"))
	t.True(found)
	t.Equal(3, v1)
}

func (t *testCompatibleSet) TestAlreadyAdded() {
	hs := NewCompatibleSet[Hinter](3)

	hr := newHinter("showme", "v2019.10")
	t.NoError(hs.Add(hr.Hint(), hr))
	err := hs.Add(hr.Hint(), hr)
	t.Error(err)
	t.ErrorContains(err, "already added")
}

func (t *testCompatibleSet) TestFind() {
	hs := NewCompatibleSet[Hinter](3)

	hr := newHinter("showme", "v2019.10")
	t.NoError(hs.Add(hr.Hint(), hr))

	t.Run("found", func() {
		v, found := hs.Find(hr.Hint())
		t.True(found)
		t.NotNil(v)

		t.True(hr.Hint().Equal(v.Hint()))
	})

	t.Run("not found", func() {
		v, found := hs.Find(EnsureParseHint("findme-v0.0.0"))
		t.False(found)
		t.Nil(v)

		v, found = hs.Find(EnsureParseHint("findme-v0.0.0"))
		t.False(found)
		t.Nil(v)
	})
}

func (t *testCompatibleSet) TestFindByString() {
	hs := NewCompatibleSet[Hinter](3)

	hr := newHinter("showme", "v2019.10")
	t.NoError(hs.Add(hr.Hint(), hr))

	t.Run("ok", func() {
		ht, v, found, err := hs.FindByString(hr.Hint().String())
		t.True(found)
		t.NoError(err)
		t.NotNil(v)

		t.True(hr.Hint().Equal(v.Hint()))
		t.True(hr.Hint().Equal(ht))
	})

	t.Run("wrong hint", func() {
		_, v, found, err := hs.FindByString("hehehe")
		t.False(found)
		t.Error(err)
		t.Nil(v)
		t.ErrorContains(err, "empty version")
	})
}

func (t *testCompatibleSet) TestFindHead() {
	hs := NewCompatibleSet[Hinter](3)

	hr0 := newHinter("showme", "v2019.10")
	t.NoError(hs.Add(hr0.Hint(), hr0))

	hr1 := newHinter("showme", "v2019.11")
	t.NoError(hs.Add(hr1.Hint(), hr1))

	v0, found := hs.Find(hr0.Hint())
	t.True(found)
	t.NotNil(v0)

	t.True(hr1.Hint().Equal(v0.Hint()))

	v1, found := hs.Find(hr1.Hint())
	t.True(found)
	t.NotNil(v1)

	t.True(hr1.Hint().Equal(v1.Hint()))
}

func (t *testCompatibleSet) TestFindByType() {
	hs := NewCompatibleSet[Hinter](3)

	hr0 := newHinter("showme", "v2019.10")
	t.NoError(hs.Add(hr0.Hint(), hr0))

	uht, v, found := hs.FindBytType(hr0.Hint().Type())
	t.True(found)

	t.True(hr0.Hint().Equal(uht))
	t.True(hr0.Hint().Equal(v.Hint()))
}

func (t *testCompatibleSet) TestFindByTypeString() {
	hs := NewCompatibleSet[Hinter](3)

	hr0 := newHinter("showme", "v2019.10")
	t.NoError(hs.Add(hr0.Hint(), hr0))

	t.Run("ok", func() {
		uht, v, found := hs.FindBytType(hr0.Hint().Type())
		t.True(found)

		t.True(hr0.Hint().Equal(uht))
		t.True(hr0.Hint().Equal(v.Hint()))
	})

	t.Run("wrong type", func() {
		_, _, found, err := hs.FindBytTypeString("k")
		t.False(found)
		t.Error(err)
		t.ErrorContains(err, "too short Type")

		_, _, found, err = hs.FindBytTypeString("k")
		t.False(found)
		t.Error(err)
		t.ErrorContains(err, "too short Type")
	})
}

func (t *testCompatibleSet) TestFindByTypeSameMajor() {
	hs := NewCompatibleSet[Hinter](3)

	hr0 := newHinter("showme", "v2019.10")
	t.NoError(hs.Add(hr0.Hint(), hr0))
	hr1 := newHinter("showme", "v2019.11")
	t.NoError(hs.Add(hr1.Hint(), hr1))

	uht, v, found := hs.FindBytType(hr0.Hint().Type())
	t.True(found)

	t.True(hr1.Hint().Equal(uht))
	t.True(hr1.Hint().Equal(v.Hint()))
}

func (t *testCompatibleSet) TestFindByTypeLowerMinor() {
	hs := NewCompatibleSet[Hinter](3)

	hr0 := newHinter("showme", "v2019.10")
	t.NoError(hs.Add(hr0.Hint(), hr0))
	hr1 := newHinter("showme", "v2019.9")
	t.NoError(hs.Add(hr1.Hint(), hr1))

	uht, v, found := hs.FindBytType(hr0.Hint().Type())
	t.True(found)

	t.True(hr0.Hint().Equal(uht))
	t.True(hr0.Hint().Equal(v.Hint()))
}

func (t *testCompatibleSet) TestFindByTypeHigerMajor() {
	hs := NewCompatibleSet[Hinter](3)

	hr0 := newHinter("showme", "v2019.10")
	t.NoError(hs.Add(hr0.Hint(), hr0))
	hr1 := newHinter("showme", "v2020.01")
	t.NoError(hs.Add(hr1.Hint(), hr1))

	uht, v, found := hs.FindBytType(hr0.Hint().Type())
	t.True(found)

	t.True(hr1.Hint().Equal(uht))
	t.True(hr1.Hint().Equal(v.Hint()))
}

func (t *testCompatibleSet) TestFindByTypeLowerMajor() {
	hs := NewCompatibleSet[Hinter](3)

	hr0 := newHinter("showme", "v2019.10")
	t.NoError(hs.Add(hr0.Hint(), hr0))
	hr1 := newHinter("showme", "v2018.11")
	t.NoError(hs.Add(hr1.Hint(), hr1))

	uht, v, found := hs.FindBytType(hr0.Hint().Type())
	t.True(found)

	t.True(hr0.Hint().Equal(uht))
	t.True(hr0.Hint().Equal(v.Hint()))
}

func (t *testCompatibleSet) TestFindCompatible() {
	hs := NewCompatibleSet[Hinter](3)

	ts := "showme"
	vs := "v2019.10.11"
	hr := newHinter(ts, vs)
	t.NoError(hs.Add(hr.Hint(), hr))

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

				ehr, found := hs.Find(ht)
				t.Equal(c.found, found, "%d(%q): %v", i, c.s, c.name)
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
