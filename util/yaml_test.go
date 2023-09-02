package util

import (
	"testing"

	"github.com/stretchr/testify/suite"
	"gopkg.in/yaml.v3"
)

type testYAMLOrderedMap struct {
	suite.Suite
}

func (t *testYAMLOrderedMap) TestMarshal() {
	t.Run("map", func() {
		m := map[string]interface{}{
			"Z": []int{0, 1, 2, 3},
			"Y": []string{"0", "1", "2"},
			"X": "showme",
		}

		b, err := yaml.Marshal(m)
		t.NoError(err)

		t.T().Logf("marshaled:\n%s", string(b))
	})

	t.Run("YAMLOrderedMap", func() {
		m := NewYAMLOrderedMap()
		m.Set("Z", []int{0, 1, 2, 3})
		m.Set("Y", []string{"0", "1", "2"})
		m.Set("X", "showme")

		b, err := yaml.Marshal(m)
		t.NoError(err)

		t.T().Logf("marshaled:\n%s", string(b))
	})
}

func (t *testYAMLOrderedMap) TestUnmarshal() {
	t.Run("map", func() {
		b := []byte(`Z:
    - 0
    - 1
    - 2
    - 3
"Y":
    - "4"
    - "5"
    - "6"
X: showme
W:
    b: b
    a: a
    d:
        C: 3
        B: 2
        A: 1
    c:
        - 10
        - 11
        - 12
`)
		t.T().Logf("yaml:\n%s", string(b))

		var m *YAMLOrderedMap
		t.NoError(yaml.Unmarshal(b, &m))

		ub, err := yaml.Marshal(m)
		t.NoError(err)

		t.T().Logf("marshaled:\n%s", string(ub))

		t.Equal(b, ub)
	})

	t.Run("under list", func() {
		b := []byte(`Z:
    - 0
    - 1
    - 2
    - g: g
      f: f
      e: e
A: null
"Y":
    - "4"
    - "5"
    - "6"
X: showme
W:
    b: b
    a: a
    d:
        C: 3
        B: 2
        A: 1
    c:
        - 10
        - 11
        - 12
`)
		t.T().Logf("yaml:\n%s", string(b))

		var m *YAMLOrderedMap
		t.NoError(yaml.Unmarshal(b, &m))

		ub, err := yaml.Marshal(m)
		t.NoError(err)

		t.T().Logf("marshaled:\n%s", string(ub))

		t.Equal(b, ub)
	})

	t.Run("[]byte -> YAMLOrderedMap", func() {
		b := []byte(`
a: 1
c: 2
d:
  e: 3
  f: 4
`)

		i, err := UnmarshalWithYAMLOrderedMap(b)
		t.NoError(err)

		m, ok := i.(*YAMLOrderedMap)
		t.True(ok)

		a, found := m.Get("a")
		t.True(found)
		t.Equal(1, a)

		c, found := m.Get("c")
		t.True(found)
		t.Equal(2, c)

		d, found := m.Get("d")
		t.True(found)

		dm, ok := d.(*YAMLOrderedMap)
		t.True(ok)

		e, found := dm.Get("e")
		t.True(found)
		t.Equal(3, e)

		f, found := dm.Get("f")
		t.True(found)
		t.Equal(4, f)
	})

	t.Run("[]byte -> nil", func() {
		b := []byte(`
`)

		i, err := UnmarshalWithYAMLOrderedMap(b)
		t.NoError(err)

		t.Nil(i)
	})

	t.Run("[]byte -> slice", func() {
		b := []byte(`
- a
- c
`)

		i, err := UnmarshalWithYAMLOrderedMap(b)
		t.NoError(err)

		l, ok := i.([]interface{})
		t.True(ok, "%T", i)

		t.Equal(l, []interface{}{"a", "c"})
	})
}

func (t *testYAMLOrderedMap) TestNested() {
	b := []byte(`
Z:
  - 0
  - 1
  - 2
  - 3
Y: ["4", "5", "6"]
X: "showme"
W:
  b: b
  a: a
  d:
    C: 3
    B: 2
    A: 1
  c:
    - 10
    - 11
    - 12
`)
	t.T().Logf("yaml:\n%s", string(b))

	var m *YAMLOrderedMap
	t.NoError(yaml.Unmarshal(b, &m))

	i, found := m.Get("W")
	t.True(found)
	w, ok := i.(*YAMLOrderedMap)
	t.True(ok)
	i, found = w.Get("d")
	t.True(found)
	udm, ok := i.(*YAMLOrderedMap)
	t.True(ok)

	t.Equal(3, udm.Len())

	dm := map[string]int{"C": 3, "B": 2, "A": 1}

	udm.Traverse(func(k string, b interface{}) bool {
		a, found := dm[k]
		t.True(found)

		t.Equal(a, b)

		return true
	})
}

func (t *testYAMLOrderedMap) TestEmptyValue() {
	b := []byte(`Z: z
A:
W: w
`)
	t.T().Logf("yaml:\n%s", string(b))

	var m *YAMLOrderedMap
	t.NoError(yaml.Unmarshal(b, &m))

	ub, err := yaml.Marshal(m)
	t.NoError(err)

	t.T().Logf("marshaled:\n%s", string(ub))

	var um *YAMLOrderedMap
	t.NoError(yaml.Unmarshal(ub, &um))

	i, found := um.Get("A")
	t.True(found)
	t.Nil(i)
}

func TestYAMLOrderedMap(t *testing.T) {
	suite.Run(t, new(testYAMLOrderedMap))
}
