package util

import (
	"sort"
	"strings"
	"testing"

	"github.com/pkg/errors"
	"github.com/stretchr/testify/suite"
)

type testShardedMap struct {
	suite.Suite
}

func (t *testShardedMap) TestExists() {
	m := NewShardedMap(10)

	keys := make([]string, 333)
	for i := range keys {
		k := ULID().String()
		keys[i] = k

		t.False(m.SetValue(k, k))
	}

	t.Equal(len(keys), m.Len())

	for i := range keys {
		t.True(m.Exists(keys[i]))
	}

	t.False(m.Exists(UUID().String()))
}

func (t *testShardedMap) TestValue() {
	m := NewShardedMap(10)

	keys := make([]string, 333)
	for i := range keys {
		k := ULID().String()
		keys[i] = k

		t.False(m.SetValue(k, k))
	}

	for i := range keys {
		k := keys[i]

		v, found := m.Value(k)
		t.True(found)
		t.Equal(k, v)
	}

	v, found := m.Value(UUID().String())
	t.False(found)
	t.Nil(v)
}

func (t *testShardedMap) TestGet() {
	t.Run("new value", func() {
		m := NewShardedMap(10)

		k := UUID().String()
		v := UUID().String()

		rv, found, err := m.Get(k, func() (interface{}, error) {
			return v, nil
		})
		t.NoError(err)
		t.False(found)
		t.Equal(v, rv)
	})

	t.Run("found", func() {
		m := NewShardedMap(10)

		k := UUID().String()
		v := UUID().String()

		t.False(m.SetValue(k, v))

		rv, found, err := m.Get(k, func() (interface{}, error) {
			return UUID().String(), nil
		})
		t.NoError(err)
		t.True(found)
		t.Equal(v, rv)
	})

	t.Run("error", func() {
		m := NewShardedMap(10)

		k := UUID().String()

		rv, found, err := m.Get(k, func() (interface{}, error) {
			return nil, errors.Errorf("hehehe")
		})
		t.Error(err)
		t.False(found)
		t.Nil(rv)

		t.Equal(0, m.Len())

		t.ErrorContains(err, "hehehe")
	})
}

func (t *testShardedMap) TestSet() {
	t.Run("new value", func() {
		m := NewShardedMap(10)

		k := UUID().String()
		v := UUID().String()

		rv, err := m.Set(k, func(found bool, i interface{}) (interface{}, error) {
			t.False(found)

			return v, nil
		})
		t.NoError(err)
		t.Equal(v, rv)
		t.Equal(1, m.Len())
	})

	t.Run("update value", func() {
		m := NewShardedMap(10)

		k := UUID().String()
		v := UUID().String()
		newv := UUID().String()

		t.False(m.SetValue(k, v))

		rv, err := m.Set(k, func(found bool, i interface{}) (interface{}, error) {
			t.True(found)
			t.Equal(v, i)

			return newv, nil
		})
		t.NoError(err)
		t.Equal(newv, rv)
		t.Equal(1, m.Len())
	})
}

func (t *testShardedMap) TestRemoveValue() {
	t.Run("unknown", func() {
		m := NewShardedMap(10)

		k := UUID().String()

		m.RemoveValue(k)
		t.False(m.Exists(k))
		t.Equal(0, m.Len())
	})

	t.Run("known", func() {
		m := NewShardedMap(10)

		k := UUID().String()
		v := UUID().String()

		t.False(m.SetValue(k, v))
		t.True(m.Exists(k))

		m.RemoveValue(k)
		t.False(m.Exists(k))
		t.Equal(0, m.Len())
	})
}

func (t *testShardedMap) TestRemove() {
	t.Run("unknown", func() {
		m := NewShardedMap(10)

		k := UUID().String()

		t.NoError(m.Remove(k, func(i interface{}) error {
			t.True(IsNilLockedValue(i))

			return nil
		}))

		t.Equal(0, m.Len())
		t.False(m.Exists(k))
	})

	t.Run("known", func() {
		m := NewShardedMap(10)

		k := UUID().String()
		v := UUID().String()

		t.False(m.SetValue(k, v))
		t.True(m.Exists(k))
		t.Equal(1, m.Len())

		t.NoError(m.Remove(k, func(i interface{}) error {
			t.False(IsNilLockedValue(i))
			t.Equal(v, i)

			return nil
		}))

		t.False(m.Exists(k))
	})
}

func (t *testShardedMap) TestTraverse() {
	t.Run("empty", func() {
		m := NewShardedMap(10)

		t.Equal(0, m.Len())

		var touched bool
		m.Traverse(func(k, v interface{}) bool {
			touched = true

			return true
		})
		t.False(touched)
	})

	t.Run("non-empty", func() {
		m := NewShardedMap(10)

		keys := make([]string, 333)
		for i := range keys {
			k := ULID().String()
			keys[i] = k

			t.False(m.SetValue(k, k))
		}

		t.Equal(len(keys), m.Len())

		var rkeys []string

		m.Traverse(func(k, v interface{}) bool {
			rkeys = append(rkeys, k.(string))

			return true
		})

		sort.Slice(keys, func(i, j int) bool {
			return strings.Compare(keys[i], keys[j]) < 0
		})

		sort.Slice(rkeys, func(i, j int) bool {
			return strings.Compare(rkeys[i], rkeys[j]) < 0
		})

		t.Equal(keys, rkeys)
	})
}

func (t *testShardedMap) TestClose() {
	t.Run("empty", func() {
		m := NewShardedMap(10)

		t.Equal(0, m.Len())

		m.Close()

		t.Equal(0, m.Len())
	})

	t.Run("non-empty", func() {
		m := NewShardedMap(10)

		keys := make([]string, 333)
		for i := range keys {
			k := ULID().String()
			keys[i] = k

			t.False(m.SetValue(k, k))
		}

		t.Equal(len(keys), m.Len())

		m.Close()

		t.Equal(0, m.Len())
	})
}

func TestShardedMap(t *testing.T) {
	suite.Run(t, new(testShardedMap))
}
