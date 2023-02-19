package util

import (
	"sort"
	"testing"

	"github.com/pkg/errors"
	"github.com/stretchr/testify/suite"
)

type testLocked struct {
	suite.Suite
}

func (t *testLocked) TestNew() {
	t.Run("empty", func() {
		l := EmptyLocked("")
		i, isempty := l.Value()
		t.True(isempty)
		t.Empty(i)

		l.EmptyValue()
		i, isempty = l.Value()
		t.True(isempty)
		t.Empty(i)
	})

	t.Run("empty with value", func() {
		l := EmptyLocked("showme")
		i, isempty := l.Value()
		t.True(isempty)
		t.Empty(i)

		l.EmptyValue()
		i, isempty = l.Value()
		t.True(isempty)
		t.Empty(i)
	})

	t.Run("new withy empty", func() {
		l := NewLocked("")
		i, isempty := l.Value()
		t.False(isempty)
		t.Equal("", i)

		l.EmptyValue()
		i, isempty = l.Value()
		t.True(isempty)
		t.Empty(i)
	})

	t.Run("new", func() {
		l := NewLocked(33)
		i, isempty := l.Value()
		t.False(isempty)
		t.Equal(33, i)

		l.EmptyValue()
		i, isempty = l.Value()
		t.True(isempty)
		t.Empty(i)
	})
}

func (t *testLocked) TestSetValue() {
	t.Run("from empty", func() {
		l := EmptyLocked("")
		i, isempty := l.Value()
		t.True(isempty)
		t.Empty(i)

		l.SetValue("showme")
		i, isempty = l.Value()
		t.False(isempty)
		t.Equal("showme", i)
	})
}

func (t *testLocked) TestGet() {
	t.Run("from empty", func() {
		l := EmptyLocked("")
		i, isempty := l.Value()
		t.True(isempty)
		t.Empty(i)

		t.NoError(l.Get(func(i string, isempty bool) error {
			t.True(isempty)
			t.Empty(i)

			return nil
		}))
	})

	t.Run("not empty", func() {
		l := NewLocked(UUID().String())
		v, isempty := l.Value()
		t.False(isempty)
		t.NotEmpty(v)

		t.NoError(l.Get(func(i string, isempty bool) error {
			t.False(isempty)
			t.Equal(v, i)

			return nil
		}))
	})

	t.Run("error", func() {
		l := NewLocked(UUID().String())
		v, isempty := l.Value()
		t.False(isempty)
		t.NotEmpty(v)

		err := l.Get(func(i string, isempty bool) error {
			t.False(isempty)
			t.Equal(v, i)

			return errors.Errorf("findme")
		})
		t.Error(err)
		t.ErrorContains(err, "findme")
	})
}

func (t *testLocked) TestGetOrCreate() {
	t.Run("from empty", func() {
		l := EmptyLocked("")
		i, isempty := l.Value()
		t.True(isempty)
		t.Empty(i)

		c, err := l.GetOrCreate(func() (string, error) {
			return "showme", nil
		})
		t.NoError(err)
		t.Equal("showme", c)

		i, isempty = l.Value()
		t.False(isempty)
		t.Equal(c, i)
	})

	t.Run("from new", func() {
		l := NewLocked("findme")
		i, isempty := l.Value()
		t.False(isempty)
		t.Equal("findme", i)

		c, err := l.GetOrCreate(func() (string, error) {
			return "showme", nil
		})
		t.NoError(err)
		t.Equal("findme", c)

		i, isempty = l.Value()
		t.False(isempty)
		t.Equal(c, i)
	})

	t.Run("error", func() {
		l := EmptyLocked("")
		i, isempty := l.Value()
		t.True(isempty)
		t.Empty(i)

		c, err := l.GetOrCreate(func() (string, error) {
			return "showme", errors.Errorf("eatme")
		})
		t.Error(err)
		t.Equal("", c)
		t.ErrorContains(err, "eatme")

		i, isempty = l.Value()
		t.True(isempty)
		t.Empty(i)
	})
}

func (t *testLocked) TestSet() {
	t.Run("from empty", func() {
		l := EmptyLocked("")
		i, isempty := l.Value()
		t.True(isempty)
		t.Empty(i)

		c, err := l.Set(func(i string, isempty bool) (string, error) {
			t.True(isempty)
			t.Empty(i)

			return "showme", nil
		})
		t.NoError(err)
		t.Equal("showme", c)

		i, isempty = l.Value()
		t.False(isempty)
		t.Equal(c, i)
	})

	t.Run("from new", func() {
		l := NewLocked("findme")
		i, isempty := l.Value()
		t.False(isempty)
		t.Equal("findme", i)

		c, err := l.Set(func(i string, isempty bool) (string, error) {
			t.False(isempty)
			t.Equal("findme", i)

			return "showme", nil
		})
		t.NoError(err)
		t.Equal("showme", c)

		i, isempty = l.Value()
		t.False(isempty)
		t.Equal(c, i)
	})

	t.Run("error", func() {
		l := EmptyLocked("findme")

		c, err := l.Set(func(i string, isempty bool) (string, error) {
			t.True(isempty)
			t.Empty(i)

			return "showme", errors.Errorf("eatme")
		})
		t.Error(err)
		t.Equal("", c)
		t.ErrorContains(err, "eatme")

		i, isempty := l.Value()
		t.True(isempty)
		t.Empty(i)
	})

	t.Run("ignore error", func() {
		l := EmptyLocked("findme")

		c, err := l.Set(func(i string, isempty bool) (string, error) {
			t.True(isempty)
			t.Empty(i)

			return "showme", ErrLockedSetIgnore.Errorf("eatme")
		})
		t.NoError(err)
		t.Equal("", c)

		i, isempty := l.Value()
		t.True(isempty)
		t.Empty(i)
	})

	t.Run("ignore error from new", func() {
		l := NewLocked("findme")

		c, err := l.Set(func(i string, isempty bool) (string, error) {
			t.False(isempty)
			t.Equal("findme", i)

			return "showme", ErrLockedSetIgnore.Errorf("eatme")
		})
		t.NoError(err)
		t.Equal("findme", c)

		i, isempty := l.Value()
		t.False(isempty)
		t.Equal("findme", i)
	})
}

func (t *testLocked) TestEmpty() {
	t.Run("empty", func() {
		l := NewLocked("findme")

		t.NoError(l.Empty(func(i string, isempty bool) error {
			t.False(isempty)
			t.Equal("findme", i)

			return nil
		}))

		i, isempty := l.Value()
		t.True(isempty)
		t.Empty(i)
	})

	t.Run("ignore", func() {
		l := NewLocked("findme")

		t.NoError(l.Empty(func(i string, isempty bool) error {
			t.False(isempty)
			t.Equal("findme", i)

			return ErrLockedSetIgnore.Call()
		}))

		i, isempty := l.Value()
		t.False(isempty)
		t.Equal("findme", i)
	})

	t.Run("error", func() {
		l := NewLocked("findme")

		err := l.Empty(func(i string, isempty bool) error {
			t.False(isempty)
			t.Equal("findme", i)

			return errors.Errorf("showme")
		})
		t.Error(err)
		t.ErrorContains(err, "showme")

		i, isempty := l.Value()
		t.False(isempty)
		t.Equal("findme", i)
	})
}

func TestLocked(t *testing.T) {
	suite.Run(t, new(testLocked))
}

type testSingleLockedMap struct {
	suite.Suite
}

func (t *testSingleLockedMap) TestNew() {
	l := NewSingleLockedMap("", 0)
	t.Equal(0, l.Len())

	_ = (interface{})(l).(LockedMap[string, int])
}

func (t *testSingleLockedMap) TestSetValue() {
	l := NewSingleLockedMap("showme", 1)
	t.Equal(0, l.Len())

	l.SetValue("showme", 1)

	i, found := l.Value("showme")
	t.True(found)
	t.Equal(1, i)

	i, found = l.Value("findme")
	t.False(found)
	t.Zero(i)

	t.Equal(1, l.Len())

	l.SetValue("findme", 2)

	i, found = l.Value("findme")
	t.True(found)
	t.Equal(2, i)

	t.Equal(2, l.Len())
}

func (t *testSingleLockedMap) TestRemoveValue() {
	l := NewSingleLockedMap("showme", 1)

	l.SetValue("showme", 1)

	i, found := l.Value("showme")
	t.True(found)
	t.Equal(1, i)

	removed := l.RemoveValue("showme")
	t.True(removed)

	i, found = l.Value("showme")
	t.False(found)
	t.Zero(i)

	removed = l.RemoveValue("showme")
	t.False(removed)
}

func (t *testSingleLockedMap) TestGet() {
	l := NewSingleLockedMap("showme", 1)

	t.Run("not found", func() {
		t.NoError(l.Get("showme", func(v int, found bool) error {
			t.False(found)
			t.Zero(v)

			return nil
		}))
	})

	t.Run("found", func() {
		_ = l.SetValue("showme", 3)

		t.NoError(l.Get("showme", func(v int, found bool) error {
			t.True(found)
			t.Equal(3, v)

			return nil
		}))
	})

	t.Run("error", func() {
		err := l.Get("showme", func(v int, found bool) error {
			return errors.Errorf("findme")
		})
		t.Error(err)
		t.ErrorContains(err, "findme")
	})
}

func (t *testSingleLockedMap) TestGetOrCreate() {
	l := NewSingleLockedMap("showme", 1)

	t.Run("exists", func() {
		l.SetValue("showme", 1)

		i, found, err := l.GetOrCreate("showme", func() (int, error) {
			return 2, nil
		})

		t.NoError(err)
		t.True(found)
		t.Equal(1, i)
	})

	t.Run("create", func() {
		i, found, err := l.GetOrCreate("findme", func() (int, error) {
			return 2, nil
		})

		t.NoError(err)
		t.False(found)
		t.Equal(2, i)
	})

	t.Run("ignore", func() {
		i, found, err := l.GetOrCreate("eatme", func() (int, error) {
			return 2, ErrLockedSetIgnore.Errorf("eatme")
		})

		t.NoError(err)
		t.False(found)
		t.Zero(i)
	})

	t.Run("error", func() {
		i, found, err := l.GetOrCreate("eatme", func() (int, error) {
			return 2, errors.Errorf("eatme")
		})

		t.Error(err)
		t.ErrorContains(err, "eatme")
		t.False(found)
		t.Zero(i)
	})
}

func (t *testSingleLockedMap) TestSet() {
	l := NewSingleLockedMap("showme", 1)

	t.Run("new value", func() {
		i, err := l.Set("showme", func(i int, found bool) (int, error) {
			return 1, nil
		})
		t.NoError(err)
		t.Equal(1, i)

		i, found := l.Value("showme")
		t.True(found)
		t.Equal(1, i)
	})

	t.Run("existing value", func() {
		i, err := l.Set("showme", func(i int, found bool) (int, error) {
			t.True(found)
			t.Equal(1, i)

			return 2, nil
		})
		t.NoError(err)
		t.Equal(2, i)

		i, found := l.Value("showme")
		t.True(found)
		t.Equal(2, i)
	})

	t.Run("ignore", func() {
		i, err := l.Set("showme", func(i int, found bool) (int, error) {
			t.True(found)
			t.Equal(2, i)

			return 3, ErrLockedSetIgnore.Call()
		})
		t.NoError(err)
		t.Equal(2, i)

		i, found := l.Value("showme")
		t.True(found)
		t.Equal(2, i)
	})

	t.Run("error", func() {
		i, err := l.Set("showme", func(i int, found bool) (int, error) {
			t.True(found)
			t.Equal(2, i)

			return 3, errors.Errorf("eatme")
		})
		t.Error(err)
		t.ErrorContains(err, "eatme")
		t.Zero(i)

		i, found := l.Value("showme")
		t.True(found)
		t.Equal(2, i)
	})
}

func (t *testSingleLockedMap) TestRemove() {
	l := NewSingleLockedMap("showme", 1)

	t.Run("unknown", func() {
		removed, err := l.Remove("showme", func(i int, found bool) error {
			t.False(found)

			return nil
		})
		t.NoError(err)
		t.False(removed)
	})

	t.Run("existing value", func() {
		l.SetValue("showme", 1)

		removed, err := l.Remove("showme", func(i int, found bool) error {
			t.True(found)
			t.Equal(1, i)

			return nil
		})
		t.NoError(err)
		t.True(removed)
	})

	t.Run("ignore", func() {
		l.SetValue("showme", 1)

		removed, err := l.Remove("showme", func(i int, found bool) error {
			t.True(found)
			t.Equal(1, i)

			return ErrLockedSetIgnore.Call()
		})
		t.NoError(err)
		t.False(removed)

		i, found := l.Value("showme")
		t.True(found)
		t.Equal(1, i)
	})
}

func (t *testSingleLockedMap) TestTraverse() {
	l := NewSingleLockedMap("showme", 1)

	kv := map[string]int{}

	for i := range make([]int, 33) {
		k := UUID().String()

		l.SetValue(k, i)
		kv[k] = i
	}

	t.Run("all", func() {
		rkv := map[string]int{}

		l.Traverse(func(k string, v int) bool {
			rkv[k] = v

			return true
		})

		t.Equal(len(kv), len(rkv))

		for k := range kv {
			v := kv[k]
			rv, found := rkv[k]
			t.True(found)
			t.Equal(v, rv)
		}
	})

	t.Run("stop", func() {
		rkv := map[string]int{}

		l.Traverse(func(k string, v int) bool {
			rkv[k] = v

			return len(rkv) < 9
		})

		t.Equal(9, len(rkv))
	})
}

func (t *testSingleLockedMap) TestEmpty() {
	l := NewSingleLockedMap("showme", 1)

	t.Run("empty", func() {
		l.Empty()
		l.Empty()

		t.Equal(0, l.Len())
	})

	t.Run("non-empty", func() {
		l.SetValue("showme", 1)
		t.Equal(1, l.Len())

		l.Empty()

		t.Equal(0, l.Len())
	})
}

func (t *testSingleLockedMap) TestClose() {
	l := NewSingleLockedMap("showme", 1)

	l.Close()

	_, found := l.Value("showme")
	t.False(found)

	found = l.SetValue("showme", 1)
	t.False(found)

	i, found, err := l.GetOrCreate("showme", func() (int, error) {
		return 2, nil
	})
	t.Error(err)
	t.False(found)
	t.Zero(i)
	t.True(errors.Is(err, ErrLockedMapClosed))

	i, err = l.Set("showme", func(i int, found bool) (int, error) {
		t.False(found)
		t.Zero(i)

		return 2, nil
	})
	t.Error(err)
	t.Zero(i)
	t.True(errors.Is(err, ErrLockedMapClosed))
}

func TestSingleLockedMap(t *testing.T) {
	suite.Run(t, new(testSingleLockedMap))
}

type testShardedMap struct {
	suite.Suite
}

func (t *testShardedMap) TestNew() {
	m, err := NewShardedMap("", "", 3)
	t.NoError(err)

	_ = (interface{})(m).(LockedMap[string, string])

	t.Run("over 1", func() {
		m, err := NewShardedMap("", "", 3)
		t.NoError(err)
		t.Equal(0, m.Len())
	})

	t.Run("0 size", func() {
		_, err := NewShardedMap("", "", 0)
		t.Error(err)
		t.ErrorContains(err, "empty size")
	})

	t.Run("1 size", func() {
		_, err := NewShardedMap("", "", 1)
		t.Error(err)
		t.ErrorContains(err, "1 size")
	})
}

func (t *testShardedMap) TestSetValue() {
	t.Run("new", func() {
		m, err := NewShardedMap("", "", 3)
		t.NoError(err)

		t.False(m.SetValue("showme", "showme"))
		i, found := m.Value("showme")
		t.True(found)
		t.Equal("showme", i)
		t.Equal(1, m.Len())

		t.True(m.Exists("showme"))
		t.False(m.Exists(UUID().String()))

		t.False(m.SetValue("findme", "findme"))
		i, found = m.Value("findme")
		t.True(found)
		t.Equal("findme", i)
		t.Equal(2, m.Len())
	})

	t.Run("exists", func() {
		m, err := NewShardedMap("", "", 3)
		t.NoError(err)

		t.False(m.SetValue("showme", "showme"))
		t.True(m.SetValue("showme", "findme"))

		i, found := m.Value("showme")
		t.True(found)
		t.Equal("findme", i)
		t.Equal(1, m.Len())
	})
}

func (t *testShardedMap) TestRemoveValue() {
	t.Run("unknown", func() {
		m, err := NewShardedMap("", "", 3)
		t.NoError(err)

		t.Equal(0, m.Len())

		t.False(m.RemoveValue(UUID().String()))
	})

	t.Run("known", func() {
		m, err := NewShardedMap("", "", 3)
		t.NoError(err)

		t.False(m.RemoveValue(UUID().String()))

		t.False(m.SetValue("showme", "showme"))
		t.Equal(1, m.Len())

		t.True(m.RemoveValue("showme"))
		t.Equal(0, m.Len())
	})
}

func (t *testShardedMap) TestGetOrCreate() {
	t.Run("create", func() {
		m, err := NewShardedMap("", "", 3)
		t.NoError(err)

		i, found, err := m.GetOrCreate("showme", func() (string, error) {
			return "showme", nil
		})
		t.NoError(err)
		t.False(found)
		t.Equal("showme", i)
		t.Equal(1, m.Len())
	})

	t.Run("known", func() {
		m, err := NewShardedMap("", "", 3)
		t.NoError(err)

		t.False(m.SetValue("showme", "showme"))
		t.Equal(1, m.Len())

		i, found, err := m.GetOrCreate("showme", func() (string, error) {
			return "findme", nil
		})
		t.NoError(err)
		t.True(found)
		t.Equal("showme", i)
		t.Equal(1, m.Len())
	})

	t.Run("unknown, ignore", func() {
		m, err := NewShardedMap("", "", 3)
		t.NoError(err)

		i, found, err := m.GetOrCreate("showme", func() (string, error) {
			return "showme", ErrLockedSetIgnore.Errorf("findme")
		})
		t.NoError(err)
		t.False(found)
		t.Zero(i)
		t.Equal(0, m.Len())
	})

	t.Run("unknown, error", func() {
		m, err := NewShardedMap("", "", 3)
		t.NoError(err)

		i, found, err := m.GetOrCreate("showme", func() (string, error) {
			return "showme", errors.Errorf("findme")
		})
		t.Error(err)
		t.False(found)
		t.Zero(i)
		t.Equal(0, m.Len())
		t.ErrorContains(err, "findme")
	})

	t.Run("known, error", func() {
		m, err := NewShardedMap("", "", 3)
		t.NoError(err)

		t.False(m.SetValue("showme", "showme"))
		t.Equal(1, m.Len())

		i, found, err := m.GetOrCreate("showme", func() (string, error) {
			return "showme", errors.Errorf("findme")
		})
		t.NoError(err)
		t.True(found)
		t.Equal("showme", i)
		t.Equal(1, m.Len())
	})
}

func (t *testShardedMap) TestSet() {
	t.Run("create", func() {
		m, err := NewShardedMap("", "", 3)
		t.NoError(err)

		i, err := m.Set("showme", func(i string, found bool) (string, error) {
			t.False(found)
			t.Zero(i)

			return "showme", nil
		})
		t.NoError(err)
		t.Equal("showme", i)
		t.Equal(1, m.Len())
	})

	t.Run("override", func() {
		m, err := NewShardedMap("", "", 3)
		t.NoError(err)

		m.SetValue("showme", "showme")

		i, err := m.Set("showme", func(i string, found bool) (string, error) {
			t.True(found)
			t.Equal("showme", i)

			return "findme", nil
		})
		t.NoError(err)
		t.Equal("findme", i)
		t.Equal(1, m.Len())
	})

	t.Run("ignore", func() {
		m, err := NewShardedMap("", "", 3)
		t.NoError(err)

		m.SetValue("showme", "showme")

		i, err := m.Set("showme", func(i string, found bool) (string, error) {
			t.True(found)
			t.Equal("showme", i)

			return "findme", ErrLockedSetIgnore.Call()
		})
		t.NoError(err)
		t.Equal("showme", i)
		t.Equal(1, m.Len())
	})

	t.Run("error", func() {
		m, err := NewShardedMap("", "", 3)
		t.NoError(err)

		m.SetValue("showme", "showme")

		i, err := m.Set("showme", func(i string, found bool) (string, error) {
			t.True(found)
			t.Equal("showme", i)

			return "findme", errors.Errorf("eatme")
		})
		t.Error(err)
		t.Zero(i)
		t.Equal(1, m.Len())
		t.ErrorContains(err, "eatme")
	})
}

func (t *testShardedMap) TestRemove() {
	m, err := NewShardedMap("showme", 1, 3)
	t.NoError(err)

	t.Run("unknown", func() {
		removed, err := m.Remove("showme", func(i int, found bool) error {
			t.False(found)

			return nil
		})
		t.NoError(err)
		t.False(removed)
	})

	t.Run("existing value", func() {
		m.SetValue("showme", 1)

		removed, err := m.Remove("showme", func(i int, found bool) error {
			t.True(found)
			t.Equal(1, i)

			return nil
		})
		t.NoError(err)
		t.True(removed)
	})

	t.Run("ignore", func() {
		m.SetValue("showme", 1)

		removed, err := m.Remove("showme", func(i int, found bool) error {
			t.True(found)
			t.Equal(1, i)

			return ErrLockedSetIgnore.Call()
		})
		t.NoError(err)
		t.False(removed)

		i, found := m.Value("showme")
		t.True(found)
		t.Equal(1, i)
	})
}

func (t *testShardedMap) TestTraverse() {
	m, err := NewShardedMap(0, 0, 3)
	t.NoError(err)

	sets := make([]int, 9)
	for i := range make([]int, 9) {
		t.False(m.SetValue(i, i))
		sets[i] = i
	}

	t.Run("all", func() {
		var collected []int

		m.Traverse(func(k, v int) bool {
			collected = append(collected, k)

			return true
		})

		t.Equal(m.Len(), len(collected))
		sort.Ints(collected)

		t.Equal(sets, collected)
	})

	t.Run("stop", func() {
		var collected []int

		m.Traverse(func(k, v int) bool {
			collected = append(collected, k)

			return len(collected) < 3
		})

		t.Equal(3, len(collected))
	})
}

func (t *testShardedMap) TestClose() {
	t.Run("close", func() {
		m, err := NewShardedMap(0, 0, 3)
		t.NoError(err)

		sets := make([]int, 9)
		for i := range make([]int, 9) {
			t.False(m.SetValue(i, i))
			sets[i] = i
		}

		for i := range m.sharded {
			t.NotNil(m.sharded[i])
		}

		t.Equal(9, m.Len())
		m.Close()
		t.Equal(0, m.Len())

		for i := range m.sharded {
			t.Nil(m.sharded[i])
		}

		i, found := m.Value(1)
		t.False(found)
		t.Zero(i)

		t.False(m.SetValue(1, 2))

		i, found, err = m.GetOrCreate(1, func() (int, error) {
			return 3, nil
		})
		t.Error(err)
		t.False(found)
		t.Zero(i)
		t.True(errors.Is(err, ErrLockedMapClosed))

		i, err = m.Set(1, func(int, bool) (int, error) {
			return 3, nil
		})
		t.Error(err)
		t.Zero(i)
		t.True(errors.Is(err, ErrLockedMapClosed))
	})

	t.Run("close empty", func() {
		m, err := NewShardedMap(0, 0, 3)
		t.NoError(err)

		m.Close()
		t.Equal(0, m.Len())
	})
}

func (t *testShardedMap) TestEmpty() {
	t.Run("empty", func() {
		m, err := NewShardedMap(0, 0, 3)
		t.NoError(err)

		sets := make([]int, 9)
		for i := range make([]int, 9) {
			t.False(m.SetValue(i, i))
			sets[i] = i
		}

		for i := range m.sharded {
			t.NotNil(m.sharded[i])
		}

		t.Equal(9, m.Len())
		m.Empty()
		t.Equal(0, m.Len())

		for i := range m.sharded {
			t.NotNil(m.sharded[i])
		}

		i, found := m.Value(1)
		t.False(found)
		t.Zero(i)

		t.False(m.SetValue(1, 2))

		i, found, err = m.GetOrCreate(2, func() (int, error) {
			return 3, nil
		})
		t.NoError(err)
		t.False(found)
		t.Equal(3, i)

		i, err = m.Set(1, func(int, bool) (int, error) {
			return 4, nil
		})
		t.NoError(err)
		t.Equal(4, i)
	})

	t.Run("empty", func() {
		m, err := NewShardedMap(0, 0, 3)
		t.NoError(err)

		m.Empty()
		t.Equal(0, m.Len())
	})
}

func TestShardedMap(t *testing.T) {
	suite.Run(t, new(testShardedMap))
}

func TestLockedMap(tt *testing.T) {
	t := new(suite.Suite)

	t.SetT(tt)

	t.Run("0 size", func() {
		_, err := NewLockedMap("", "", 0)
		t.Error(err)
		t.ErrorContains(err, "empty size")
	})

	t.Run("1 size == SingleLockedMap", func() {
		l, err := NewLockedMap("", "", 1)
		t.NoError(err)

		_ = l.(*SingleLockedMap[string, string])
	})

	t.Run("over 1 size == ShardedMap", func() {
		l, err := NewLockedMap("", "", 3)
		t.NoError(err)

		_ = l.(*ShardedMap[string, string])
	})
}
