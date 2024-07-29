package util

import (
	"math"
	"sort"
	"sync"
	"testing"
	"time"

	"github.com/pkg/errors"
	"github.com/spikeekips/mitum/util/logging"
	"github.com/stretchr/testify/suite"
)

type testLocked struct {
	suite.Suite
}

func (t *testLocked) TestNew() {
	t.Run("empty", func() {
		l := EmptyLocked[string]()
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
		l := EmptyLocked[string]()
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
		l := EmptyLocked[string]()
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
	t.Run("f error", func() {
		l := NewLocked[string]("findme")

		err := l.GetOrCreate(
			func(i string, created bool) error {
				t.False(created)

				return errors.Errorf("killme")
			},
			func() (string, error) {
				return "showme", nil
			},
		)
		t.Error(err)
		t.ErrorContains(err, "killme")

		i, isempty := l.Value()
		t.False(isempty)
		t.Equal("findme", i)
	})

	t.Run("from empty", func() {
		l := EmptyLocked[string]()
		i, isempty := l.Value()
		t.True(isempty)
		t.Empty(i)

		var c string

		t.NoError(l.GetOrCreate(
			func(i string, created bool) error {
				t.True(created)
				t.Equal("showme", i)

				c = i

				return nil
			},
			func() (string, error) {
				return "showme", nil
			},
		))

		i, isempty = l.Value()
		t.False(isempty)
		t.Equal(c, i)
	})

	t.Run("from new", func() {
		l := NewLocked("findme")
		i, isempty := l.Value()
		t.False(isempty)
		t.Equal("findme", i)

		var c string

		t.NoError(l.GetOrCreate(
			func(i string, created bool) error {
				t.False(created)
				t.Equal("findme", i)

				c = i

				return nil
			},
			func() (string, error) {
				return "showme", nil
			},
		))

		i, isempty = l.Value()
		t.False(isempty)
		t.Equal(c, i)
	})

	t.Run("error", func() {
		l := EmptyLocked[string]()
		i, isempty := l.Value()
		t.True(isempty)
		t.Empty(i)

		err := l.GetOrCreate(
			func(i string, created bool) error {
				t.False(created)

				return nil
			},
			func() (string, error) {
				return "showme", errors.Errorf("eatme")
			},
		)
		t.Error(err)
		t.ErrorContains(err, "eatme")

		i, isempty = l.Value()
		t.True(isempty)
		t.Empty(i)
	})
}

func (t *testLocked) TestSet() {
	t.Run("from empty", func() {
		l := EmptyLocked[string]()
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
		l := EmptyLocked[string]()

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
		l := EmptyLocked[string]()

		c, err := l.Set(func(i string, isempty bool) (string, error) {
			t.True(isempty)
			t.Empty(i)

			return "showme", ErrLockedSetIgnore
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

			return "showme", ErrLockedSetIgnore
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

			return ErrLockedSetIgnore
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
	l := NewSingleLockedMap[string, int]()
	t.Equal(0, l.Len())

	_ = (interface{})(l).(LockedMap[string, int])
}

func (t *testSingleLockedMap) TestSetValue() {
	l := NewSingleLockedMap[string, int]()
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
	l := NewSingleLockedMap[string, int]()

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
	l := NewSingleLockedMap[string, int]()

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
	l := NewSingleLockedMap[string, int]()

	t.Run("exists", func() {
		l.SetValue("showme", 1)

		t.NoError(l.GetOrCreate("showme",
			func(v int, created bool) error {
				t.False(created)
				t.Equal(1, v)

				return nil
			},
			func() (int, error) {
				return 2, nil
			},
		))
	})

	t.Run("create", func() {
		t.NoError(l.GetOrCreate("findme",
			func(i int, created bool) error {
				t.True(created)
				t.Equal(2, i)

				return nil
			},
			func() (int, error) {
				return 2, nil
			},
		))
	})

	t.Run("ignore", func() {
		t.NoError(l.GetOrCreate("eatme",
			func(i int, created bool) error {
				t.False(created)
				t.Zero(i)

				return nil
			},
			func() (int, error) {
				return 2, ErrLockedSetIgnore
			},
		))
	})

	t.Run("error", func() {
		err := l.GetOrCreate("eatme",
			func(int, bool) error {
				return nil
			},
			func() (int, error) {
				return 2, errors.Errorf("eatme")
			},
		)

		t.Error(err)
		t.ErrorContains(err, "eatme")
	})
}

func (t *testSingleLockedMap) TestSet() {
	l := NewSingleLockedMap[string, int]()

	t.Run("new value", func() {
		i, created, err := l.Set("showme", func(i int, found bool) (int, error) {
			return 1, nil
		})
		t.NoError(err)
		t.True(created)
		t.Equal(1, i)

		i, found := l.Value("showme")
		t.True(found)
		t.Equal(1, i)
	})

	t.Run("existing value", func() {
		i, created, err := l.Set("showme", func(i int, found bool) (int, error) {
			t.True(found)
			t.Equal(1, i)

			return 2, nil
		})
		t.NoError(err)
		t.False(created)
		t.Equal(2, i)

		i, found := l.Value("showme")
		t.True(found)
		t.Equal(2, i)
	})

	t.Run("ignore", func() {
		i, created, err := l.Set("showme", func(i int, found bool) (int, error) {
			t.True(found)
			t.Equal(2, i)

			return 3, ErrLockedSetIgnore
		})
		t.NoError(err)
		t.False(created)
		t.Equal(2, i)

		i, found := l.Value("showme")
		t.True(found)
		t.Equal(2, i)
	})

	t.Run("error", func() {
		i, created, err := l.Set("showme", func(i int, found bool) (int, error) {
			t.True(found)
			t.Equal(2, i)

			return 3, errors.Errorf("eatme")
		})
		t.Error(err)
		t.False(created)
		t.ErrorContains(err, "eatme")
		t.Zero(i)

		i, found := l.Value("showme")
		t.True(found)
		t.Equal(2, i)
	})
}

func (t *testSingleLockedMap) TestRemove() {
	l := NewSingleLockedMap[string, int]()

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

			return ErrLockedSetIgnore
		})
		t.NoError(err)
		t.False(removed)

		i, found := l.Value("showme")
		t.True(found)
		t.Equal(1, i)
	})
}

func (t *testSingleLockedMap) TestSetOrRemove() {
	l := NewSingleLockedMap[string, int]()

	t.Run("set; unknown", func() {
		_, created, removed, err := l.SetOrRemove("showme", func(i int, found bool) (int, bool, error) {
			t.False(found)

			return 3, false, nil
		})
		t.NoError(err)
		t.True(created)
		t.False(removed)

		i, found := l.Value("showme")
		t.True(found)
		t.Equal(3, i)
	})

	t.Run("set; found, not remove", func() {
		_, created, removed, err := l.SetOrRemove("showme", func(i int, found bool) (int, bool, error) {
			t.True(found)

			return i + 3, false, nil
		})
		t.NoError(err)
		t.False(created)
		t.False(removed)

		i, found := l.Value("showme")
		t.True(found)
		t.Equal(6, i)
	})

	t.Run("remove; unknown", func() {
		_, created, removed, err := l.SetOrRemove("findme", func(i int, found bool) (int, bool, error) {
			t.False(found)

			return 0, true, nil
		})
		t.NoError(err)
		t.False(created)
		t.False(removed)

		i, found := l.Value("showme")
		t.True(found)
		t.Equal(6, i)
	})

	t.Run("remove; found", func() {
		_, created, removed, err := l.SetOrRemove("showme", func(i int, found bool) (int, bool, error) {
			t.True(found)

			return 0, true, nil
		})
		t.NoError(err)
		t.False(created)
		t.True(removed)

		_, found := l.Value("showme")
		t.False(found)
	})
}

func (t *testSingleLockedMap) TestTraverse() {
	l := NewSingleLockedMap[string, int]()

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
	l := NewSingleLockedMap[string, int]()

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
	l := NewSingleLockedMap[string, int]()

	l.Close()

	_, found := l.Value("showme")
	t.False(found)

	added := l.SetValue("showme", 1)
	t.False(added)

	err := l.GetOrCreate("showme",
		func(int, bool) error {
			return nil
		},
		func() (int, error) {
			return 2, nil
		},
	)
	t.Error(err)
	t.ErrorIs(err, ErrLockedMapClosed)

	i, created, err := l.Set("showme", func(i int, found bool) (int, error) {
		t.False(found)
		t.Zero(i)

		return 2, nil
	})
	t.Error(err)
	t.False(created)
	t.Zero(i)
	t.ErrorIs(err, ErrLockedMapClosed)
}

func TestSingleLockedMap(t *testing.T) {
	suite.Run(t, new(testSingleLockedMap))
}

type testShardedMap struct {
	suite.Suite
}

func (t *testShardedMap) TestNew() {
	m, err := NewShardedMap[string, string](3, nil)
	t.NoError(err)

	_ = (interface{})(m).(LockedMap[string, string])

	t.Run("over 1", func() {
		m, err := NewShardedMap[string, string](3, nil)
		t.NoError(err)
		t.Equal(0, m.Len())
	})

	t.Run("0 size", func() {
		_, err := NewShardedMap[string, string](0, nil)
		t.Error(err)
		t.ErrorContains(err, "empty size")
	})

	t.Run("1 size", func() {
		_, err := NewShardedMap[string, string](1, nil)
		t.Error(err)
		t.ErrorContains(err, "1 size")
	})
}

func (t *testShardedMap) TestSetValue() {
	t.Run("new", func() {
		m, err := NewShardedMap[string, string](3, nil)
		t.NoError(err)

		t.True(m.SetValue("showme", "showme"))
		i, found := m.Value("showme")
		t.True(found)
		t.Equal("showme", i)
		t.Equal(1, m.Len())

		t.True(m.Exists("showme"))
		t.False(m.Exists(UUID().String()))

		t.True(m.SetValue("findme", "findme"))
		i, found = m.Value("findme")
		t.True(found)
		t.Equal("findme", i)
		t.Equal(2, m.Len())
	})

	t.Run("exists", func() {
		m, err := NewShardedMap[string, string](3, nil)
		t.NoError(err)

		t.True(m.SetValue("showme", "showme"))
		t.False(m.SetValue("showme", "findme"))

		i, found := m.Value("showme")
		t.True(found)
		t.Equal("findme", i)
		t.Equal(1, m.Len())
	})
}

func (t *testShardedMap) TestRemoveValue() {
	t.Run("unknown", func() {
		m, err := NewShardedMap[string, string](3, nil)
		t.NoError(err)

		t.Equal(0, m.Len())

		t.False(m.RemoveValue(UUID().String()))
	})

	t.Run("known", func() {
		m, err := NewShardedMap[string, string](3, nil)
		t.NoError(err)

		t.False(m.RemoveValue(UUID().String()))

		t.True(m.SetValue("showme", "showme"))
		t.Equal(1, m.Len())

		t.True(m.RemoveValue("showme"))
		t.Equal(0, m.Len())
	})
}

func (t *testShardedMap) TestGetOrCreate() {
	t.Run("create", func() {
		m, err := NewShardedMap[string, string](3, nil)
		t.NoError(err)

		t.NoError(m.GetOrCreate(
			"showme",
			func(i string, created bool) error {
				t.NoError(err)
				t.True(created)
				t.Equal("showme", i)

				return nil
			},
			func() (string, error) {
				return "showme", nil
			},
		))
		t.Equal(1, m.Len())
	})

	t.Run("known", func() {
		m, err := NewShardedMap[string, string](3, nil)
		t.NoError(err)

		t.True(m.SetValue("showme", "showme"))
		t.Equal(1, m.Len())

		t.NoError(m.GetOrCreate("showme",
			func(i string, created bool) error {
				t.False(created)
				t.Equal("showme", i)

				return nil
			},
			func() (string, error) {
				return "findme", nil
			},
		))
		t.NoError(err)
		t.Equal(1, m.Len())
	})

	t.Run("unknown, ignore", func() {
		m, err := NewShardedMap[string, string](3, nil)
		t.NoError(err)

		t.NoError(m.GetOrCreate("showme",
			func(string, bool) error {
				return nil
			},
			func() (string, error) {
				return "showme", ErrLockedSetIgnore
			},
		))

		t.False(m.Exists("showme"))

		i, found := m.Value("showme")
		t.False(found)
		t.Zero(i)
		t.Equal(0, m.Len())
	})

	t.Run("unknown, error", func() {
		m, err := NewShardedMap[string, string](3, nil)
		t.NoError(err)

		err = m.GetOrCreate("showme",
			func(string, bool) error {
				return nil
			},
			func() (string, error) {
				return "showme", errors.Errorf("findme")
			},
		)
		t.Error(err)
		t.ErrorContains(err, "findme")

		t.False(m.Exists("showme"))

		i, found := m.Value("showme")
		t.False(found)
		t.Zero(i)

		t.Equal(0, m.Len())
	})

	t.Run("known, error", func() {
		m, err := NewShardedMap[string, string](3, nil)
		t.NoError(err)

		t.True(m.SetValue("showme", "showme"))
		t.Equal(1, m.Len())

		t.NoError(m.GetOrCreate("showme",
			func(i string, created bool) error {
				t.False(created)
				t.Equal("showme", i)

				return nil
			},
			func() (string, error) {
				return "showme", errors.Errorf("findme")
			},
		))
		t.NoError(err)

		t.Equal(1, m.Len())
	})
}

func (t *testShardedMap) TestSet() {
	t.Run("create", func() {
		m, err := NewShardedMap[string, string](3, nil)
		t.NoError(err)

		i, created, err := m.Set("showme", func(i string, found bool) (string, error) {
			t.False(found)
			t.Zero(i)

			return "showme", nil
		})
		t.NoError(err)
		t.True(created)
		t.Equal("showme", i)
		t.Equal(1, m.Len())
	})

	t.Run("override", func() {
		m, err := NewShardedMap[string, string](3, nil)
		t.NoError(err)

		m.SetValue("showme", "showme")

		i, created, err := m.Set("showme", func(i string, found bool) (string, error) {
			t.True(found)
			t.Equal("showme", i)

			return "findme", nil
		})
		t.NoError(err)
		t.False(created)
		t.Equal("findme", i)
		t.Equal(1, m.Len())
	})

	t.Run("ignore", func() {
		m, err := NewShardedMap[string, string](3, nil)
		t.NoError(err)

		m.SetValue("showme", "showme")

		i, created, err := m.Set("showme", func(i string, found bool) (string, error) {
			t.True(found)
			t.Equal("showme", i)

			return "findme", ErrLockedSetIgnore
		})
		t.NoError(err)
		t.False(created)
		t.Equal("showme", i)
		t.Equal(1, m.Len())
	})

	t.Run("error", func() {
		m, err := NewShardedMap[string, string](3, nil)
		t.NoError(err)

		m.SetValue("showme", "showme")

		i, created, err := m.Set("showme", func(i string, found bool) (string, error) {
			t.True(found)
			t.Equal("showme", i)

			return "findme", errors.Errorf("eatme")
		})
		t.Error(err)
		t.False(created)
		t.Zero(i)
		t.Equal(1, m.Len())
		t.ErrorContains(err, "eatme")
	})
}

func (t *testShardedMap) TestRemove() {
	m, err := NewShardedMap[string, int](3, nil)
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

			return ErrLockedSetIgnore
		})
		t.NoError(err)
		t.False(removed)

		i, found := m.Value("showme")
		t.True(found)
		t.Equal(1, i)
	})
}

func (t *testShardedMap) TestTraverse() {
	m, err := NewShardedMap[int, int](3, nil)
	t.NoError(err)

	sets := make([]int, 9)
	for i := range make([]int, 9) {
		t.True(m.SetValue(i, i))
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

func (t *testShardedMap) TestTraverseMap() {
	t.Run("1 depth", func() {
		l, _ := NewShardedMap[int, int](3, nil)

		for i := range make([]int, 3) {
			l.SetValue(i, i)
		}

		var mcount int
		l.TraverseMap(func(m LockedMap[int, int]) bool {
			mcount++

			return true
		})

		t.Equal(3, mcount)
	})

	t.Run("multiple depth #0", func() {
		l, _ := NewDeepShardedMap[int, int]([]uint64{7, 7, 7}, nil)

		for i := range make([]int, 343) {
			l.SetValue(i, i)
			v, _ := l.Value(i)
			t.Equal(i, v)
		}

		var mcount int
		l.TraverseMap(func(m LockedMap[int, int]) bool {
			mcount++

			return true
		})

		t.Equal(343, mcount)
	})

	t.Run("multiple depth #1", func() {
		l, _ := NewDeepShardedMap[int, int]([]uint64{7, 7}, nil)

		for i := range make([]int, 49) {
			l.SetValue(i, i)
			v, _ := l.Value(i)
			t.Equal(i, v)
		}

		var mcount int
		l.TraverseMap(func(m LockedMap[int, int]) bool {
			mcount++

			return true
		})

		t.Equal(49, mcount)
	})
}

func (t *testShardedMap) TestClose() {
	t.Run("close", func() {
		m, err := NewShardedMap[int, int](3, nil)
		t.NoError(err)

		sets := make([]int, 9)
		for i := range make([]int, 9) {
			t.True(m.SetValue(i, i))
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

		err = m.GetOrCreate(1,
			func(int, bool) error {
				return nil
			},
			func() (int, error) {
				return 3, nil
			},
		)
		t.Error(err)
		t.ErrorIs(err, ErrLockedMapClosed)

		i, created, err := m.Set(1, func(int, bool) (int, error) {
			return 3, nil
		})
		t.Error(err)
		t.Zero(i)
		t.False(created)
		t.ErrorIs(err, ErrLockedMapClosed)
	})

	t.Run("close empty", func() {
		m, err := NewShardedMap[int, int](3, nil)
		t.NoError(err)

		m.Close()
		t.Equal(0, m.Len())
	})
}

func (t *testShardedMap) TestEmpty() {
	t.Run("empty", func() {
		m, err := NewShardedMap[int, int](3, nil)
		t.NoError(err)

		sets := make([]int, 9)
		for i := range make([]int, 9) {
			t.True(m.SetValue(i, i))
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

		t.True(m.SetValue(1, 2))

		t.NoError(m.GetOrCreate(2,
			func(i int, created bool) error {
				t.True(created)
				t.Equal(3, i)

				return nil
			},
			func() (int, error) {
				return 3, nil
			},
		))
		t.NoError(err)

		i, created, err := m.Set(1, func(int, bool) (int, error) {
			return 4, nil
		})
		t.NoError(err)
		t.False(created)
		t.Equal(4, i)
	})

	t.Run("empty", func() {
		m, err := NewShardedMap[int, int](3, nil)
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
		_, err := NewLockedMap[string, string](0, nil)
		t.Error(err)
		t.ErrorContains(err, "empty size")
	})

	t.Run("1 size == SingleLockedMap", func() {
		l, err := NewLockedMap[string, string](1, nil)
		t.NoError(err)

		_ = l.(*SingleLockedMap[string, string])
	})

	t.Run("over 1 size == ShardedMap", func() {
		l, err := NewLockedMap[string, string](3, nil)
		t.NoError(err)

		_ = l.(*ShardedMap[string, string])
	})
}

type testLimitedLockedMap struct {
	suite.Suite
}

func (t *testLimitedLockedMap) TestNew() {
	m := NewSingleLockedMap[string, int]()

	l := NewLimitedLockedMap(m, func(k string) bool {
		switch k {
		case "0":
			return true
		default:
			return false
		}
	})

	t.True(m.SetValue("0", 10))
	t.True(m.SetValue("1", 11))

	t.Run("Exists", func() {
		t.True(m.Exists("0"))
		t.True(m.Exists("1"))
		t.False(m.Exists("2"))

		t.True(l.Exists("0"))
		t.False(l.Exists("1"))
		t.False(l.Exists("2"))
	})

	t.Run("Value", func() {
		i, found := l.Value("0")
		t.Equal(10, i)
		t.True(found)
		i, found = l.Value("1")
		t.Equal(0, i)
		t.False(found)
		i, found = l.Value("2")
		t.Equal(0, i)
		t.False(found)
	})

	t.Run("SetValue", func() {
		added := l.SetValue("0", 20)
		t.False(added)
		i, found := m.Value("0")
		t.True(found)
		t.Equal(20, i)

		added = l.SetValue("1", 21)
		t.False(added)
		i, found = m.Value("1")
		t.True(found)
		t.Equal(11, i)
	})

	t.Run("RemoveValue", func() {
		defer func() {
			m.SetValue("0", 10)
			m.SetValue("1", 11)
		}()

		removed := l.RemoveValue("0")
		t.True(removed)

		_, found := m.Value("0")
		t.False(found)

		removed = l.RemoveValue("1")
		t.False(removed)
	})

	t.Run("Get", func() {
		t.NoError(l.Get("0", func(v int, found bool) error {
			t.Equal(10, v)
			t.True(found)

			return nil
		}))

		t.NoError(l.Get("1", func(v int, found bool) error {
			t.Equal(0, v)
			t.False(found)

			return nil
		}))

		err := l.Get("1", func(v int, found bool) error {
			t.Equal(0, v)
			t.False(found)

			return errors.Errorf("hohoho")
		})
		t.ErrorContains(err, "hohoho")

		l.Close()
		defer func() {
			l.isclosed = false
		}()

		err = l.Get("1", func(v int, found bool) error {
			t.Equal(0, v)
			t.False(found)

			return errors.Errorf("hohoho")
		})
		t.ErrorIs(err, ErrLockedMapClosed)
	})

	t.Run("GetOrCreate", func() {
		t.NoError(l.GetOrCreate("0", func(v int, created bool) error {
			t.Equal(10, v)
			t.False(created)

			return nil
		}, func() (int, error) {
			return 0, nil
		}))

		var called bool

		t.NoError(l.GetOrCreate("1", func(v int, created bool) error {
			called = true

			return nil
		}, func() (int, error) {
			return 0, nil
		}))
		t.False(called)

		t.NoError(l.GetOrCreate("2", func(v int, created bool) error {
			called = true

			return nil
		}, func() (int, error) {
			return 0, nil
		}))
		t.False(called)

		l.Close()
		defer func() {
			l.isclosed = false
		}()

		err := l.GetOrCreate("2", func(v int, created bool) error {
			called = true

			return nil
		}, func() (int, error) {
			return 0, nil
		})
		t.ErrorIs(err, ErrLockedMapClosed)
	})

	t.Run("GetOrCreate; new key", func() {
		t.False(m.Exists("2"))

		l := NewLimitedLockedMap(m, func(k string) bool {
			switch k {
			case "0", "2":
				return true
			default:
				return false
			}
		})

		t.NoError(l.GetOrCreate("2", func(v int, created bool) error {
			t.Equal(20, v)
			t.True(created)

			return nil
		}, func() (int, error) {
			return 20, nil
		}))

		t.True(l.Exists("2"))
	})

	t.Run("Set", func() {
		defer func() {
			m.SetValue("0", 10)
			m.SetValue("1", 11)
		}()

		v, created, err := l.Set("0", func(v int, found bool) (int, error) {
			t.Equal(10, v)
			t.True(found)

			return 20, nil
		})
		t.Equal(20, v)
		t.False(created)
		t.NoError(err)

		var called bool

		v, created, err = l.Set("1", func(v int, found bool) (int, error) {
			called = true

			return 20, nil
		})
		t.Equal(0, v)
		t.False(created)
		t.NoError(err)
		t.False(called)

		l.Close()
		defer func() {
			l.isclosed = false
		}()

		_, _, err = l.Set("1", func(v int, found bool) (int, error) {
			called = true

			return 20, nil
		})
		t.ErrorIs(err, ErrLockedMapClosed)
	})

	t.Run("Remove", func() {
		defer func() {
			m.SetValue("0", 10)
			m.SetValue("1", 11)
		}()

		removed, err := l.Remove("0", func(v int, found bool) error {
			t.Equal(10, v)
			t.True(found)

			return nil
		})
		t.NoError(err)
		t.True(removed)

		var called bool

		removed, err = l.Remove("1", func(v int, found bool) error {
			called = true

			return nil
		})
		t.NoError(err)
		t.False(removed)
		t.False(called)

		l.Close()
		defer func() {
			l.isclosed = false
		}()

		_, err = l.Remove("1", func(v int, found bool) error {
			called = true

			return nil
		})
		t.ErrorIs(err, ErrLockedMapClosed)
	})

	t.Run("SetOrRemove; set", func() {
		defer func() {
			m.SetValue("0", 10)
			m.SetValue("1", 11)
		}()

		v, created, removed, err := l.SetOrRemove("0", func(v int, found bool) (int, bool, error) {
			t.Equal(10, v)
			t.True(found)

			return 20, false, nil
		})
		t.NoError(err)
		t.False(created)
		t.False(removed)
		t.Equal(20, v)

		var called bool

		v, created, removed, err = l.SetOrRemove("1", func(v int, found bool) (int, bool, error) {
			called = true

			return 21, false, nil
		})
		t.NoError(err)
		t.False(created)
		t.False(removed)
		t.False(called)
		t.Equal(0, v)

		l.Close()
		defer func() {
			l.isclosed = false
		}()

		_, _, _, err = l.SetOrRemove("1", func(v int, found bool) (int, bool, error) {
			called = true

			return 21, false, nil
		})
		t.ErrorIs(err, ErrLockedMapClosed)
	})

	t.Run("SetOrRemove; remove", func() {
		defer func() {
			m.SetValue("0", 10)
			m.SetValue("1", 11)
		}()

		v, created, removed, err := l.SetOrRemove("0", func(v int, found bool) (int, bool, error) {
			t.Equal(10, v)
			t.True(found)

			return 20, true, nil
		})
		t.NoError(err)
		t.False(created)
		t.True(removed)
		t.Equal(0, v)
		t.False(l.Exists("0"))

		var called bool

		v, created, removed, err = l.SetOrRemove("1", func(v int, found bool) (int, bool, error) {
			called = true

			return 0, true, nil
		})
		t.NoError(err)
		t.False(created)
		t.False(removed)
		t.Equal(0, v)
		t.False(called)
	})

	t.Run("Traverse", func() {
		t.True(l.Traverse(func(k string, v int) bool {
			t.Equal("0", k)
			t.Equal(10, v)

			return true
		}))
	})

	t.Run("Len", func() {
		t.Equal(1, l.Len())
	})

	t.Run("Map", func() {
		m := l.Map()

		t.Equal(1, len(m))
		v, found := m["0"]
		t.True(found)
		t.Equal(10, v)
	})

	t.Run("Len: Empty, Close", func() {
		l.Empty()
		t.Equal(1, l.Len())

		l.Close()
		t.Equal(0, l.Len())
	})
}

func TestLimitedLockedMap(t *testing.T) {
	suite.Run(t, new(testLimitedLockedMap))
}

type testMultiKeysLockedMap struct {
	logging.BaseTestSuite
}

func (t *testMultiKeysLockedMap) TestClose() {
	m := NewSingleLockedMap[string, int]()
	defer m.Close()

	m.SetValue("0", 10)
	m.SetValue("1", 11)

	mk := NewMultiKeysLockedMap(m)

	t.True(m.Exists("0"))
	t.True(m.Exists("1"))

	mk.Close()

	t.True(m.Exists("0"))
	t.True(m.Exists("1"))
}

func (t *testMultiKeysLockedMap) TestLock() {
	m := NewSingleLockedMap[string, int]()
	defer m.Close()

	m.SetValue("0", 10)
	m.SetValue("1", 11)
	m.SetValue("2", 12)
	m.SetValue("3", 13)

	mk := NewMultiKeysLockedMap(m)
	defer mk.Close()

	t.Run("empty keys", func() {
		l, unlock, err := mk.Lock([]string{})
		t.NoError(err)
		t.Nil(l)
		t.Nil(unlock)
	})

	t.Run("duplicated keys", func() {
		l, unlock, err := mk.Lock([]string{"0", "1", "1", "0"})
		t.NoError(err)
		defer unlock()

		t.True(l.Exists("0"))
		t.True(l.Exists("1"))
		t.False(l.Exists("2"))
		t.False(l.Exists("3"))

		t.Equal(2, len(mk.lockchs))
	})

	t.Run("0, 1", func() {
		l, unlock, err := mk.Lock([]string{"0", "1"})
		t.NoError(err)
		t.True(l.Exists("0"))
		t.True(l.Exists("1"))
		t.False(l.Exists("2"))
		t.False(l.Exists("3"))

		unlockedch := make(chan bool)
		go func() {
			_, unlock, err := mk.Lock([]string{"1", "2"})
			if err != nil {
				panic(err)
			}

			defer unlock()

			close(unlockedch)
		}()

		t.T().Log("not yet unlock()")

		select {
		case <-time.After(time.Second):
		case <-unlockedch:
			t.Fail("before unlock(), unlocked")
			return
		}

		t.T().Log("unlock()")
		unlock()

		select {
		case <-time.After(time.Second):
			t.Fail("after unlock(), not unlocked")
		case <-unlockedch:
		}
	})

	t.Run("0, 1 with unknown key", func() {
		l, unlock, err := mk.Lock([]string{"0", "1", "9"})
		t.NoError(err)
		t.True(l.Exists("0"))
		t.True(l.Exists("1"))
		t.False(l.Exists("2"))
		t.False(l.Exists("3"))
		t.False(l.Exists("9"))

		unlockedch := make(chan bool)
		go func() {
			l2, unlock, err := mk.Lock([]string{"9"})
			if err != nil {
				panic(err)
			}
			defer unlock()

			t.True(l2.SetValue("9", 9))

			close(unlockedch)
		}()

		t.T().Log("not yet unlock()")

		select {
		case <-time.After(time.Second):
		case <-unlockedch:
			t.Fail("before unlock(), unlocked")
			return
		}

		t.T().Log("unlock()")
		unlock()

		select {
		case <-time.After(time.Second):
			t.Fail("after unlock(), not unlocked")
		case <-unlockedch:
		}
	})

	t.Run("unlock; LimitedLockedMap closed", func() {
		l, unlock, err := mk.Lock([]string{"0", "1", "1", "0"})
		t.NoError(err)
		t.True(l.Exists("0"))

		unlock()

		err = l.Get("0", func(int, bool) error { return nil })
		t.ErrorIs(err, ErrLockedMapClosed)
	})
}

func (t *testMultiKeysLockedMap) TestLockFunc() {
	m := NewSingleLockedMap[string, int]()
	defer m.Close()

	m.SetValue("0", 10)
	m.SetValue("1", 11)
	m.SetValue("2", 12)
	m.SetValue("3", 13)

	mk := NewMultiKeysLockedMap(m)
	defer mk.Close()

	t.Run("empty keys", func() {
		var called bool

		t.NoError(mk.LockFunc([]string{},
			func(l LockedMap[string, int]) error {
				called = true

				return nil
			},
		))
		t.False(called)
	})

	t.Run("duplicated keys", func() {
		t.NoError(mk.LockFunc(
			[]string{"0", "1", "1", "0"},
			func(l LockedMap[string, int]) error {
				t.Equal(2, len(mk.lockchs))

				return nil
			},
		))
	})

	t.Run("0, 1", func() {
		called01ch := make(chan bool)
		unlock01ch := make(chan bool)
		locked12ch := make(chan bool)
		locked13ch := make(chan bool)

		go func() {
			t.LockedLog("trying to lock 0, 1")

			err := mk.LockFunc(
				[]string{"0", "1"},
				func(l LockedMap[string, int]) error {
					close(called01ch)

					t.LockedLog("locked 0, 1")

					<-unlock01ch
					t.LockedLog("unlocked 0, 1")

					return nil
				},
			)
			if err != nil {
				panic(err)
			}
		}()

		go func() {
			<-called01ch

			t.LockedLog("trying to lock 1, 2")

			err := mk.LockFunc(
				[]string{"1", "2"},
				func(l LockedMap[string, int]) error {
					t.LockedLog("locked 1, 2")

					close(locked12ch)

					return nil
				},
			)
			if err != nil {
				panic(err)
			}
		}()

		go func() {
			<-called01ch

			t.LockedLog("trying to lock 1, 3")

			err := mk.LockFunc(
				[]string{"1", "3"},
				func(l LockedMap[string, int]) error {
					t.LockedLog("locked 1, 3")

					close(locked13ch)

					return nil
				},
			)
			if err != nil {
				panic(err)
			}
		}()

		select {
		case <-time.After(time.Second):
		case <-locked12ch:
			t.Fail("before unlock(), 1, 2 unlocked")
			return
		case <-locked13ch:
			t.Fail("before unlock(), 1, 3 unlocked")
			return
		}

		t.T().Log("unlock() 0, 1")
		close(unlock01ch)

		select {
		case <-time.After(time.Second):
			t.Fail("after unlock(), not unlocked")
		case <-locked12ch:
		case <-locked13ch:
		}
	})
}

func (t *testMultiKeysLockedMap) TestAsyncLockFunc() {
	m := NewSingleLockedMap[int, int]()
	defer m.Close()

	allkeys := make([]int, 99)

	for i := range allkeys {
		m.SetValue(i, i)
		m.SetValue(i, i)
		m.SetValue(i, i)
		m.SetValue(i, i)

		allkeys[i] = i
	}

	mk := NewMultiKeysLockedMap(m)
	defer mk.Close()

	worksize := 33

	run := func(i int, keys []int) error {
		if i%10 == 0 {
			keys = append(keys, 0)
		}

		t.LockedLog("> %d: locked: %v", i, keys)
		defer t.LockedLog("< %d: unlocked: %v", i, keys)

		return mk.LockFunc(keys, func(m LockedMap[int, int]) error {
			for k := range keys {
				m.Set(keys[k], func(v int, _ bool) (int, error) {
					return v * 3, nil
				})
			}

			<-time.After(time.Millisecond * 333)

			return nil
		})
	}

	var wg sync.WaitGroup
	wg.Add(worksize)

	for i := range make([]int, worksize) {
		i := i

		go func() {
			defer wg.Done()

			keys, err := RandomChoiceSlice(allkeys, 3)
			if err != nil {
				panic(err)
			}

			if err := run(i, keys); err != nil {
				panic(err)
			}
		}()
	}
	wg.Wait()

	t.T().Log("0 called at last")
	t.NoError(mk.LockFunc([]int{0}, func(m LockedMap[int, int]) error {
		t.T().Logf("> locked: 0")
		defer t.T().Logf("< unlocked: 0")

		m.SetValue(0, -1)

		return nil
	}))

	i0, found := m.Value(0)
	t.True(found)
	t.Equal(-1, i0)
}

func (t *testMultiKeysLockedMap) TestDataRace() {
	type ptr struct {
		i int
	}

	m := NewSingleLockedMap[int, *ptr]()
	defer m.Close()

	allkeys := make([]int, 4)

	for i := range allkeys {
		m.SetValue(i, &ptr{i: 0})

		allkeys[i] = i
	}

	mk := NewMultiKeysLockedMap(m)
	defer mk.Close()

	run := func(i int, keys []int) error {
		t.LockedLog("locked: %d %v", i, keys)

		return mk.LockFunc(keys, func(m LockedMap[int, *ptr]) error {
			for k := range keys {
				m.Set(keys[k], func(v *ptr, _ bool) (*ptr, error) {
					v.i *= 2

					return v, nil
				})
			}

			return nil
		})
	}

	worksize := math.MaxInt16

	var wg sync.WaitGroup
	wg.Add(worksize)

	for i := range make([]int, worksize) {
		i := i

		go func() {
			defer wg.Done()

			keys, err := RandomChoiceSlice(allkeys, 3)
			if err != nil {
				panic(err)
			}

			if err := run(i, keys); err != nil {
				panic(err)
			}
		}()
	}

	wg.Wait()
}

func TestMultiKeysLockedMap(t *testing.T) {
	suite.Run(t, new(testMultiKeysLockedMap))
}
