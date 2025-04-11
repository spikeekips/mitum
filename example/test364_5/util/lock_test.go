package util

import (
	"iter"
	"maps"
	"math"
	"slices"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/pkg/errors"
	"github.com/spikeekips/mitum/util"
	"github.com/stretchr/testify/suite"
)

type testSingleLockedMap struct {
	BaseSuite
}

func (t *testSingleLockedMap) TestNew() {
	l := NewSingleLockedMap[string, int]()
	t.Equal(0, l.Len())

	var _ LockedMap[string, int] = l
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

func (t *testSingleLockedMap) TestRemoveValues() {
	l := NewSingleLockedMap[string, int]()

	t.Run("ok", func() {
		l.SetValue("showme", 1)

		i, found := l.Value("showme")
		t.True(found)
		t.Equal(1, i)

		removed := l.RemoveValues("showme")
		t.Equal(1, removed)

		i, found = l.Value("showme")
		t.False(found)
		t.Zero(i)

		removed = l.RemoveValues("showme")
		t.Equal(0, removed)
	})

	t.Run("multiple keys", func() {
		l.SetValue("a", 1)
		l.SetValue("b", 2)
		l.SetValue("c", 3)

		t.Equal(2, l.RemoveValues("a", "b"))

		i, found := l.Value("a")
		t.Zero(i)
		t.False(found)
		i, found = l.Value("b")
		t.Zero(i)
		t.False(found)
		i, found = l.Value("c")
		t.Equal(3, i)
		t.True(found)
	})
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
			return errors.New("findme")
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
			func() (int, error) {
				return 2, nil
			},
			func(v int, created bool) error {
				t.False(created)
				t.Equal(1, v)

				return nil
			},
		))
	})

	t.Run("create", func() {
		t.NoError(l.GetOrCreate("findme",
			func() (int, error) {
				return 2, nil
			},
			func(i int, created bool) error {
				t.True(created)
				t.Equal(2, i)

				return nil
			},
		))
	})

	t.Run("ignore", func() {
		t.NoError(l.GetOrCreate("eatme",
			func() (int, error) {
				return 2, ErrLockedSetIgnore
			},
			func(i int, created bool) error {
				t.False(created)
				t.Zero(i)

				return nil
			},
		))
	})

	t.Run("error", func() {
		err := l.GetOrCreate("eatme",
			func() (int, error) {
				return 2, errors.New("eatme")
			},
			func(int, bool) error {
				return nil
			},
		)

		t.Error(err)
		t.ErrorContains(err, "eatme")
	})
}

func (t *testSingleLockedMap) TestSet() {
	l := NewSingleLockedMap[string, int]()

	t.Run("new value", func() {
		created, err := l.Set("showme", func(i int, found bool) (int, error) {
			return 1, nil
		})
		t.NoError(err)
		t.True(created)

		i, found := l.Value("showme")
		t.True(found)
		t.Equal(1, i)
	})

	t.Run("existing value", func() {
		created, err := l.Set("showme", func(i int, found bool) (int, error) {
			t.True(found)
			t.Equal(1, i)

			return 2, nil
		})
		t.NoError(err)
		t.False(created)

		i, found := l.Value("showme")
		t.True(found)
		t.Equal(2, i)
	})

	t.Run("ignore", func() {
		created, err := l.Set("showme", func(i int, found bool) (int, error) {
			t.True(found)
			t.Equal(2, i)

			return 3, ErrLockedSetIgnore
		})
		t.NoError(err)
		t.False(created)

		i, found := l.Value("showme")
		t.True(found)
		t.Equal(2, i)
	})

	t.Run("error", func() {
		created, err := l.Set("showme", func(i int, found bool) (int, error) {
			t.True(found)
			t.Equal(2, i)

			return 3, errors.New("eatme")
		})
		t.Error(err)
		t.False(created)
		t.ErrorContains(err, "eatme")

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
		created, removed, err := l.SetOrRemove("showme", func(i int, found bool) (int, bool, error) {
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
		created, removed, err := l.SetOrRemove("showme", func(i int, found bool) (int, bool, error) {
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
		created, removed, err := l.SetOrRemove("findme", func(i int, found bool) (int, bool, error) {
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
		created, removed, err := l.SetOrRemove("showme", func(i int, found bool) (int, bool, error) {
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

func (t *testSingleLockedMap) TestSeq2() {
	l := NewSingleLockedMap[string, int]()

	kv := map[string]int{}

	for i := range make([]int, 33) {
		k := util.UUID().String()

		l.SetValue(k, i)
		kv[k] = i
	}

	t.Run("all", func() {
		rkv := map[string]int{}

		for k, v := range l.Seq2 {
			rkv[k] = v
		}

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

		for k, v := range l.Seq2 {
			rkv[k] = v

			if len(rkv) == 9 {
				break
			}
		}

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
		func() (int, error) {
			return 2, nil
		},
		func(int, bool) error {
			return nil
		},
	)
	t.Error(err)
	t.ErrorIs(err, ErrLockedMapClosed)

	created, err := l.Set("showme", func(i int, found bool) (int, error) {
		t.False(found)
		t.Zero(i)

		return 2, nil
	})
	t.Error(err)
	t.False(created)
	t.ErrorIs(err, ErrLockedMapClosed)
}

func (t *testSingleLockedMap) TestLock() {
	t.Run("empty; get, set, delete", func() {
		l := NewSingleLockedMap[int, int]()

		var wg sync.WaitGroup

		for range 33 {
			wg.Add(1)

			go func() {
				defer wg.Done()

				t.IfErrPanic(l.Lock(func(
					get func(int) (int, bool),
					set func(int, int) bool,
					remove func(int) bool,
					traverse iter.Seq2[int, int],
				) error {
					_, found := get(3)
					t.False(found)

					created := set(3, 4)
					t.True(created)

					_, found = get(3)
					t.True(found)

					removed := remove(3)
					t.True(removed)

					t.Empty(maps.Collect(traverse))

					return nil
				}))
			}()
		}

		wg.Wait()

		t.Equal(0, l.Len())
	})

	t.Run("error", func() {
		l := NewSingleLockedMap[int, int]()

		err := l.Lock(func(
			get func(int) (int, bool),
			_ func(int, int) bool,
			remove func(int) bool,
			_ iter.Seq2[int, int],
		) error {
			return errors.New("showme")
		})
		t.ErrorContains(err, "showme")
	})
}

func TestSingleLockedMap(t *testing.T) {
	suite.Run(t, new(testSingleLockedMap))
}

type testShardedLockedMap struct {
	BaseSuite
}

func (t *testShardedLockedMap) TestNew() {
	m, err := NewShardedLockedMap[string, string](nil, 3)
	t.NoError(err)

	var _ LockedMap[string, string] = m

	t.Run("over 1", func() {
		m, err := NewShardedLockedMap[string, string](nil, 3)
		t.NoError(err)
		t.Equal(0, m.Len())
	})

	t.Run("0 size", func() {
		_, err := NewShardedLockedMap[string, string](nil, 0)
		t.Error(err)
		t.ErrorContains(err, "empty size")
	})

	t.Run("1 size", func() {
		_, err := NewShardedLockedMap[string, string](nil, 1)
		t.Error(err)
		t.ErrorContains(err, "1 size")
	})
}

func (t *testShardedLockedMap) TestSetValue() {
	t.Run("new", func() {
		m, err := NewShardedLockedMap[string, string](nil, 3)
		t.NoError(err)

		t.True(m.SetValue("showme", "showme"))
		i, found := m.Value("showme")
		t.True(found)
		t.Equal("showme", i)
		t.Equal(1, m.Len())

		t.True(m.Exists("showme"))
		t.False(m.Exists(util.UUID().String()))

		t.True(m.SetValue("findme", "findme"))
		i, found = m.Value("findme")
		t.True(found)
		t.Equal("findme", i)
		t.Equal(2, m.Len())
	})

	t.Run("exists", func() {
		m, err := NewShardedLockedMap[string, string](nil, 3)
		t.NoError(err)

		t.True(m.SetValue("showme", "showme"))
		t.False(m.SetValue("showme", "findme"))

		i, found := m.Value("showme")
		t.True(found)
		t.Equal("findme", i)
		t.Equal(1, m.Len())
	})

	t.Run("over size", func() {
		m, err := NewShardedLockedMap[int, int](nil, 2)
		t.NoError(err)

		expected := map[int]int{0: 0, 1: 1, 2: 2}
		for k, v := range maps.All(expected) {
			t.True(m.SetValue(k, v))
		}

		t.Equal(expected, m.Map())
	})
}

func (t *testShardedLockedMap) TestRemoveValues() {
	t.Run("unknown", func() {
		m, err := NewShardedLockedMap[string, string](nil, 3)
		t.NoError(err)

		t.Equal(0, m.Len())

		t.Equal(0, m.RemoveValues(util.UUID().String()))
	})

	t.Run("known", func() {
		m, err := NewShardedLockedMap[string, string](nil, 3)
		t.NoError(err)

		t.Equal(0, m.RemoveValues(util.UUID().String()))

		t.True(m.SetValue("showme", "showme"))
		t.Equal(1, m.Len())

		t.Equal(1, m.RemoveValues("showme"))
		t.Equal(0, m.Len())
	})

	t.Run("multiple keys", func() {
		m, err := NewShardedLockedMap[string, string](nil, 3)
		t.NoError(err)

		t.True(m.SetValue("a", "a"))
		t.True(m.SetValue("b", "b"))
		t.True(m.SetValue("c", "c"))

		t.T().Log("index:", func() string {
			var s []string

			for i := range slices.Values(m.sharded) {
				if i == nil {
					s = append(s, "x")
				} else {
					s = append(s, "o")
				}
			}

			return "{" + strings.Join(s, " ") + "}"
		}())

		t.Equal(0, m.RemoveValues(util.UUID().String()))

		t.Equal(2, m.RemoveValues("a", "b"))

		i, found := m.Value("a")
		t.Zero(i)
		t.False(found)
		i, found = m.Value("b")
		t.Zero(i)
		t.False(found)
		i, found = m.Value("c")
		t.Equal("c", i)
		t.True(found)
	})
}

func (t *testShardedLockedMap) TestGetOrCreate() {
	t.Run("create", func() {
		m, err := NewShardedLockedMap[string, string](nil, 3)
		t.NoError(err)

		t.NoError(m.GetOrCreate(
			"showme",
			func() (string, error) {
				return "showme", nil
			},
			func(i string, created bool) error {
				t.NoError(err)
				t.True(created)
				t.Equal("showme", i)

				return nil
			},
		))
		t.Equal(1, m.Len())
	})

	t.Run("known", func() {
		m, err := NewShardedLockedMap[string, string](nil, 3)
		t.NoError(err)

		t.True(m.SetValue("showme", "showme"))
		t.Equal(1, m.Len())

		t.NoError(m.GetOrCreate("showme",
			func() (string, error) {
				return "findme", nil
			},
			func(i string, created bool) error {
				t.False(created)
				t.Equal("showme", i)

				return nil
			},
		))
		t.NoError(err)
		t.Equal(1, m.Len())
	})

	t.Run("unknown, ignore", func() {
		m, err := NewShardedLockedMap[string, string](nil, 3)
		t.NoError(err)

		t.NoError(m.GetOrCreate("showme",
			func() (string, error) {
				return "showme", ErrLockedSetIgnore
			},
			func(string, bool) error {
				return nil
			},
		))

		t.False(m.Exists("showme"))

		i, found := m.Value("showme")
		t.False(found)
		t.Zero(i)
		t.Equal(0, m.Len())
	})

	t.Run("unknown, error", func() {
		m, err := NewShardedLockedMap[string, string](nil, 3)
		t.NoError(err)

		err = m.GetOrCreate("showme",
			func() (string, error) {
				return "showme", errors.New("findme")
			},
			func(string, bool) error {
				return nil
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
		m, err := NewShardedLockedMap[string, string](nil, 3)
		t.NoError(err)

		t.True(m.SetValue("showme", "showme"))
		t.Equal(1, m.Len())

		t.NoError(m.GetOrCreate("showme",
			func() (string, error) {
				return "showme", errors.New("findme")
			},
			func(i string, created bool) error {
				t.False(created)
				t.Equal("showme", i)

				return nil
			},
		))
		t.NoError(err)

		t.Equal(1, m.Len())
	})
}

func (t *testShardedLockedMap) TestSet() {
	t.Run("create", func() {
		m, err := NewShardedLockedMap[string, string](nil, 3)
		t.NoError(err)

		created, err := m.Set("showme", func(i string, found bool) (string, error) {
			t.False(found)
			t.Zero(i)

			return "showme", nil
		})
		t.NoError(err)
		t.True(created)
		t.Equal(1, m.Len())
	})

	t.Run("override", func() {
		m, err := NewShardedLockedMap[string, string](nil, 3)
		t.NoError(err)

		m.SetValue("showme", "showme")

		created, err := m.Set("showme", func(i string, found bool) (string, error) {
			t.True(found)
			t.Equal("showme", i)

			return "findme", nil
		})
		t.NoError(err)
		t.False(created)
		t.Equal(1, m.Len())
	})

	t.Run("ignore", func() {
		m, err := NewShardedLockedMap[string, string](nil, 3)
		t.NoError(err)

		m.SetValue("showme", "showme")

		created, err := m.Set("showme", func(i string, found bool) (string, error) {
			t.True(found)
			t.Equal("showme", i)

			return "findme", ErrLockedSetIgnore
		})
		t.NoError(err)
		t.False(created)
		t.Equal(1, m.Len())
	})

	t.Run("error", func() {
		m, err := NewShardedLockedMap[string, string](nil, 3)
		t.NoError(err)

		m.SetValue("showme", "showme")

		created, err := m.Set("showme", func(i string, found bool) (string, error) {
			t.True(found)
			t.Equal("showme", i)

			return "findme", errors.New("eatme")
		})
		t.Error(err)
		t.False(created)
		t.Equal(1, m.Len())
		t.ErrorContains(err, "eatme")
	})
}

func (t *testShardedLockedMap) TestRemove() {
	m, err := NewShardedLockedMap[string, int](nil, 3)
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

func (t *testShardedLockedMap) TestSeq2() {
	m, err := NewShardedLockedMap[int, int](nil, 3)
	t.NoError(err)

	sets := make([]int, 9)
	for i := range make([]int, 9) {
		t.True(m.SetValue(i, i))
		sets[i] = i
	}

	t.Run("all", func() {
		var collected []int

		for k := range m.Seq2 {
			collected = append(collected, k)
		}

		t.Equal(m.Len(), len(collected))
		sort.Ints(collected)

		t.Equal(sets, collected)
	})

	t.Run("stop", func() {
		var collected []int

		for k := range m.Seq2 {
			collected = append(collected, k)
			if len(collected) == 3 {
				break
			}
		}

		t.Equal(3, len(collected))
	})
}

func (t *testShardedLockedMap) TestClose() {
	t.Run("close", func() {
		m, err := NewShardedLockedMap[int, int](nil, 3)
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
			func() (int, error) {
				return 3, nil
			},
			func(int, bool) error {
				return nil
			},
		)
		t.Error(err)
		t.ErrorIs(err, ErrLockedMapClosed)

		created, err := m.Set(1, func(int, bool) (int, error) {
			return 3, nil
		})
		t.Error(err)
		t.False(created)
		t.ErrorIs(err, ErrLockedMapClosed)
	})

	t.Run("close empty", func() {
		m, err := NewShardedLockedMap[int, int](nil, 3)
		t.NoError(err)

		m.Close()
		t.Equal(0, m.Len())
	})
}

func (t *testShardedLockedMap) TestEmpty() {
	t.Run("empty", func() {
		m, err := NewShardedLockedMap[int, int](nil, 3)
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
			func() (int, error) {
				return 3, nil
			},
			func(i int, created bool) error {
				t.True(created)
				t.Equal(3, i)

				return nil
			},
		))
		t.NoError(err)

		created, err := m.Set(1, func(int, bool) (int, error) {
			return 4, nil
		})
		t.NoError(err)
		t.False(created)
	})

	t.Run("empty", func() {
		m, err := NewShardedLockedMap[int, int](nil, 3)
		t.NoError(err)

		m.Empty()
		t.Equal(0, m.Len())
	})
}

func (t *testShardedLockedMap) TestLock() {
	t.Run("empty; get, set, delete", func() {
		l, err := NewShardedLockedMap[int, int](nil, 3)
		t.NoError(err)

		var wg sync.WaitGroup

		for range 33 {
			wg.Add(1)

			go func() {
				defer wg.Done()

				t.IfErrPanic(l.Lock(func(
					get func(int) (int, bool),
					set func(int, int) bool,
					remove func(int) bool,
					traverse iter.Seq2[int, int],
				) error {
					_, found := get(3)
					t.False(found)

					created := set(3, 4)
					t.True(created)

					_, found = get(3)
					t.True(found)

					removed := remove(3)
					t.True(removed)

					t.Empty(maps.Collect(traverse))

					return nil
				}))
			}()
		}

		wg.Wait()

		t.Equal(0, l.Len())
	})

	t.Run("error", func() {
		l := NewSingleLockedMap[int, int]()

		err := l.Lock(func(
			get func(int) (int, bool),
			_ func(int, int) bool,
			remove func(int) bool,
			_ iter.Seq2[int, int],
		) error {
			return errors.New("showme")
		})
		t.ErrorContains(err, "showme")
	})
}

func TestShardedLockedMap(t *testing.T) {
	suite.Run(t, new(testShardedLockedMap))
}

func TestLockedMap(tt *testing.T) {
	t := new(suite.Suite)

	t.SetT(tt)

	t.Run("0 size", func() {
		_, err := NewLockedMap[string, string](nil, 0)
		t.Error(err)
		t.ErrorContains(err, "empty size")
	})

	t.Run("1 size == SingleLockedMap", func() {
		l, err := NewLockedMap[string, string](nil, 1)
		t.NoError(err)

		_ = l.(*SingleLockedMap[string, string])
	})

	t.Run("over 1 size == ShardedMap", func() {
		l, err := NewLockedMap[string, string](nil, 3)
		t.NoError(err)

		_ = l.(*ShardedLockedMap[string, string])
	})
}

type testMultiKeysLockedMap struct {
	BaseSuite
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
				t.IfErrPanic(err)
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
				t.IfErrPanic(err)
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
			t.Print("trying to lock 0, 1")

			err := mk.LockFunc(
				[]string{"0", "1"},
				func(l LockedMap[string, int]) error {
					close(called01ch)

					t.Print("locked 0, 1")

					<-unlock01ch
					t.Print("unlocked 0, 1")

					return nil
				},
			)
			if err != nil {
				t.IfErrPanic(err)
			}
		}()

		go func() {
			<-called01ch

			t.Print("trying to lock 1, 2")

			err := mk.LockFunc(
				[]string{"1", "2"},
				func(l LockedMap[string, int]) error {
					t.Print("locked 1, 2")

					close(locked12ch)

					return nil
				},
			)
			if err != nil {
				t.IfErrPanic(err)
			}
		}()

		go func() {
			<-called01ch

			t.Print("trying to lock 1, 3")

			err := mk.LockFunc(
				[]string{"1", "3"},
				func(l LockedMap[string, int]) error {
					t.Print("locked 1, 3")

					close(locked13ch)

					return nil
				},
			)
			if err != nil {
				t.IfErrPanic(err)
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
			t.Print("locked12ch released")
		}

		select {
		case <-time.After(time.Second):
			t.Fail("after unlock(), not unlocked")
		case <-locked13ch:
			t.Print("locked13ch released")
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

		t.Print("> %d: locked: %v", i, keys)
		defer t.Print("< %d: unlocked: %v", i, keys)

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

			keys, err := util.RandomChoiceSlice(allkeys, 3)
			if err != nil {
				t.IfErrPanic(err)
			}

			if err := run(i, keys); err != nil {
				t.IfErrPanic(err)
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
		t.Print("locked: %d %v", i, keys)

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

			keys, err := util.RandomChoiceSlice(allkeys, 3)
			if err != nil {
				t.IfErrPanic(err)
			}

			if err := run(i, keys); err != nil {
				t.IfErrPanic(err)
			}
		}()
	}

	wg.Wait()
}

func TestMultiKeysLockedMap(t *testing.T) {
	suite.Run(t, new(testMultiKeysLockedMap))
}

type testSetOnlyLockedMap struct {
	BaseSuite
}

func (t *testSetOnlyLockedMap) newMap(grow uint64) *SetOnlyLockedMap[int, int] {
	l := NewSingleLockedMap[int, int]()

	return NewSetOnlyLockedMap(l, grow)
}

func (t *testSetOnlyLockedMap) TestClose() {
	t.Run("Close()", func() {
		l := t.newMap(1)
		for i := range 3 {
			l.SetValue(i, i)
		}

		l.Close()
		t.Empty(l.keys)
	})

	t.Run("CloseFunc()", func() {
		l := t.newMap(1)
		for i := range 3 {
			l.SetValue(i, i)
		}

		l.CloseFunc(func() {})
		t.Empty(l.keys)
	})
}

func (t *testSetOnlyLockedMap) TestEmpty() {
	l := t.newMap(1)
	for i := range 3 {
		l.SetValue(i, i)
	}

	l.Empty()
	t.Empty(l.keys)
}

func (t *testSetOnlyLockedMap) TestSetValue() {
	t.Run("ok", func() {
		l := t.newMap(1)
		for i := range 3 {
			l.SetValue(i, i)
		}

		t.Equal([]int{0, 1, 2}, l.keys)
	})

	t.Run("duplicate", func() {
		l := t.newMap(1)
		for i := range 3 {
			l.SetValue(i, i)
		}
		for i := range l.Len() {
			l.SetValue(i, i)
		}

		t.Equal([]int{0, 1, 2}, l.keys)
	})
}

func (t *testSetOnlyLockedMap) TestSet() {
	t.Run("ok", func() {
		l := t.newMap(1)
		for i := range 3 {
			l.Set(i, func(int, bool) (int, error) { return i, nil })
		}

		t.Equal([]int{0, 1, 2}, l.keys)
	})

	t.Run("duplicate", func() {
		l := t.newMap(1)
		for i := range 3 {
			l.Set(i, func(int, bool) (int, error) { return i, nil })
		}
		for i := range l.Len() {
			l.Set(i, func(int, bool) (int, error) { return i, nil })
		}

		t.Equal([]int{0, 1, 2}, l.keys)
	})
}

func (t *testSetOnlyLockedMap) TestGetOrCreate() {
	l := t.newMap(1)
	for i := range 3 {
		l.SetValue(i, i)
	}

	t.Run("get", func() {
		for i := range 3 {
			l.GetOrCreate(
				i,
				func() (int, error) { return i + 1, nil },
				func(j int, created bool) error {
					t.False(created)
					t.Equal(i, j)

					return nil
				},
			)
		}

		t.Equal([]int{0, 1, 2}, l.keys)
	})

	t.Run("create", func() {
		for i := range 3 {
			l.GetOrCreate(
				i+3,
				func() (int, error) { return i + 3, nil },
				func(j int, created bool) error {
					t.True(created)
					t.Equal(i+3, j)

					return nil
				},
			)
		}

		t.Equal([]int{0, 1, 2, 3, 4, 5}, l.keys)
	})
}

func (t *testSetOnlyLockedMap) TestRemoveValues() {
	l := t.newMap(1)
	for i := range 3 {
		l.SetValue(i, i)
	}

	t.Equal(0, l.RemoveValues(0, 1, 2))
	t.Equal([]int{0, 1, 2}, l.keys)
}

func (t *testSetOnlyLockedMap) TestRemove() {
	t.Run("ok", func() {
		l := t.newMap(1)
		for i := range 3 {
			l.SetValue(i, i)
		}

		for i := range l.Len() {
			removed, err := l.Remove(i, func(int, bool) error {
				return nil
			})
			t.ErrorIs(err, ErrSetOnlyLockedMapNotAllowed)
			t.False(removed)
		}
	})

	t.Run("unknown key", func() {
		l := t.newMap(1)

		for i := range 3 {
			removed, err := l.Remove(i, func(int, bool) error {
				return nil
			})
			t.NoError(err)
			t.False(removed)
		}
	})
}

func (t *testSetOnlyLockedMap) TestLock() {
	l := t.newMap(1)
	for i := range 3 {
		l.SetValue(i, i)
	}

	t.Run("get", func() {
		for i := range l.Len() {
			l.Lock(func(
				get func(int) (int, bool),
				_ func(int, int) bool,
				_ func(int) bool,
				_ iter.Seq2[int, int],
			) error {
				v, found := get(i)
				t.True(found)
				t.Equal(i, v)

				return nil
			})
		}

		for i := range l.Len() {
			l.Lock(func(
				get func(int) (int, bool),
				_ func(int, int) bool,
				_ func(int) bool,
				_ iter.Seq2[int, int],
			) error {
				v, found := get(i + 3)
				t.False(found)
				t.Zero(v)

				return nil
			})
		}
	})

	t.Run("set; duplicate", func() {
		for i := range l.Len() {
			l.Lock(func(
				_ func(int) (int, bool),
				set func(int, int) bool,
				_ func(int) bool,
				_ iter.Seq2[int, int],
			) error {
				created := set(i, i+3)
				t.False(created)

				return nil
			})
		}

		t.Equal([]int{0, 1, 2}, l.keys)
	})

	t.Run("set; new", func() {
		l := t.newMap(1)
		for i := range 3 {
			l.SetValue(i, i)
		}

		for i := range l.Len() {
			l.Lock(func(
				_ func(int) (int, bool),
				set func(int, int) bool,
				_ func(int) bool,
				_ iter.Seq2[int, int],
			) error {
				created := set(i+3, i+3)
				t.True(created)

				return nil
			})
		}

		t.Equal([]int{0, 1, 2, 3, 4, 5}, l.keys)
	})

	t.Run("remove", func() {
		for i := range l.Len() {
			l.Lock(func(
				_ func(int) (int, bool),
				_ func(int, int) bool,
				remove func(int) bool,
				_ iter.Seq2[int, int],
			) error {
				removed := remove(i)
				t.False(removed)

				return nil
			})
		}

		t.Equal([]int{0, 1, 2}, l.keys)
	})

	t.Run("remove unknown", func() {
		for i := range l.Len() {
			l.Lock(func(
				_ func(int) (int, bool),
				_ func(int, int) bool,
				remove func(int) bool,
				_ iter.Seq2[int, int],
			) error {
				removed := remove(i + 3)
				t.False(removed)

				return nil
			})
		}

		t.Equal([]int{0, 1, 2}, l.keys)
	})
}

func (t *testSetOnlyLockedMap) TestNSeq2() {
	t.Run("emty map", func() {
		l := t.newMap(1)

		var called bool
		for range l.NSeq2(3) {
			called = true
		}

		t.False(called)
	})

	t.Run("zero n", func() {
		l := t.newMap(1)
		for i := range 3 {
			l.SetValue(i, i)
		}

		var called bool
		for range l.NSeq2(0) {
			called = true
		}

		t.False(called)
	})

	t.Run("n == 1", func() {
		l := t.newMap(1)
		for i := range 3 {
			l.SetValue(i, i)
		}

		var nseq int
		for range l.NSeq2(1) {
			nseq++
		}
		t.Equal(l.Len(), nseq)

		keys := slices.Collect(maps.Keys(l.Map()))
		rkeys := make([]int, 0, l.Len())

		for seq := range l.NSeq2(1) {
			for k, v := range seq {
				j, found := l.Value(k)
				t.True(found)
				t.Equal(j, v)

				rkeys = append(rkeys, k)
			}
		}

		slices.Sort(keys)
		slices.Sort(rkeys)
		t.Equal(keys, rkeys)
	})

	t.Run("n == len", func() {
		l := t.newMap(1)
		for i := range 3 {
			l.SetValue(i, i)
		}

		var nseq int
		for range l.NSeq2(uint64(l.Len())) {
			nseq++
		}
		t.Equal(1, nseq)

		keys := slices.Collect(maps.Keys(l.Map()))
		rkeys := make([]int, 0, l.Len())

		for seq := range l.NSeq2(uint64(l.Len())) {
			for k, v := range seq {
				j, found := l.Value(k)
				t.True(found)
				t.Equal(j, v)

				rkeys = append(rkeys, k)
			}
		}

		slices.Sort(keys)
		slices.Sort(rkeys)
		t.Equal(keys, rkeys)
	})

	t.Run("n > len", func() {
		l := t.newMap(1)
		for i := range 3 {
			l.SetValue(i, i)
		}

		var nseq int
		for range l.NSeq2(uint64(l.Len() + 1)) {
			nseq++
		}
		t.Equal(1, nseq)

		keys := slices.Collect(maps.Keys(l.Map()))
		rkeys := make([]int, 0, l.Len())

		for seq := range l.NSeq2(uint64(l.Len() + 1)) {
			for k, v := range seq {
				j, found := l.Value(k)
				t.True(found)
				t.Equal(j, v)

				rkeys = append(rkeys, k)
			}
		}

		slices.Sort(keys)
		slices.Sort(rkeys)
		t.Equal(keys, rkeys)
	})

	t.Run("value not found", func() {
		l := t.newMap(1)
		for i := range 3 {
			l.SetValue(i, i)
		}

		t.Equal(1, l.LockedMap.RemoveValues(1))
		t.Equal([]int{0, 1, 2}, l.keys)

		keys := slices.Collect(maps.Keys(l.Map()))
		rkeys := make([]int, 0, l.Len())

		for seq := range l.NSeq2(1) {
			for k, v := range seq {
				j, found := l.Value(k)
				t.True(found)
				t.Equal(j, v)

				rkeys = append(rkeys, k)
			}
		}

		slices.Sort(keys)
		slices.Sort(rkeys)
		t.Equal(keys, rkeys)
	})
}

func (t *testSetOnlyLockedMap) TestResetKeys() {
	t.Run("empty", func() {
		l := t.newMap(1)

		l.ResetKeys()
		t.Empty(l.keys)
	})

	t.Run("not empty; exact keys", func() {
		l := t.newMap(1)
		for i := range 3 {
			l.SetValue(i, i)
		}

		l.ResetKeys()

		slices.Sort(l.keys)
		t.Equal([]int{0, 1, 2}, l.keys)
	})

	t.Run("keys missing", func() {
		l := t.newMap(1)
		for i := range 3 {
			l.SetValue(i, i)
		}

		t.Equal(2, l.LockedMap.RemoveValues(1, 0))
		t.Equal([]int{0, 1, 2}, l.keys)

		l.ResetKeys()

		t.Equal([]int{2}, l.keys)
	})

	t.Run("keys added", func() {
		l := t.newMap(1)
		for i := range 3 {
			l.SetValue(i, i)
		}

		l.LockedMap.SetValue(3, 3)
		l.LockedMap.SetValue(4, 4)
		l.LockedMap.SetValue(5, 5)
		t.Equal([]int{0, 1, 2}, l.keys)

		l.ResetKeys()

		slices.Sort(l.keys)
		t.Equal([]int{0, 1, 2, 3, 4, 5}, l.keys)
	})
}

func TestSetOnlyLockedMap(t *testing.T) {
	suite.Run(t, new(testSetOnlyLockedMap))
}
