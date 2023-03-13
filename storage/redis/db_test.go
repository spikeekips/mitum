//go:build test && redis
// +build test,redis

package redisstorage

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"testing"

	"github.com/redis/go-redis/v9"
	"github.com/spikeekips/mitum/storage"
	"github.com/spikeekips/mitum/util"
	"github.com/stretchr/testify/suite"
)

type testRedisStorage struct {
	suite.Suite
}

func (t *testRedisStorage) TestNew() {
	t.Run("default", func() {
		st, err := NewStorage(context.Background(), &redis.Options{}, "test")
		t.NoError(err)
		t.NotNil(st)

		defer st.Close()
		defer st.Clean(context.Background())
	})

	t.Run("unknown redis server", func() {
		st, err := NewStorage(context.Background(), &redis.Options{Addr: util.UUID().String()}, "test")
		t.Error(err)
		t.Nil(st)
		t.True(errors.Is(err, storage.ErrConnection))
		t.ErrorContains(err, "failed to connect to redis server")
	})
}

func (t *testRedisStorage) TestClose() {
	st, err := NewStorage(context.Background(), &redis.Options{}, "test")
	t.NoError(err)
	t.NotNil(st)

	defer st.Close()
	defer st.Clean(context.Background())

	t.Run("close", func() {
		t.NoError(st.Close())
	})

	t.Run("close again", func() {
		err := st.Close()
		t.Error(err)
		t.True(errors.Is(err, storage.ErrInternal))
		t.ErrorContains(err, "failed to close redis client")
	})

	t.Run("exec", func() {
		_, found, err := st.Get(context.Background(), util.UUID().String())
		t.Error(err)
		t.False(found)
		t.True(errors.Is(err, storage.ErrExec))
		t.ErrorContains(err, "failed to get")
	})

	t.Run("connect", func() {
		err := st.Connect(context.Background())
		t.NoError(err)
	})

	t.Run("exec again", func() {
		_, found, err := st.Get(context.Background(), util.UUID().String())
		t.NoError(err)
		t.False(found)

		found, err = st.Exists(context.Background(), util.UUID().String())
		t.NoError(err)
		t.False(found)
	})
}

func (t *testRedisStorage) TestGetSet() {
	st, err := NewStorage(context.Background(), &redis.Options{}, "test")
	t.NoError(err)
	t.NotNil(st)

	defer st.Close()
	defer st.Clean(context.Background())

	key := util.UUID().String()
	value := util.UUID().Bytes()

	t.Run("set", func() {
		t.NoError(st.Set(context.Background(), key, value))
	})

	t.Run("get", func() {
		rvalue, found, err := st.Get(context.Background(), key)
		t.NoError(err)
		t.True(found)
		t.Equal(value, rvalue)
	})

	t.Run("exists", func() {
		found, err := st.Exists(context.Background(), key)
		t.NoError(err)
		t.True(found)
	})

	t.Run("unknown key", func() {
		found, err := st.Exists(context.Background(), util.UUID().String())
		t.NoError(err)
		t.False(found)
	})
}

func (t *testRedisStorage) TestZAddArgs() {
	st, err := NewStorage(context.Background(), &redis.Options{}, "test")
	t.NoError(err)
	t.NotNil(st)

	defer st.Close()
	defer st.Clean(context.Background())

	t.Run("integer score", func() {
		key := util.UUID().String()

		members := make([]string, 6)
		args := redis.ZAddArgs{NX: true, Members: make([]redis.Z, 6)}
		for i := range members {
			member := fmt.Sprintf("%s-%d", key, i)
			args.Members[i] = redis.Z{
				Score:  float64(i),
				Member: member,
			}

			members[i] = member
		}

		t.NoError(st.ZAddArgs(context.Background(), key, args))

		var r []string
		err := st.ZRangeArgs(context.Background(), redis.ZRangeArgs{
			Key:     key,
			Start:   "0",
			Stop:    "5",
			ByScore: true,
			Rev:     false,
		}, func(i string) (bool, error) {
			r = append(r, i)

			return true, nil
		})
		t.NoError(err)
		t.Equal(members, r)

		// reverse
		r = nil
		err = st.ZRangeArgs(context.Background(), redis.ZRangeArgs{
			Key:     key,
			Start:   "0",
			Stop:    "5",
			ByScore: true,
			Rev:     true,
		}, func(i string) (bool, error) {
			r = append(r, i)

			return true, nil
		})
		t.NoError(err)

		sort.Sort(sort.Reverse(sort.StringSlice(members)))
		t.Equal(members, r)
	})

	t.Run("filter by score", func() {
		key := util.UUID().String()

		members := make([]string, 6)
		args := redis.ZAddArgs{NX: true, Members: make([]redis.Z, 6)}
		for i := range members {
			member := fmt.Sprintf("%s-%d", key, i)
			args.Members[i] = redis.Z{
				Score:  float64(i),
				Member: member,
			}

			members[i] = member
		}

		t.NoError(st.ZAddArgs(context.Background(), key, args))

		var r []string
		err := st.ZRangeArgs(context.Background(), redis.ZRangeArgs{
			Key:     key,
			Start:   "3",
			Stop:    "5",
			ByScore: true,
			Rev:     false,
		}, func(i string) (bool, error) {
			r = append(r, i)

			return true, nil
		})
		t.NoError(err)
		t.Equal(members[3:], r)

		// reverse
		r = nil
		err = st.ZRangeArgs(context.Background(), redis.ZRangeArgs{
			Key:     key,
			Start:   "3",
			Stop:    "5",
			ByScore: true,
			Rev:     true,
		}, func(i string) (bool, error) {
			r = append(r, i)

			return true, nil
		})
		t.NoError(err)

		sort.Sort(sort.Reverse(sort.StringSlice(members)))
		t.Equal(members[:3], r)
	})

	t.Run("count", func() {
		key := util.UUID().String()

		members := make([]string, 6)
		args := redis.ZAddArgs{NX: true, Members: make([]redis.Z, 6)}
		for i := range members {
			member := fmt.Sprintf("%s-%d", key, i)
			args.Members[i] = redis.Z{
				Score:  float64(i),
				Member: member,
			}

			members[i] = member
		}

		t.NoError(st.ZAddArgs(context.Background(), key, args))

		var r []string
		err := st.ZRangeArgs(context.Background(), redis.ZRangeArgs{
			Key:     key,
			Start:   "0",
			Stop:    "5",
			ByScore: true,
			Rev:     false,
			Count:   1,
		}, func(i string) (bool, error) {
			r = append(r, i)

			return true, nil
		})
		t.NoError(err)
		t.Equal(members[:1], r)

		r = nil
		err = st.ZRangeArgs(context.Background(), redis.ZRangeArgs{
			Key:     key,
			Start:   "0",
			Stop:    "5",
			ByScore: true,
			Rev:     false,
			Count:   2,
		}, func(i string) (bool, error) {
			r = append(r, i)

			return true, nil
		})
		t.NoError(err)
		t.Equal(members[:2], r)

		r = nil
		err = st.ZRangeArgs(context.Background(), redis.ZRangeArgs{
			Key:     key,
			Start:   "0",
			Stop:    "5",
			ByScore: true,
			Rev:     false,
			Count:   int64(len(members)) + 100,
		}, func(i string) (bool, error) {
			r = append(r, i)

			return true, nil
		})
		t.NoError(err)
		t.Equal(members, r)
	})

	t.Run("by lexical sort", func() {
		key := util.UUID().String()

		members := make([]string, 6)
		args := redis.ZAddArgs{NX: true, Members: make([]redis.Z, 6)}
		for i := range members {
			member := fmt.Sprintf("%s-%021d", key, i)
			args.Members[i] = redis.Z{
				Score:  0, // must be same score
				Member: member,
			}

			members[i] = member
		}

		t.NoError(st.ZAddArgs(context.Background(), key, args))

		var r []string
		err := st.ZRangeArgs(context.Background(), redis.ZRangeArgs{
			Key:   key,
			Start: fmt.Sprintf("[%s-%021d", key, 0),
			Stop:  fmt.Sprintf("[%s-%021d", key, 5),
			ByLex: true,
			Rev:   false,
		}, func(i string) (bool, error) {
			r = append(r, i)

			return true, nil
		})
		t.NoError(err)
		t.Equal(members, r)

		// set Start
		r = nil
		err = st.ZRangeArgs(context.Background(), redis.ZRangeArgs{
			Key:   key,
			Start: fmt.Sprintf("(%s-%021d", key, 3),
			Stop:  fmt.Sprintf("[%s-%021d", key, 5),
			ByLex: true,
			Rev:   false,
		}, func(i string) (bool, error) {
			r = append(r, i)

			return true, nil
		})
		t.NoError(err)
		t.Equal(members[4:], r)

		// count over items
		r = nil
		err = st.ZRangeArgs(context.Background(), redis.ZRangeArgs{
			Key:   key,
			Start: fmt.Sprintf("[%s-%021d", key, 0),
			Stop:  fmt.Sprintf("[%s-%021d", key, 5),
			ByLex: true,
			Rev:   false,
			Count: int64(len(members)) + 100,
		}, func(i string) (bool, error) {
			r = append(r, i)

			return true, nil
		})
		t.NoError(err)
		t.Equal(members, r)

		// reverse
		r = nil
		err = st.ZRangeArgs(context.Background(), redis.ZRangeArgs{
			Key:   key,
			Start: fmt.Sprintf("[%s-%021d", key, 0),
			Stop:  fmt.Sprintf("[%s-%021d", key, 5),
			ByLex: true,
			Rev:   true,
		}, func(i string) (bool, error) {
			r = append(r, i)

			return true, nil
		})
		t.NoError(err)

		sort.Sort(sort.Reverse(sort.StringSlice(members)))
		t.Equal(members, r)
	})
}

func TestRedisStorage(t *testing.T) {
	suite.Run(t, new(testRedisStorage))
}
