//go:build redis
// +build redis

package redisstorage

import (
	"context"
	"errors"
	"testing"

	"github.com/go-redis/redis/v8"
	"github.com/spikeekips/mitum/storage"
	"github.com/spikeekips/mitum/util"
	"github.com/stretchr/testify/suite"
)

type testRedisStorage struct {
	suite.Suite
}

func (t *testRedisStorage) TestNew() {
	t.Run("default", func() {
		st, err := NewStorage(context.Background(), &redis.Options{}, "")
		t.NoError(err)
		t.NotNil(st)
	})

	t.Run("unknown redis server", func() {
		st, err := NewStorage(context.Background(), &redis.Options{Addr: util.UUID().String()}, "")
		t.Error(err)
		t.Nil(st)
		t.True(errors.Is(err, storage.ConnectionError))
		t.Contains(err.Error(), "failed to connect to redis server")
	})
}

func (t *testRedisStorage) TestClose() {
	st, err := NewStorage(context.Background(), &redis.Options{}, "test")
	t.NoError(err)
	t.NotNil(st)

	t.Run("close", func() {
		t.NoError(st.Close())
	})

	t.Run("close again", func() {
		err := st.Close()
		t.Error(err)
		t.True(errors.Is(err, storage.InternalError))
		t.Contains(err.Error(), "failed to close redis client")
	})

	t.Run("exec", func() {
		_, found, err := st.Get(context.Background(), util.UUID().String())
		t.Error(err)
		t.False(found)
		t.True(errors.Is(err, storage.ExecError))
		t.Contains(err.Error(), "failed to get")
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

func TestRedisStorage(t *testing.T) {
	suite.Run(t, new(testRedisStorage))
}
