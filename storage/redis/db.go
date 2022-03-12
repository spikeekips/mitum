package redisstorage

import (
	"context"
	"sync"

	"github.com/go-redis/redis/v8"
	"github.com/pkg/errors"
	"github.com/spikeekips/mitum/storage"
)

type Storage struct {
	sync.RWMutex
	client *redis.Client
	prefix string
}

func NewStorage(ctx context.Context, opt *redis.Options, prefix string) (*Storage, error) {
	st := &Storage{
		prefix: prefix,
	}

	if err := st.connect(ctx, opt); err != nil {
		return nil, errors.Wrap(err, "")
	}

	return st, nil
}

func (st *Storage) Connect(ctx context.Context) error {
	st.Lock()
	defer st.Unlock()

	return st.connect(ctx, st.client.Options())
}

func (st *Storage) connect(ctx context.Context, opt *redis.Options) error {
	client := redis.NewClient(opt)
	if err := client.Ping(ctx).Err(); err != nil {
		return storage.ConnectionError.Wrapf(err, "failed to connect to redis server")
	}

	st.client = client

	return nil
}

func (st *Storage) Close() error {
	st.Lock()
	defer st.Unlock()

	if err := st.client.Close(); err != nil {
		return storage.InternalError.Wrapf(err, "failed to close redis client")
	}

	return nil
}

func (st *Storage) key(key string) string {
	return st.prefix + "-" + key
}

func (st *Storage) Get(ctx context.Context, key string) ([]byte, bool, error) {
	st.RLock()
	defer st.RUnlock()

	r := st.client.Get(ctx, st.key(key))
	switch {
	case r.Err() == redis.Nil:
		return nil, false, nil
	case r.Err() != nil:
		return nil, false, storage.ExecError.Wrapf(r.Err(), "failed to get from redis storage")
	default:
		return []byte(r.Val()), true, nil
	}
}

func (st *Storage) Set(ctx context.Context, key string, b []byte) error {
	st.RLock()
	defer st.RUnlock()

	r := st.client.Set(ctx, st.key(key), b, 0)
	switch {
	case r.Err() != nil:
		return storage.ExecError.Wrapf(r.Err(), "failed to set from redis storage")
	default:
		return nil
	}
}

func (st *Storage) Exists(ctx context.Context, key string) (bool, error) {
	st.RLock()
	defer st.RUnlock()

	r := st.client.Exists(ctx, st.key(key))
	switch {
	case r.Err() != nil:
		return false, storage.ExecError.Wrapf(r.Err(), "failed exists from redis storage")
	default:
		return r.Val() == 1, nil
	}
}

func (st *Storage) Clean(ctx context.Context) error {
	st.RLock()
	defer st.RUnlock()

	if err := st.client.FlushAll(ctx).Err(); err != nil {
		return storage.ExecError.Wrapf(err, "failed to clean redis server")
	}

	return nil
}
