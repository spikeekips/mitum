package redisstorage

import (
	"context"
	"sync"

	"github.com/pkg/errors"
	"github.com/redis/go-redis/v9"
	"github.com/spikeekips/mitum/storage"
	"github.com/spikeekips/mitum/util"
)

type Storage struct {
	client *redis.Client
	prefix string
	sync.RWMutex
}

func NewStorage(ctx context.Context, opt *redis.Options, prefix string) (*Storage, error) {
	st := &Storage{
		prefix: prefix,
	}

	if err := st.connect(ctx, opt); err != nil {
		return nil, err
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
		return storage.ErrConnection.Wrapf(err, "failed to connect to redis server")
	}

	st.client = client

	return nil
}

func (st *Storage) Close() error {
	st.Lock()
	defer st.Unlock()

	if err := st.client.Close(); err != nil {
		return storage.ErrInternal.Wrapf(err, "failed to close redis client")
	}

	return nil
}

func (st *Storage) key(key string) string {
	return st.prefix + "-" + key
}

func (st *Storage) unkey(s string) string {
	i := st.prefix + "-"
	if len(s) < len(i)+1 {
		return s
	}

	return s[len(i):]
}

func (st *Storage) Get(ctx context.Context, key string) (b []byte, found bool, _ error) {
	r := st.client.Get(ctx, st.key(key))

	switch {
	case r.Err() == nil:
		return []byte(r.Val()), true, nil
	case errors.Is(r.Err(), redis.Nil):
		return nil, false, nil
	default:
		return nil, false, storage.ErrExec.Wrapf(r.Err(), "failed to get from redis storage")
	}
}

func (st *Storage) Set(ctx context.Context, key string, b []byte) error {
	r := st.client.Set(ctx, st.key(key), b, 0)

	switch {
	case r.Err() != nil:
		return storage.ErrExec.Wrap(errors.Wrap(r.Err(), "failed to set from redis storage"))
	default:
		return nil
	}
}

func (st *Storage) Exists(ctx context.Context, key string) (bool, error) {
	r := st.client.Exists(ctx, st.key(key))

	switch {
	case r.Err() != nil:
		return false, storage.ErrExec.Wrap(errors.Wrap(r.Err(), "failed exists from redis storage"))
	default:
		return r.Val() == 1, nil
	}
}

func (st *Storage) Clean(ctx context.Context) error {
	e := util.StringErrorFunc("failed to clean redis storage")

	var cursor uint64

	for {
		keys, c, err := st.client.Scan(ctx, cursor, st.prefix+"*", 333).Result() //nolint:gomnd // bulk size
		if err != nil {
			return e(errors.WithStack(err), "")
		}

		cursor = c

		if len(keys) > 0 {
			if _, err := st.client.Del(ctx, keys...).Result(); err != nil {
				return e(errors.WithStack(err), "")
			}
		}

		if cursor == 0 {
			break
		}
	}

	return nil
}

func (st *Storage) ZAddArgs(ctx context.Context, key string, args redis.ZAddArgs) error {
	for i := range args.Members {
		z := args.Members[i]
		z.Member = st.key(z.Member.(string)) //nolint:forcetypeassert //...
		args.Members[i] = z
	}

	if err := st.client.ZAddArgs(ctx, st.key(key), args).Err(); err != nil {
		return storage.ErrExec.Wrap(errors.Wrap(err, "failed to ZAddArgs"))
	}

	return nil
}

func (st *Storage) ZRangeArgs(ctx context.Context, z redis.ZRangeArgs, f func(string) (bool, error)) error {
	z.Key = st.key(z.Key)

	if z.ByLex {
		if z.Start != nil {
			zstart := z.Start.(string) //nolint:forcetypeassert //...
			z.Start = zstart[:1] + st.key(zstart[1:])
		}

		if z.Stop != nil {
			zstop := z.Stop.(string) //nolint:forcetypeassert //...
			z.Stop = zstop[:1] + st.key(zstop[1:])
		}
	}

	sl, err := st.client.ZRangeArgs(ctx, z).Result()
	if err != nil {
		return storage.ErrExec.Wrap(errors.Wrap(err, "failed to ZRangeArgs"))
	}

	for i := range sl {
		switch keep, err := f(st.unkey(sl[i])); {
		case err != nil:
			return err
		case !keep:
			return nil
		}
	}

	return nil
}
