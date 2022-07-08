package util

import (
	"time"

	"github.com/bluele/gcache"
	"github.com/pkg/errors"
)

type ObjectPool interface {
	Exists(string) bool
	Get(string) (interface{}, bool /* if found, true */)
	Set(string, interface{}, *time.Duration)
}

type GCacheObjectPool struct {
	cache gcache.Cache
}

func NewGCacheObjectPool(size int) *GCacheObjectPool {
	return &GCacheObjectPool{
		cache: gcache.New(size).LRU().Build(),
	}
}

func (po *GCacheObjectPool) Exists(key string) bool {
	return po.cache.Has(key)
}

func (po *GCacheObjectPool) Get(key string) (interface{}, bool) {
	i, err := po.cache.Get(key)

	switch {
	case errors.Is(err, gcache.KeyNotFoundError):
		return nil, false
	case err != nil:
		return nil, false
	}

	return i, true
}

func (po *GCacheObjectPool) Set(key string, v interface{}, expire *time.Duration) {
	if expire != nil {
		_ = po.cache.SetWithExpire(key, v, *expire)

		return
	}

	_ = po.cache.Set(key, v)
}

type LockedObjectPool struct {
	maps *ShardedMap
}

func NewLockedObjectPool() *LockedObjectPool {
	return &LockedObjectPool{
		maps: NewShardedMap(1 << 13), //nolint:gomnd //...
	}
}

func (po *LockedObjectPool) Exists(key string) bool {
	_, found := po.maps.Value(key)

	return found
}

func (po *LockedObjectPool) Get(key string) (interface{}, bool) {
	return po.maps.Value(key)
}

func (po *LockedObjectPool) Set(key string, v interface{}, _ *time.Duration) {
	_ = po.maps.SetValue(key, v)
}
