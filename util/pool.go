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

func (po *GCacheObjectPool) Close() {
	po.cache.Purge()
}

func (po *GCacheObjectPool) Purge() {
	po.cache.Purge()
}
