package util

import (
	"github.com/bluele/gcache"
	"github.com/pkg/errors"
)

type ObjectPool interface {
	Exists(string) bool
	Get(string) (interface{}, bool /* if found, true */)
	Set(string, interface{})
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

func (po *GCacheObjectPool) Set(key string, v interface{}) {
	_ = po.cache.Set(key, v)
}

type LockedObjectPool struct {
	maps *LockedMap
}

func NewLockedObjectPool() *LockedObjectPool {
	return &LockedObjectPool{
		maps: NewLockedMap(),
	}
}

func (po *LockedObjectPool) Exists(key string) bool {
	_, found := po.maps.Value(key)

	return found
}

func (po *LockedObjectPool) Get(key string) (interface{}, bool) {
	return po.maps.Value(key)
}

func (po *LockedObjectPool) Set(key string, v interface{}) {
	_ = po.maps.SetValue(key, v)
}
