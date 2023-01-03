package util

import (
	"time"

	"github.com/bluele/gcache"
	"github.com/pkg/errors"
	"golang.org/x/exp/constraints"
)

type GCache[K constraints.Ordered, V any] struct {
	c gcache.Cache
}

func NewLRUGCache[K constraints.Ordered, V any](_ K, _ V, size int) *GCache[K, V] {
	return &GCache[K, V]{
		c: gcache.New(size).LRU().Build(),
	}
}

func NewLFUGCache[K constraints.Ordered, V any](_ K, _ V, size int) *GCache[K, V] {
	return &GCache[K, V]{
		c: gcache.New(size).LFU().Build(),
	}
}

func (c *GCache[K, V]) Exists(key K) bool {
	return c.c.Has(key)
}

func (c *GCache[K, V]) Get(key string) (v V, found bool) {
	i, err := c.c.Get(key)

	switch {
	case errors.Is(err, gcache.KeyNotFoundError):
		return v, false
	case err != nil:
		return v, false
	}

	return i.(V), true //nolint:forcetypeassert //...
}

func (c *GCache[K, V]) Set(key K, v V, expire time.Duration) {
	if expire > 0 {
		_ = c.c.SetWithExpire(key, v, expire)

		return
	}

	_ = c.c.Set(key, v)
}

func (c *GCache[K, V]) Close() {
	c.c.Purge()
}
