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

func NewLRUGCache[K constraints.Ordered, V any](size int) *GCache[K, V] {
	return &GCache[K, V]{
		c: gcache.New(size).LRU().Build(),
	}
}

func NewLFUGCache[K constraints.Ordered, V any](size int) *GCache[K, V] {
	return &GCache[K, V]{
		c: gcache.New(size).LFU().Build(),
	}
}

func (c *GCache[K, V]) Exists(key K) bool {
	return c.c.Has(key)
}

func (c *GCache[K, V]) Get(key K) (v V, found bool) {
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

func (c *GCache[K, V]) Remove(key K) bool {
	return c.c.Remove(key)
}

func (c *GCache[K, V]) Close() {
	c.c.Purge()
}

func (c *GCache[K, V]) Purge() {
	c.c.Purge()
}

func (c *GCache[K, V]) Traverse(f func(K, V) bool) {
	keys := c.c.Keys(true)

	for i := range keys {
		k := keys[i].(K) //nolint:forcetypeassert //...

		switch j, found := c.Get(k); {
		case !found:
			continue
		case !f(k, j):
			return
		}
	}
}
