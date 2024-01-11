package util

import (
	"cmp"
	"time"

	"github.com/bluele/gcache"
	"github.com/pkg/errors"
)

type GCache[K cmp.Ordered, V any] interface {
	Exists(K) bool
	Get(K) (_ V, found bool)
	Set(_ K, _ V, expire time.Duration)
	Remove(K) bool
	Close()
	Purge()
	Traverse(func(K, V) bool)
}

type BaseGCache[K cmp.Ordered, V any] struct {
	c gcache.Cache
}

func NewLRUGCache[K cmp.Ordered, V any](size int) *BaseGCache[K, V] {
	return &BaseGCache[K, V]{
		c: gcache.New(size).LRU().Build(),
	}
}

func NewLFUGCache[K cmp.Ordered, V any](size int) *BaseGCache[K, V] {
	return &BaseGCache[K, V]{
		c: gcache.New(size).LFU().Build(),
	}
}

func (c *BaseGCache[K, V]) Exists(key K) bool {
	return c.c.Has(key)
}

func (c *BaseGCache[K, V]) Get(key K) (v V, found bool) {
	i, err := c.c.Get(key)

	switch {
	case errors.Is(err, gcache.KeyNotFoundError):
		return v, false
	case err != nil:
		return v, false
	}

	return i.(V), true //nolint:forcetypeassert //...
}

func (c *BaseGCache[K, V]) Set(key K, v V, expire time.Duration) {
	if expire > 0 {
		_ = c.c.SetWithExpire(key, v, expire)

		return
	}

	_ = c.c.Set(key, v)
}

func (c *BaseGCache[K, V]) Remove(key K) bool {
	return c.c.Remove(key)
}

func (c *BaseGCache[K, V]) Close() {
	c.c.Purge()
}

func (c *BaseGCache[K, V]) Purge() {
	c.c.Purge()
}

func (c *BaseGCache[K, V]) Traverse(f func(K, V) bool) {
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

type PurgeFuncGCache[K cmp.Ordered, V any] struct {
	GCache[K, V]
	purgef func() bool
}

func NewPurgeFuncGCache[K cmp.Ordered, V any](
	b GCache[K, V],
	purgef func() bool,
) *PurgeFuncGCache[K, V] {
	return &PurgeFuncGCache[K, V]{
		GCache: b,
		purgef: purgef,
	}
}

func (c *PurgeFuncGCache[K, V]) Purge() {
	if !c.purgef() {
		return
	}

	c.GCache.Purge()
}
