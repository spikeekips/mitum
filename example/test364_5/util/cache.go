package util

import (
	"cmp"
	"context"
	"sync"
	"time"

	"github.com/spikeekips/mitum/util"
)

type EmptyGCache[K, V any] struct{}

func NewEmptyGCache[K, V any]() EmptyGCache[K, V] { return EmptyGCache[K, V]{} }

func (EmptyGCache[K, V]) Exists(K) bool            { return false }
func (EmptyGCache[K, V]) Get(K) (v V, _ bool)      { return v, false }
func (EmptyGCache[K, V]) Set(K, V, time.Duration)  {}
func (EmptyGCache[K, V]) Remove(K) bool            { return false }
func (EmptyGCache[K, V]) Close()                   {}
func (EmptyGCache[K, V]) Purge()                   {}
func (EmptyGCache[K, V]) Traverse(func(K, V) bool) {}

func NewLazyCache[K comparable, V any, O cmp.Ordered](
	ctx context.Context,
	cacheSize int,
	bufSize, outBufSize uint64,
	order func(V) O,
) (
	func(K) (V, bool),
	func(K, V, time.Duration),
	func(K, V, time.Duration),
	func(),
) {
	cache := util.NewLRUGCache[K, [2]any /* {V, <O: set time> } */](cacheSize)

	add, addDone, seq := NewContextUnlimitedChans[[4]any /* {K, V, <expire>, <O:set time>} */](
		ctx, bufSize, outBufSize)

	donech := make(chan struct{})

	var lock sync.RWMutex

	seqGetSet := func(k K, v V, o O, d time.Duration) {
		lock.Lock()
		defer lock.Unlock()

		switch i, found := cache.Get(k); {
		case !found:
		case o < i[1].(O):
			return
		}

		cache.Set(k, [2]any{v, o}, d)
	}

	go func() {
		for i := range seq {
			var v V
			if i[1] != nil {
				v = i[1].(V)
			}

			seqGetSet(i[0].(K), v, i[3].(O), i[2].(time.Duration))
		}

		close(donech)
	}()

	return func(k K) (v V, _ bool) {
			switch i, found := cache.Get(k); {
			case !found:
				return v, false
			default:
				if i[0] != nil {
					v = i[0].(V)
				}

				return v, true
			}
		},
		func(k K, v V, d time.Duration) {
			lock.Lock()
			defer lock.Unlock()

			cache.Set(k, [2]any{v, order(v)}, d)
		},
		func(k K, v V, d time.Duration) {
			add([4]any{k, v, d, order(v)})
		},
		func() {
			addDone()

			<-donech
		}
}

func NewLastFirstLazyCache[K comparable, V any](
	ctx context.Context,
	cacheSize int,
	bufSize, outBufSize uint64,
) (
	func(K) (V, bool),
	func(K, V, time.Duration),
	func(K, V, time.Duration),
	func(),
) {
	return NewLazyCache[K, V, int64](ctx, cacheSize, bufSize, outBufSize, func(V) int64 {
		return time.Now().UnixNano()
	})
}
