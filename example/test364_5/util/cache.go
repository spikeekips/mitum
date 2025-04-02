package util

import (
	"context"
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

func NewLazyCache[K comparable, V any](
	ctx context.Context,
	cacheSize int,
	bufSize, outBufSize uint64,
) (
	func(K) (V, bool),
	func(K, V, time.Duration),
	func(K, V, time.Duration),
	func(),
) {
	cache := util.NewLRUGCache[K, [2]any /* {V, <set time> } */](cacheSize)

	add, addDone, seq := NewContextUnlimitedChans[[4]any /* {K, V, <expire>, <set time>} */](
		ctx, bufSize, outBufSize)

	donech := make(chan struct{})

	go func() {
	end:
		for i := range seq {
			switch v, found := cache.Get(i[0].(K)); {
			case !found:
			case i[3].(int64) < v[1].(int64):
				continue end
			}

			var v V
			if i[1] != nil {
				v = i[1].(V)
			}

			cache.Set(i[0].(K), [2]any{v, i[3]}, i[2].(time.Duration))
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
			cache.Set(k, [2]any{v, time.Now().UnixNano()}, d)
		},
		func(k K, v V, d time.Duration) {
			add([4]any{k, v, d, time.Now().UnixNano()})
		},
		func() {
			addDone()

			<-donech
		}
}
