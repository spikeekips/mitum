package util

import "time"

type EmptyGCache[K, V any] struct{}

func NewEmptyGCache[K, V any]() EmptyGCache[K, V] { return EmptyGCache[K, V]{} }

func (EmptyGCache[K, V]) Exists(K) bool            { return false }
func (EmptyGCache[K, V]) Get(K) (v V, _ bool)      { return v, false }
func (EmptyGCache[K, V]) Set(K, V, time.Duration)  {}
func (EmptyGCache[K, V]) Remove(K) bool            { return false }
func (EmptyGCache[K, V]) Close()                   {}
func (EmptyGCache[K, V]) Purge()                   {}
func (EmptyGCache[K, V]) Traverse(func(K, V) bool) {}
