package util

import (
	"math"
	"sync"
	"sync/atomic"

	"github.com/pkg/errors"
	"golang.org/x/exp/constraints"
)

var (
	ErrLockedSetIgnore = NewError("ignore to set locked value")
	ErrLockedMapClosed = NewError("locked map closed")
)

type Locked[T any] struct {
	value T
	sync.RWMutex
	isempty bool
}

func EmptyLocked[T any](T) *Locked[T] {
	return &Locked[T]{isempty: true}
}

func NewLocked[T any](v T) *Locked[T] {
	return &Locked[T]{value: v}
}

func (l *Locked[T]) Value() (v T, isempty bool) {
	l.RLock()
	defer l.RUnlock()

	if l.isempty {
		return v, true
	}

	return l.value, false
}

func (l *Locked[T]) SetValue(v T) *Locked[T] {
	l.Lock()
	defer l.Unlock()

	l.value = v
	l.isempty = false

	return l
}

func (l *Locked[T]) EmptyValue() *Locked[T] {
	l.Lock()
	defer l.Unlock()

	var v T

	l.value = v
	l.isempty = true

	return l
}

func (l *Locked[T]) Get(create func() (T, error)) (v T, _ error) {
	l.Lock()
	defer l.Unlock()

	if !l.isempty {
		return l.value, nil
	}

	switch i, err := create(); {
	case err != nil:
		return v, err
	default:
		l.value = i
		l.isempty = false

		return i, nil
	}
}

func (l *Locked[T]) Set(f func(_ T, isempty bool) (T, error)) (v T, _ error) {
	l.Lock()
	defer l.Unlock()

	switch i, err := f(l.value, l.isempty); {
	case err == nil:
		l.value = i
		l.isempty = false

		return i, nil
	case errors.Is(err, ErrLockedSetIgnore):
		return l.value, nil
	default:
		return v, err
	}
}

func (l *Locked[T]) Empty(f func(_ T, isempty bool) error) error {
	l.Lock()
	defer l.Unlock()

	switch err := f(l.value, l.isempty); {
	case err == nil:
		var v T

		l.value = v
		l.isempty = true

		return nil
	case errors.Is(err, ErrLockedSetIgnore):
		return nil
	default:
		return err
	}
}

type lockedMapKeys interface {
	constraints.Ordered
}

type LockedMap[K lockedMapKeys, V any] interface {
	Exists(key K) (found bool)
	Value(key K) (value V, found bool)
	SetValue(key K, value V) (found bool)
	RemoveValue(key K) (removed bool)
	Get(key K, create func() (value V, _ error)) (value V, found bool, _ error)
	Set(key K, f func(_ V, found bool) (value V, _ error)) (value V, _ error)
	Remove(key K, f func(value V) error) (removed bool, _ error)
	Traverse(f func(key K, value V) (keep bool))
	Len() int
	Empty()
	Close()
}

func NewLockedMap[K lockedMapKeys, V any](k K, v V, size int64) (LockedMap[K, V], error) {
	switch {
	case size < 1:
		return nil, errors.Errorf("failed new ShardedMap; empty size")
	case size == 1:
		return NewSingleLockedMap(k, v), nil
	default:
		return NewShardedMap(k, v, size)
	}
}

type SingleLockedMap[K lockedMapKeys, V any] struct {
	m map[K]V
	sync.RWMutex
}

func NewSingleLockedMap[K lockedMapKeys, V any](K, V) *SingleLockedMap[K, V] {
	return &SingleLockedMap[K, V]{
		m: map[K]V{},
	}
}

func (l *SingleLockedMap[K, V]) Exists(k K) bool {
	l.RLock()
	defer l.RUnlock()

	_, found := l.m[k]

	return found
}

func (l *SingleLockedMap[K, V]) Value(k K) (v V, found bool) {
	l.RLock()
	defer l.RUnlock()

	if l.m == nil {
		return v, false
	}

	v, found = l.m[k]

	return v, found
}

func (l *SingleLockedMap[K, V]) SetValue(k K, v V) (found bool) {
	l.Lock()
	defer l.Unlock()

	if l.m == nil {
		return false
	}

	_, found = l.m[k]

	l.m[k] = v

	return found
}

func (l *SingleLockedMap[K, V]) RemoveValue(k K) bool {
	l.Lock()
	defer l.Unlock()

	if l.m == nil {
		return false
	}

	_, found := l.m[k]
	if found {
		delete(l.m, k)
	}

	return found
}

func (l *SingleLockedMap[K, V]) Get(k K, create func() (V, error)) (v V, found bool, err error) {
	v, found, _, err = l.get(k, create)

	return v, found, err
}

func (l *SingleLockedMap[K, V]) get(k K, create func() (V, error)) (v V, found, created bool, _ error) {
	l.Lock()
	defer l.Unlock()

	if l.m == nil {
		return v, false, false, ErrLockedMapClosed.Call()
	}

	if i, found := l.m[k]; found {
		return i, true, false, nil
	}

	switch i, err := create(); {
	case err == nil:
		l.m[k] = i

		return i, false, true, nil
	case errors.Is(err, ErrLockedSetIgnore):
		return v, false, false, nil
	default:
		return v, false, false, err
	}
}

func (l *SingleLockedMap[K, V]) Set(k K, f func(_ V, found bool) (V, error)) (V, error) {
	v, _, err := l.set(k, f)

	return v, err
}

func (l *SingleLockedMap[K, V]) set(k K, f func(_ V, found bool) (V, error)) (v V, created bool, _ error) {
	l.Lock()
	defer l.Unlock()

	if l.m == nil {
		return v, false, ErrLockedMapClosed.Call()
	}

	i, found := l.m[k]

	switch j, err := f(i, found); {
	case err == nil:
		l.m[k] = j

		return j, !found, nil
	case errors.Is(err, ErrLockedSetIgnore):
		return i, false, nil // NOTE existing value
	default:
		return v, false, err
	}
}

func (l *SingleLockedMap[K, V]) Remove(k K, f func(V) error) (bool, error) {
	l.Lock()
	defer l.Unlock()

	i, found := l.m[k]
	if !found {
		return false, nil
	}

	if f != nil {
		switch err := f(i); {
		case err == nil:
		case errors.Is(err, ErrLockedSetIgnore):
			return false, nil
		default:
			return false, err
		}
	}

	delete(l.m, k)

	return true, nil
}

func (l *SingleLockedMap[K, V]) Traverse(f func(K, V) bool) {
	l.traverse(f)
}

func (l *SingleLockedMap[K, V]) traverse(f func(K, V) bool) bool {
	l.RLock()
	defer l.RUnlock()

	for k := range l.m {
		if !f(k, l.m[k]) {
			return false
		}
	}

	return true
}

func (l *SingleLockedMap[K, V]) Len() int {
	l.RLock()
	defer l.RUnlock()

	return len(l.m)
}

func (l *SingleLockedMap[K, V]) Empty() {
	l.Lock()
	defer l.Unlock()

	l.m = map[K]V{}
}

func (l *SingleLockedMap[K, V]) Close() {
	l.Lock()
	defer l.Unlock()

	l.m = nil
}

const (
	shardedprime = uint32(16_777_619)
	shardedseed  = uint32(2_166_136_261)
)

type ShardedMap[K lockedMapKeys, V any] struct {
	sharded []*SingleLockedMap[K, V]
	length  int64
	sync.RWMutex
}

func NewShardedMap[K lockedMapKeys, V any](_ K, _ V, size int64) (*ShardedMap[K, V], error) {
	switch {
	case size < 1:
		return nil, errors.Errorf("failed new ShardedMap; empty size")
	case size == 1:
		return nil, errors.Errorf("failed new ShardedMap; 1 size; use SingleLockedMap")
	}

	return &ShardedMap[K, V]{
		sharded: make([]*SingleLockedMap[K, V], size),
	}, nil
}

func (l *ShardedMap[K, V]) Exists(k K) bool {
	switch i, isclosed := l.item(k); {
	case isclosed:
		return false
	default:
		return i.Exists(k)
	}
}

func (l *ShardedMap[K, V]) Value(k K) (v V, found bool) {
	switch i, isclosed := l.item(k); {
	case isclosed:
		return v, false
	default:
		return i.Value(k)
	}
}

func (l *ShardedMap[K, V]) SetValue(k K, v V) (found bool) {
	switch i, isclosed := l.item(k); {
	case isclosed:
		return false
	default:
		found = i.SetValue(k, v)
		if !found {
			atomic.AddInt64(&l.length, 1)
		}

		return found
	}
}

func (l *ShardedMap[K, V]) RemoveValue(k K) bool {
	switch i, isclosed := l.item(k); {
	case isclosed:
		return false
	default:
		removed := i.RemoveValue(k)
		if removed {
			atomic.AddInt64(&l.length, -1)
		}

		return removed
	}
}

func (l *ShardedMap[K, V]) Get(k K, create func() (V, error)) (v V, found bool, _ error) {
	switch i, isclosed := l.item(k); {
	case isclosed:
		return v, false, ErrLockedMapClosed.Call()
	default:
		v, found, created, err := i.get(k, create)
		if err == nil && created {
			atomic.AddInt64(&l.length, 1)
		}

		return v, found, err
	}
}

func (l *ShardedMap[K, V]) Set(k K, f func(V, bool) (V, error)) (v V, _ error) {
	switch i, isclosed := l.item(k); {
	case isclosed:
		return v, ErrLockedMapClosed.Call()
	default:
		v, created, err := i.set(k, f)
		if err == nil && created {
			atomic.AddInt64(&l.length, 1)
		}

		return v, err
	}
}

func (l *ShardedMap[K, V]) Remove(k K, f func(V) error) (bool, error) {
	switch i, isclosed := l.item(k); {
	case isclosed:
		return false, ErrLockedMapClosed.Call()
	default:
		removed, err := i.Remove(k, f)
		if err == nil && removed {
			atomic.AddInt64(&l.length, -1)
		}

		return removed, err
	}
}

func (l *ShardedMap[K, V]) Traverse(f func(K, V) bool) {
	var last int

	next := func() *SingleLockedMap[K, V] {
		l.RLock()
		defer l.RUnlock()

		for {
			switch {
			case len(l.sharded) < 1:
				return nil
			case len(l.sharded) == last:
				return nil
			}

			item := l.sharded[last]

			last++

			if item != nil {
				return item
			}
		}
	}

	for {
		item := next()
		if item == nil {
			break
		}

		if !item.traverse(f) {
			break
		}
	}
}

func (l *ShardedMap[K, V]) Len() int {
	return int(atomic.LoadInt64(&l.length))
}

func (l *ShardedMap[K, V]) Close() {
	l.Lock()
	defer l.Unlock()

	for i := range l.sharded {
		if l.sharded[i] == nil {
			continue
		}

		l.sharded[i].Close()
	}

	l.sharded = nil
	atomic.StoreInt64(&l.length, 0)
}

func (l *ShardedMap[K, V]) Empty() {
	for i := range l.sharded {
		if l.sharded[i] == nil {
			continue
		}

		l.sharded[i].Empty()
	}

	atomic.StoreInt64(&l.length, 0)
}

func (l *ShardedMap[K, V]) item(k K) (_ *SingleLockedMap[K, V], isclosed bool) {
	l.Lock()
	defer l.Unlock()

	if len(l.sharded) < 1 {
		return nil, true
	}

	i := l.fnv(k)

	item := l.sharded[i]
	if item == nil {
		var v V

		item = NewSingleLockedMap(k, v)

		l.sharded[i] = item
	}

	return item, false
}

func (l *ShardedMap[K, V]) fnv(k K) uint64 {
	var i uint64

	switch t := (interface{})(k).(type) {
	case string:
		i = l.stringfnv(t)
	default:
		i = l.intfnv(k)
	}

	return i % uint64(len(l.sharded))
}

func (*ShardedMap[K, V]) intfnv(k K) uint64 {
	var i uint64

	switch t := (interface{})(k).(type) {
	case int:
		i = uint64(math.Abs(float64(t)))
	case int8:
		i = uint64(math.Abs(float64(t)))
	case int16:
		i = uint64(math.Abs(float64(t)))
	case int32:
		i = uint64(math.Abs(float64(t)))
	case int64:
		i = uint64(math.Abs(float64(t)))
	case uint:
		i = uint64(t)
	case uint8:
		i = uint64(t)
	case uint16:
		i = uint64(t)
	case uint32:
		i = uint64(t)
	case uint64:
		i = t
	}

	return i
}

func (*ShardedMap[K, V]) stringfnv(k string) uint64 {
	h := shardedseed

	kl := len(k)
	for i := 0; i < kl; i++ {
		h *= shardedprime
		h ^= uint32(k[i])
	}

	return uint64(h)
}
