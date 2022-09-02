package util

import (
	"sync"
	"sync/atomic"

	"github.com/pkg/errors"
)

var ErrLockedSetIgnore = NewError("ignore to set locked value")

type NilLockedValue struct{}

type Locked struct {
	value interface{}
	sync.RWMutex
}

func EmptyLocked() *Locked {
	return &Locked{value: NilLockedValue{}}
}

func NewLocked(i interface{}) *Locked {
	return &Locked{value: i}
}

func (l *Locked) Value() (v interface{}, isnil bool) {
	l.RLock()
	defer l.RUnlock()

	if isNilLockedValue(l.value) {
		return nil, true
	}

	return l.value, false
}

func (l *Locked) SetValue(i interface{}) *Locked {
	l.Lock()
	defer l.Unlock()

	l.value = i

	return l
}

func (l *Locked) Get(create func() (interface{}, error)) (interface{}, error) {
	l.Lock()
	defer l.Unlock()

	if !isNilLockedValue(l.value) {
		return l.value, nil
	}

	switch i, err := create(); {
	case err != nil:
		return nil, err
	default:
		l.value = i

		return i, nil
	}
}

func (l *Locked) Empty() *Locked {
	l.Lock()
	defer l.Unlock()

	l.value = NilLockedValue{}

	return l
}

func (l *Locked) Set(f func(bool, interface{}) (interface{}, error)) (interface{}, error) {
	l.Lock()
	defer l.Unlock()

	i := l.value

	isnil := isNilLockedValue(l.value)
	if isnil {
		i = nil
	}

	switch j, err := f(isnil, i); {
	case err == nil:
		l.value = j

		return j, nil
	case errors.Is(err, ErrLockedSetIgnore):
		return i, nil
	default:
		return j, err
	}
}

type LockedMap struct {
	m map[interface{}]interface{}
	sync.RWMutex
}

func NewLockedMap() *LockedMap {
	return &LockedMap{
		m: map[interface{}]interface{}{},
	}
}

func (l *LockedMap) Close() {
	l.Lock()
	defer l.Unlock()

	l.m = nil
}

func (l *LockedMap) Exists(k interface{}) bool {
	l.RLock()
	defer l.RUnlock()

	_, found := l.m[k]

	return found
}

func (l *LockedMap) Value(k interface{}) (interface{}, bool) {
	l.RLock()
	defer l.RUnlock()

	switch i, found := l.m[k]; {
	case !found:
		return nil, false
	default:
		return i, true
	}
}

func (l *LockedMap) SetValue(k, v interface{}) bool {
	l.Lock()
	defer l.Unlock()

	_, found := l.m[k]

	l.m[k] = v

	return found
}

func (l *LockedMap) Get(k interface{}, create func() (interface{}, error)) (v interface{}, found bool, _ error) {
	l.Lock()
	defer l.Unlock()

	if i, found := l.m[k]; found {
		return i, true, nil
	}

	switch i, err := create(); {
	case err != nil:
		return nil, false, err
	default:
		l.m[k] = i
		return i, false, nil
	}
}

func (l *LockedMap) Set(k interface{}, f func(bool, interface{}) (interface{}, error)) (interface{}, error) {
	l.Lock()
	defer l.Unlock()

	i, found := l.m[k]

	switch j, err := f(found, i); {
	case err == nil:
		l.m[k] = j

		return j, nil
	case errors.Is(err, ErrLockedSetIgnore):
		return i, nil
	default:
		return j, err
	}
}

func (l *LockedMap) RemoveValue(k interface{}) bool {
	l.Lock()
	defer l.Unlock()

	_, found := l.m[k]
	if found {
		delete(l.m, k)
	}

	return found
}

func (l *LockedMap) Remove(k interface{}, f func(interface{}) error) (bool, error) {
	l.Lock()
	defer l.Unlock()

	i, found := l.m[k]
	if !found {
		return false, nil
	}

	if f != nil {
		if err := f(i); err != nil {
			return true, err
		}
	}

	delete(l.m, k)

	return true, nil
}

func (l *LockedMap) Traverse(f func(interface{}, interface{}) bool) {
	l.RLock()
	defer l.RUnlock()

	for k := range l.m {
		if !f(k, l.m[k]) {
			break
		}
	}
}

func (l *LockedMap) Len() int {
	l.RLock()
	defer l.RUnlock()

	return len(l.m)
}

func (l *LockedMap) Empty() {
	l.Lock()
	defer l.Unlock()

	l.m = map[interface{}]interface{}{}
}

type ShardedMap struct {
	sharded []*LockedMap
	length  int64
	sync.Mutex
}

func NewShardedMap(size int64) *ShardedMap {
	sharded := make([]*LockedMap, size)

	for i := int64(0); i < size; i++ {
		sharded[i] = NewLockedMap()
	}

	return &ShardedMap{
		sharded: sharded,
	}
}

func (l *ShardedMap) Close() {
	l.Lock()
	defer l.Unlock()

	for i := range l.sharded {
		l.sharded[i].Close()
	}

	l.sharded = nil
	atomic.StoreInt64(&l.length, 0)
}

func (l *ShardedMap) Empty() {
	for i := range l.sharded {
		l.sharded[i].Empty()
	}

	atomic.StoreInt64(&l.length, 0)
}

func (l *ShardedMap) Exists(k string) bool {
	i := l.fnv(k)
	if i < 0 {
		return false
	}

	return l.sharded[i].Exists(k)
}

func (l *ShardedMap) Value(k string) (interface{}, bool) {
	i := l.fnv(k)
	if i < 0 {
		return nil, false
	}

	return l.sharded[i].Value(k)
}

func (l *ShardedMap) SetValue(k string, v interface{}) bool {
	var found bool
	_, _ = l.Set(k, func(i bool, _ interface{}) (interface{}, error) {
		found = i

		return v, nil
	})

	return found
}

func (l *ShardedMap) Get(k string, create func() (interface{}, error)) (v interface{}, found bool, _ error) {
	i := l.fnv(k)
	if i < 0 {
		return nil, false, nil
	}

	return l.sharded[i].Get(k, create)
}

func (l *ShardedMap) Set(k string, f func(bool, interface{}) (interface{}, error)) (interface{}, error) {
	i := l.fnv(k)
	if i < 0 {
		return nil, nil
	}

	return l.sharded[i].Set(k, func(found bool, i interface{}) (interface{}, error) {
		v, err := f(found, i)
		if err != nil {
			return v, err
		}

		if !found {
			atomic.AddInt64(&l.length, 1)
		}

		return v, nil
	})
}

func (l *ShardedMap) RemoveValue(k string) bool {
	removed := l.sharded[l.fnv(k)].RemoveValue(k)
	if removed {
		atomic.AddInt64(&l.length, -1)
	}

	return removed
}

func (l *ShardedMap) Remove(k string, f func(interface{}) error) (bool, error) {
	i := l.fnv(k)
	if i < 0 {
		return false, nil
	}

	return l.sharded[i].Remove(k, func(i interface{}) error {
		if f != nil {
			if err := f(i); err != nil {
				return err
			}
		}

		atomic.AddInt64(&l.length, -1)

		return nil
	})
}

func (l *ShardedMap) Traverse(f func(interface{}, interface{}) bool) {
	for i := range l.sharded {
		l.sharded[i].Traverse(f)
	}
}

func (l *ShardedMap) Len() int {
	return int(atomic.LoadInt64(&l.length))
}

const (
	shardedprime = uint32(16777619)
	shardedseed  = uint32(2166136261)
)

func (l *ShardedMap) fnv(k string) int64 {
	l.Lock()
	defer l.Unlock()

	if len(l.sharded) < 1 {
		return -1
	}

	h := shardedseed

	kl := len(k)
	for i := 0; i < kl; i++ {
		h *= shardedprime
		h ^= uint32(k[i])
	}

	return int64(h) % int64(len(l.sharded))
}

func isNilLockedValue(i interface{}) bool {
	_, ok := i.(NilLockedValue)

	return ok
}
