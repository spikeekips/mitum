package util

import (
	"crypto/rand"
	"fmt"
	"math"
	"math/big"
	mathrand "math/rand"
	"sync"
	"sync/atomic"

	"github.com/pkg/errors"
	"golang.org/x/exp/constraints"
	"golang.org/x/sync/singleflight"
)

var (
	ErrLockedSetIgnore = NewIDError("ignore to set locked value")
	ErrLockedMapClosed = NewIDError("locked map closed")
)

type Locked[T any] struct {
	value T
	sync.RWMutex
	isempty bool
}

func EmptyLocked[T any]() *Locked[T] {
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

func (l *Locked[T]) MustValue() (v T) {
	l.RLock()
	defer l.RUnlock()

	return l.value
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

func (l *Locked[T]) Get(f func(T, bool) error) error {
	l.RLock()
	defer l.RUnlock()

	return f(l.value, l.isempty)
}

func (l *Locked[T]) GetOrCreate(
	f func(_ T, created bool) error,
	create func() (T, error),
) error {
	l.Lock()
	defer l.Unlock()

	if !l.isempty {
		return f(l.value, false)
	}

	switch i, err := create(); {
	case errors.Is(err, ErrLockedSetIgnore):
		return nil
	case err != nil:
		return err
	default:
		l.value = i
		l.isempty = false

		return f(l.value, true)
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
	constraints.Integer | constraints.Float | ~string
}

type LockedMap[K lockedMapKeys, V any] interface { //nolint:interfacebloat //...
	Exists(key K) (found bool)
	Value(key K) (value V, found bool)
	SetValue(key K, value V) (added bool)
	RemoveValue(key K) (removed bool)
	Get(key K, f func(value V, found bool) error) error
	GetOrCreate(key K, f func(value V, created bool) error, create func() (value V, _ error)) error
	Set(key K, f func(_ V, found bool) (value V, _ error)) (value V, created bool, _ error)
	Remove(key K, f func(value V, found bool) error) (removed bool, _ error)
	SetOrRemove(key K, f func(_ V, found bool) (value V, remove bool, _ error)) (_ V, created, removed bool, _ error)
	Traverse(f func(key K, value V) (keep bool)) bool
	Len() int
	Empty()
	Close()
	Map() map[K]V
}

func NewLockedMap[K lockedMapKeys, V any](
	size uint64,
	newMap func() LockedMap[K, V],
) (LockedMap[K, V], error) {
	if newMap == nil {
		newMap = func() LockedMap[K, V] { //revive:disable-line:modifies-parameter
			return NewSingleLockedMap[K, V]()
		}
	}

	switch {
	case size < 1:
		return nil, errors.Errorf("new LockedMap; empty size")
	case size == 1:
		return newMap(), nil
	default:
		return NewShardedMap[K, V](size, newMap)
	}
}

type SingleLockedMap[K lockedMapKeys, V any] struct {
	m map[K]V
	sync.RWMutex
}

func NewSingleLockedMap[K lockedMapKeys, V any]() *SingleLockedMap[K, V] {
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

func (l *SingleLockedMap[K, V]) SetValue(k K, v V) (added bool) {
	l.Lock()
	defer l.Unlock()

	if l.m == nil {
		return false
	}

	_, found := l.m[k]

	l.m[k] = v

	return !found
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

func (l *SingleLockedMap[K, V]) Get(key K, f func(value V, found bool) error) error {
	l.RLock()
	defer l.RUnlock()

	var v V
	var found bool

	if l.m != nil {
		v, found = l.m[key]
	}

	return f(v, found)
}

func (l *SingleLockedMap[K, V]) GetOrCreate(
	k K,
	f func(_ V, created bool) error,
	create func() (V, error),
) error {
	l.Lock()
	defer l.Unlock()

	if l.m == nil {
		return ErrLockedMapClosed.WithStack()
	}

	if i, found := l.m[k]; found {
		return f(i, false)
	}

	switch i, err := create(); {
	case err == nil:
		l.m[k] = i

		return f(i, true)
	case errors.Is(err, ErrLockedSetIgnore):
		return nil
	default:
		return err
	}
}

func (l *SingleLockedMap[K, V]) Set(k K, f func(_ V, found bool) (V, error)) (v V, created bool, _ error) {
	l.Lock()
	defer l.Unlock()

	if l.m == nil {
		return v, false, ErrLockedMapClosed.WithStack()
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

func (l *SingleLockedMap[K, V]) Remove(k K, f func(V, bool) error) (bool, error) {
	l.Lock()
	defer l.Unlock()

	i, found := l.m[k]

	switch err := f(i, found); {
	case err == nil:
		if found {
			delete(l.m, k)
		}

		return found, nil
	case errors.Is(err, ErrLockedSetIgnore):
		return false, nil
	default:
		return false, err
	}
}

func (l *SingleLockedMap[K, V]) SetOrRemove(
	k K,
	f func(V, bool) (v V, _ bool, _ error),
) (v V, _ bool, _ bool, _ error) {
	l.Lock()
	defer l.Unlock()

	if l.m == nil {
		return v, false, false, ErrLockedMapClosed.WithStack()
	}

	i, found := l.m[k]

	switch j, remove, err := f(i, found); {
	case errors.Is(err, ErrLockedSetIgnore):
		return i, false, false, nil // NOTE existing value
	case err != nil:
		return v, false, false, err
	case found && remove:
		delete(l.m, k)

		return v, false, true, nil
	case !remove:
		l.m[k] = j

		return j, !found, false, nil
	default:
		return v, false, false, nil
	}
}

func (l *SingleLockedMap[K, V]) Traverse(f func(K, V) bool) bool {
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

func (l *SingleLockedMap[K, V]) Map() (m map[K]V) {
	l.RLock()
	defer l.RUnlock()

	for k := range l.m {
		if m == nil {
			m = map[K]V{}
		}

		m[k] = l.m[k]
	}

	return m
}

type ShardedMap[K lockedMapKeys, V any] struct {
	hashf   func(interface{}) (uint64, interface{})
	newMap  func() LockedMap[K, V]
	sharded []LockedMap[K, V]
	seed    uint32
	length  int64
	sync.RWMutex
}

func NewShardedMap[K lockedMapKeys, V any](
	size uint64,
	newMap func() LockedMap[K, V],
) (*ShardedMap[K, V], error) {
	seed := newDjb2Seed()

	return NewShardedMapWithSeed(
		seed,
		size,
		func(k interface{}, size uint64) (uint64, interface{}) {
			return defaultHashFunc(seed, k, size)
		},
		newMap,
	)
}

func NewDeepShardedMap[K lockedMapKeys, V any](
	sizes []uint64,
	newMap func() LockedMap[K, V],
) (*ShardedMap[K, V], error) {
	switch {
	case len(sizes) < 1:
		return nil, errors.Errorf("new DeepShardedMap; empty size")
	case len(sizes) < 2:
		return nil, errors.Errorf("new DeepShardedMap; size should be at least 2")
	}

	seed := newDjb2Seed()

	var total uint64 = 1

	for i := range sizes {
		total *= sizes[i]
	}

	newMaps := make([]func() LockedMap[K, V], len(sizes))
	for i := range newMaps {
		i := i
		size := sizes[i]
		lsizes := sizes[i : len(sizes)-1]
		df := deepHashFunc(seed, lsizes, total)

		hashf := func(k interface{}, size uint64) (uint64, interface{}) {
			j, ok := k.(deepHashedKey)
			if !ok {
				panic(fmt.Sprintf("expected deepHashedKey, not %T", k))
			}

			return df(j, size)
		}

		newMaps[i] = func() LockedMap[K, V] {
			var f func() LockedMap[K, V]

			switch {
			case i == len(newMaps)-1:
				f = newMap
			default:
				f = newMaps[i+1]
			}

			l, _ := NewShardedMapWithSeed[K, V](seed, size, hashf, f)

			return l
		}
	}

	df := deepHashFunc(seed, sizes[:len(sizes)-1], total)
	hashf := func(k interface{}, size uint64) (uint64, interface{}) {
		return df(defaultindex(seed, k), size)
	}

	return NewShardedMapWithSeed(seed, sizes[0], hashf, newMaps[0])
}

func NewShardedMapWithSeed[K lockedMapKeys, V any](
	seed uint32,
	size uint64,
	hashf func(interface{}, uint64) (uint64, interface{}),
	newMap func() LockedMap[K, V],
) (*ShardedMap[K, V], error) {
	switch {
	case size < 1:
		return nil, errors.Errorf("new ShardedMap; empty size")
	case size == 1:
		return nil, errors.Errorf("new ShardedMap; 1 size; use LockedMap")
	}

	if newMap == nil {
		newMap = func() LockedMap[K, V] { //revive:disable-line:modifies-parameter
			return NewSingleLockedMap[K, V]()
		}
	}

	return &ShardedMap[K, V]{
		seed:    seed,
		sharded: make([]LockedMap[K, V], size),
		hashf: func(k interface{}) (uint64, interface{}) {
			return hashf(k, size)
		},
		newMap: newMap,
	}, nil
}

func (l *ShardedMap[K, V]) Exists(k K) bool {
	switch i, found, isclosed := l.loadItem(k); {
	case isclosed, !found:
		return false
	default:
		return i.Exists(k)
	}
}

func (l *ShardedMap[K, V]) Value(k K) (v V, found bool) {
	switch i, found, isclosed := l.loadItem(k); {
	case isclosed, !found:
		return v, false
	default:
		return i.Value(k)
	}
}

func (l *ShardedMap[K, V]) SetValue(k K, v V) (added bool) {
	switch i, isclosed := l.newItem(k); {
	case isclosed:
		return false
	default:
		added = i.SetValue(k, v)
		if added {
			atomic.AddInt64(&l.length, 1)
		}

		return added
	}
}

func (l *ShardedMap[K, V]) RemoveValue(k K) bool {
	switch i, found, isclosed := l.loadItem(k); {
	case isclosed, !found:
		return false
	default:
		removed := i.RemoveValue(k)
		if removed {
			atomic.AddInt64(&l.length, -1)
		}

		return removed
	}
}

func (l *ShardedMap[K, V]) Get(key K, f func(value V, found bool) error) error {
	switch i, found, isclosed := l.loadItem(key); {
	case isclosed:
		return ErrLockedMapClosed.WithStack()
	case isclosed, !found:
		var v V

		return f(v, false)
	default:
		return i.Get(key, f)
	}
}

func (l *ShardedMap[K, V]) GetOrCreate(
	k K,
	f func(_ V, created bool) error,
	create func() (V, error),
) error {
	switch i, isclosed := l.newItem(k); {
	case isclosed:
		return ErrLockedMapClosed.WithStack()
	default:
		var created bool

		err := i.GetOrCreate(
			k,
			func(v V, c bool) error {
				created = c

				return f(v, c)
			},
			create,
		)
		if err == nil && created {
			atomic.AddInt64(&l.length, 1)
		}

		return err
	}
}

func (l *ShardedMap[K, V]) Set(k K, f func(V, bool) (V, error)) (v V, created bool, _ error) {
	switch i, isclosed := l.newItem(k); {
	case isclosed:
		return v, false, ErrLockedMapClosed.WithStack()
	default:
		v, created, err := i.Set(k, f)
		if err == nil && created {
			atomic.AddInt64(&l.length, 1)
		}

		return v, created, err
	}
}

func (l *ShardedMap[K, V]) Remove(k K, f func(V, bool) error) (bool, error) {
	switch i, found, isclosed := l.loadItem(k); {
	case isclosed:
		return false, ErrLockedMapClosed.WithStack()
	case found:
		removed, err := i.Remove(k, f)
		if err == nil && removed {
			atomic.AddInt64(&l.length, -1)
		}

		return removed, err
	}

	var v V

	switch err := f(v, false); {
	case errors.Is(err, ErrLockedSetIgnore):
		return false, nil
	default:
		return false, err
	}
}

func (l *ShardedMap[K, V]) SetOrRemove(k K, f func(V, bool) (V, bool, error)) (v V, _ bool, _ bool, _ error) {
	switch i, isclosed := l.newItem(k); {
	case isclosed:
		return v, false, false, ErrLockedMapClosed.WithStack()
	default:
		j, created, removed, err := i.SetOrRemove(k, f)
		if err == nil {
			switch {
			case created:
				atomic.AddInt64(&l.length, 1)
			case removed:
				atomic.AddInt64(&l.length, -1)
			}
		}

		return j, created, removed, err
	}
}

func (l *ShardedMap[K, V]) Traverse(f func(K, V) bool) bool {
	var last int

	for {
		item := l.next(&last)
		if item == nil {
			break
		}

		if !item.Traverse(f) {
			return false
		}
	}

	return true
}

func (l *ShardedMap[K, V]) Map() (m map[K]V) {
	l.RLock()
	defer l.RUnlock()

	for i := range l.sharded {
		if m == nil {
			m = map[K]V{}
		}

		if l.sharded[i] == nil {
			continue
		}

		sm := l.sharded[i].Map()
		for k := range sm {
			m[k] = sm[k]
		}
	}

	return m
}

type traverseMaper[K lockedMapKeys, V any] interface {
	TraverseMap(f func(LockedMap[K, V]) bool) bool
}

func (l *ShardedMap[K, V]) TraverseMap(f func(LockedMap[K, V]) bool) bool {
	var last int

	for {
		item := l.next(&last)
		if item == nil {
			break
		}

		switch i, ok := item.(traverseMaper[K, V]); {
		case ok:
			if !i.TraverseMap(f) {
				return false
			}
		default:
			if !f(item) {
				return false
			}
		}
	}

	return true
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
	l.Lock()
	defer l.Unlock()

	for i := range l.sharded {
		if l.sharded[i] == nil {
			continue
		}

		l.sharded[i].Empty()
	}

	atomic.StoreInt64(&l.length, 0)
}

func (l *ShardedMap[K, V]) loadItem(k interface{}) (_ LockedMap[K, V], found, iscloed bool) {
	l.RLock()
	defer l.RUnlock()

	if len(l.sharded) < 1 {
		return nil, false, true
	}

	index, nextkey := l.hashf(k)

	item := l.sharded[index]
	if item == nil {
		return nil, false, false
	}

	switch m, ok := item.(*ShardedMap[K, V]); {
	case ok:
		item, found, iscloed = m.loadItem(nextkey)
	default:
		found = true
	}

	return item, found, iscloed
}

func (l *ShardedMap[K, V]) newItem(k interface{}) (_ LockedMap[K, V], isclosed bool) {
	l.Lock()
	defer l.Unlock()

	if len(l.sharded) < 1 {
		return nil, true
	}

	index, nextkey := l.hashf(k)

	item := l.sharded[index]

	if item == nil {
		item = l.newMap()

		l.sharded[index] = item
	}

	if m, ok := item.(*ShardedMap[K, V]); ok {
		item, isclosed = m.newItem(nextkey)
	}

	return item, isclosed
}

func (l *ShardedMap[K, V]) next(last *int) LockedMap[K, V] {
	l.RLock()
	defer l.RUnlock()

	for {
		switch {
		case len(l.sharded) < 1:
			return nil
		case len(l.sharded) == *last:
			return nil
		}

		item := l.sharded[*last]

		*last++

		if item != nil {
			return item
		}
	}
}

func defaultindex(seed uint32, k interface{}) uint64 {
	var i uint64

	switch t := k.(type) {
	case fmt.Stringer:
		i = stringdjb2(seed, t.String())
	case string:
		i = stringdjb2(seed, t)
	case float32:
		i = uint64(math.Abs(float64(t)))
	case float64:
		i = uint64(math.Abs(t))
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
	case uintptr:
		i = uint64(t)
	}

	return i
}

func defaultHashFunc(seed uint32, k interface{}, size uint64) (uint64, interface{}) {
	return defaultindex(seed, k) % size, k
}

func newDjb2Seed() uint32 {
	max := big.NewInt(0).SetUint64(uint64(math.MaxUint32))

	switch rnd, err := rand.Int(rand.Reader, max); {
	case err != nil:
		return mathrand.Uint32()
	default:
		return uint32(rnd.Uint64())
	}
}

// stringdjb2 derived from
// https://github.com/patrickmn/go-cache/blob/master/sharded.go .
func stringdjb2(seed uint32, k string) uint64 {
	var (
		l = uint32(len(k))
		d = 5381 + seed + l
		i = uint32(0)
	)

	if l >= 4 { //nolint:gomnd //...
		for i < l-4 {
			d = (d * 33) ^ uint32(k[i])   //nolint:gomnd //...
			d = (d * 33) ^ uint32(k[i+1]) //nolint:gomnd //...
			d = (d * 33) ^ uint32(k[i+2]) //nolint:gomnd //...
			d = (d * 33) ^ uint32(k[i+3]) //nolint:gomnd //...
			i += 4
		}
	}

	switch l - i {
	case 1:
	case 2:
		d = (d * 33) ^ uint32(k[i]) //nolint:gomnd //...
	case 3: //nolint:gomnd //...
		d = (d * 33) ^ uint32(k[i])   //nolint:gomnd //...
		d = (d * 33) ^ uint32(k[i+1]) //nolint:gomnd //...
	case 4: //nolint:gomnd //...
		d = (d * 33) ^ uint32(k[i])   //nolint:gomnd //...
		d = (d * 33) ^ uint32(k[i+1]) //nolint:gomnd //...
		d = (d * 33) ^ uint32(k[i+2]) //nolint:gomnd //...
	}

	return uint64(d ^ (d >> 16)) //nolint:gomnd //...
}

type deepHashedKey uint64

func deepHashFunc(seed uint32, sizes []uint64, total uint64) func(interface{}, uint64) (uint64, interface{}) {
	return func(k interface{}, size uint64) (uint64, interface{}) {
		var index uint64

		switch j, ok := k.(deepHashedKey); {
		case ok:
			index = uint64(j)
		default:
			index = defaultindex(seed, k)
		}

		newindex := float64(index % total)

		for j := len(sizes) - 1; j >= 0; j-- {
			newindex = math.Floor(newindex / float64(sizes[j]))
		}

		return uint64(newindex) % size, deepHashedKey(index)
	}
}

func SingleflightDo[T any]( //revive:disable-line:error-return
	sg *singleflight.Group, key string, f func() (T, error),
) (t T, _ error, _ bool) {
	i, err, shared := sg.Do(key, func() (interface{}, error) {
		return f()
	})

	if i != nil {
		t = i.(T) //nolint:forcetypeassert //...
	}

	return t, errors.WithStack(err), shared
}
