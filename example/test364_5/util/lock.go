package util

import (
	"fmt"
	"maps"
	"math"
	"slices"
	"sync"
	"sync/atomic"

	"github.com/pkg/errors"
	"github.com/spikeekips/mitum/util"
)

var (
	ErrLockedSetIgnore = util.NewIDError("locked ignore")
	ErrLockedMapClosed = util.NewIDError("locked map closed")
)

type LockedMap[K comparable /* key */, V any /* value */] interface { //nolint:interfacebloat //...
	Len() int
	Empty()
	Close()
	Map() map[K]V
	Exists(K) (found bool)
	Value(K) (_ V, found bool)
	SetValue(K, V) (created bool)
	RemoveValue(K) (removed bool)
	Get(K, func(_ V, found bool) error) error
	GetOrCreate(_ K, create func() (V, error), get func(_ V, created bool) error) error
	GetOrRemove(K, func(_ V, found bool) (remove bool, _ error)) error
	Set(K, func(_ V, found bool) (V, error)) (created bool, _ error)
	SetOrRemove(K, func(_ V, found bool) (_ V, remove bool, _ error)) (created, removed bool, _ error)
	Remove(K, func(_ V, found bool) error) (removed bool, _ error)
	Lock(func(
		get func(K) (_ V, found bool),
		set func(K, V) (created bool),
		remove func(K) (removed bool),
	) error) error
	Seq2(yield func(K, V) bool)
}

type SingleLockedMap[K comparable, V any] struct {
	m map[K]V
	l sync.RWMutex
}

func NewSingleLockedMap[K comparable, V any]() *SingleLockedMap[K, V] {
	return &SingleLockedMap[K, V]{
		m: map[K]V{},
	}
}

func (l *SingleLockedMap[K, V]) Len() int {
	l.l.RLock()
	defer l.l.RUnlock()

	return len(l.m)
}

func (l *SingleLockedMap[K, V]) Map() map[K]V {
	l.l.RLock()
	defer l.l.RUnlock()

	return l.m
}

func (l *SingleLockedMap[K, V]) Empty() {
	l.l.Lock()
	defer l.l.Unlock()

	l.m = map[K]V{}
}

func (l *SingleLockedMap[K, V]) Close() {
	l.l.Lock()
	defer l.l.Unlock()

	clear(l.m)
	l.m = nil
}

func (l *SingleLockedMap[K, V]) Exists(k K) bool {
	l.l.RLock()
	defer l.l.RUnlock()

	_, found := l.m[k]

	return found
}

func (l *SingleLockedMap[K, V]) Value(k K) (v V, found bool) {
	l.l.RLock()
	defer l.l.RUnlock()

	v, found = l.m[k]

	return v, found
}

func (l *SingleLockedMap[K, V]) SetValue(k K, v V) (created bool) {
	l.l.Lock()
	defer l.l.Unlock()

	if l.m == nil {
		return false
	}

	_, found := l.m[k]

	l.m[k] = v

	return !found
}

func (l *SingleLockedMap[K, V]) RemoveValue(k K) bool {
	l.l.Lock()
	defer l.l.Unlock()

	_, found := l.m[k]
	if found {
		delete(l.m, k)
	}

	return found
}

func (l *SingleLockedMap[K, V]) Get(k K, f func(value V, found bool) error) error {
	l.l.RLock()
	defer l.l.RUnlock()

	v, found := l.m[k]

	return f(v, found)
}

func (l *SingleLockedMap[K, V]) GetOrCreate(
	k K,
	create func() (V, error),
	get func(_ V, created bool) error,
) error {
	l.l.Lock()
	defer l.l.Unlock()

	if l.m == nil {
		return ErrLockedMapClosed.WithStack()
	}

	if i, found := l.m[k]; found {
		return get(i, false)
	}

	switch i, err := create(); {
	case err == nil:
		l.m[k] = i

		return get(i, true)
	case errors.Is(err, ErrLockedSetIgnore):
		return nil
	default:
		return err
	}
}

func (l *SingleLockedMap[K, V]) GetOrRemove(
	k K,
	get func(_ V, found bool) (remove bool, _ error),
) error {
	l.l.Lock()
	defer l.l.Unlock()

	if l.m == nil {
		return ErrLockedMapClosed.WithStack()
	}

	v, found := l.m[k]

	switch remove, err := get(v, found); {
	case err == nil:
		if found && remove {
			delete(l.m, k)
		}

		return nil
	case errors.Is(err, ErrLockedSetIgnore):
		return nil
	default:
		return err
	}
}

func (l *SingleLockedMap[K, V]) Set(
	k K,
	f func(_ V, found bool) (V, error),
) (created bool, _ error) {
	l.l.Lock()
	defer l.l.Unlock()

	if l.m == nil {
		return false, ErrLockedMapClosed.WithStack()
	}

	i, found := l.m[k]

	switch j, err := f(i, found); {
	case err == nil:
		l.m[k] = j

		return !found, nil
	case errors.Is(err, ErrLockedSetIgnore):
		return false, nil
	default:
		return false, err
	}
}

func (l *SingleLockedMap[K, V]) SetOrRemove(
	k K,
	f func(V, bool) (_ V, remove bool, _ error),
) (created, removed bool, _ error) {
	l.l.Lock()
	defer l.l.Unlock()

	if l.m == nil {
		return false, false, ErrLockedMapClosed.WithStack()
	}

	i, found := l.m[k]

	switch j, remove, err := f(i, found); {
	case errors.Is(err, ErrLockedSetIgnore):
		return false, false, nil
	case err != nil:
		return false, false, err
	case found && remove:
		delete(l.m, k)

		return false, true, nil
	case !remove:
		l.m[k] = j

		return !found, false, nil
	default:
		return false, false, nil
	}
}

func (l *SingleLockedMap[K, V]) Remove(
	k K,
	f func(V, bool) error,
) (removed bool, _ error) {
	l.l.Lock()
	defer l.l.Unlock()

	if l.m == nil {
		return false, ErrLockedMapClosed.WithStack()
	}

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

func (l *SingleLockedMap[K, V]) Lock(
	f func(
		get func(K) (_ V, found bool),
		set func(K, V) (created bool),
		remove func(K) (removed bool),
	) error,
) error {
	l.l.Lock()
	defer l.l.Unlock()

	return f(
		func(k K) (_ V, found bool) {
			v, found := l.m[k]

			return v, found
		},
		func(k K, v V) (created bool) {
			_, found := l.m[k]

			l.m[k] = v

			return !found
		},
		func(k K) (removed bool) {
			if _, found := l.m[k]; !found {
				return false
			}

			delete(l.m, k)

			return true
		},
	)
}

func (l *SingleLockedMap[K, V]) Seq2(yield func(K, V) bool) {
	l.l.RLock()
	defer l.l.RUnlock()

	for k, v := range maps.All(l.m) {
		if !yield(k, v) {
			return
		}
	}
}

type ShardedLockedMap[K comparable, V any] struct {
	hashf   func(K) uint64
	newMapf func() (LockedMap[K, V], error)
	sharded []LockedMap[K, V]
	length  int64
	l       sync.RWMutex
}

func NewShardedLockedMap[K comparable, V any](
	newMapf func() (LockedMap[K, V], error),
	size, grow uint64,
) (*ShardedLockedMap[K, V], error) {
	switch {
	case size < 1:
		return nil, errors.Errorf("new ShardedLockedMap; empty size")
	case size == 1:
		return nil, errors.Errorf("new ShardedLockedMap; 1 size; use LockedMap")
	case grow < size:
		return nil, errors.Errorf("new ShardedLockedMap; grow < size")
	}

	if newMapf == nil {
		newMapf = func() (LockedMap[K, V], error) {
			return NewSingleLockedMap[K, V](), nil
		}
	}

	seed := NewHashSeed()

	return &ShardedLockedMap[K, V]{
		sharded: make([]LockedMap[K, V], size, grow),
		hashf: func(k K) uint64 {
			return defaultLockedMapHashFunc(seed, k, size)
		},
		newMapf: newMapf,
	}, nil
}

func (l *ShardedLockedMap[K, V]) loadChild(k K) (_ LockedMap[K, V], found, iscloed bool) {
	l.l.RLock()
	defer l.l.RUnlock()

	return l.unlockedloadChild(k)
}

func (l *ShardedLockedMap[K, V]) unlockedloadChild(k K) (_ LockedMap[K, V], found, iscloed bool) {
	if l.sharded == nil {
		return nil, false, true
	}

	index := l.hashf(k)

	item := l.sharded[index]
	if item == nil {
		return nil, false, false
	}

	if m, ok := item.(*ShardedLockedMap[K, V]); ok {
		return m.loadChild(k)
	}

	return item, true, false
}

func (l *ShardedLockedMap[K, V]) getOrCreateChild(k K) (_ LockedMap[K, V], isclosed bool) {
	l.l.Lock()
	defer l.l.Unlock()

	return l.unlockedGetOrCreateChild(k)
}

func (l *ShardedLockedMap[K, V]) unlockedGetOrCreateChild(k K) (_ LockedMap[K, V], isclosed bool) {
	if l.sharded == nil {
		return nil, true
	}

	index := l.hashf(k)

	item := l.sharded[index]

	if item == nil {
		i, err := l.newMapf()
		if err != nil {
			l.close()

			return nil, true
		}

		item = i
		l.sharded[index] = item
	}

	if m, ok := item.(*ShardedLockedMap[K, V]); ok {
		return m.getOrCreateChild(k)
	}

	return item, isclosed
}

func (l *ShardedLockedMap[K, V]) Len() int {
	return int(atomic.LoadInt64(&l.length))
}

func (l *ShardedLockedMap[K, V]) Map() (m map[K]V) {
	l.l.RLock()
	defer l.l.RUnlock()

	for s := range slices.Values(l.sharded) {
		if s == nil {
			continue
		}

		for k, v := range s.Seq2 {
			if m == nil {
				m = map[K]V{}
			}

			m[k] = v
		}
	}

	return m
}

func (l *ShardedLockedMap[K, V]) Empty() {
	l.l.Lock()
	defer l.l.Unlock()

	for s := range slices.Values(l.sharded) {
		if s == nil {
			continue
		}

		s.Empty()
	}

	atomic.StoreInt64(&l.length, 0)
}

func (l *ShardedLockedMap[K, V]) Close() {
	l.l.Lock()
	defer l.l.Unlock()

	l.close()
}

func (l *ShardedLockedMap[K, V]) close() {
	for s := range slices.Values(l.sharded) {
		if s == nil {
			continue
		}

		s.Close()
	}

	ClearSlice(l.sharded)
	l.sharded = nil
	atomic.StoreInt64(&l.length, 0)
}

func (l *ShardedLockedMap[K, V]) Exists(k K) bool {
	switch i, found, isclosed := l.loadChild(k); {
	case isclosed, !found:
		return false
	default:
		return i.Exists(k)
	}
}

func (l *ShardedLockedMap[K, V]) Value(k K) (v V, _ bool) {
	switch i, found, isclosed := l.loadChild(k); {
	case isclosed, !found:
		return v, false
	default:
		return i.Value(k)
	}
}

func (l *ShardedLockedMap[K, V]) SetValue(k K, v V) (created bool) {
	switch i, isclosed := l.getOrCreateChild(k); {
	case isclosed:
		return false
	default:
		created = i.SetValue(k, v)
		if created {
			atomic.AddInt64(&l.length, 1)
		}

		return created
	}
}

func (l *ShardedLockedMap[K, V]) RemoveValue(k K) bool {
	switch i, found, isclosed := l.loadChild(k); {
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

func (l *ShardedLockedMap[K, V]) Get(k K, f func(V, bool) error) error {
	switch i, found, isclosed := l.loadChild(k); {
	case isclosed:
		return ErrLockedMapClosed.WithStack()
	case !found:
		var v V

		return f(v, false)
	default:
		return i.Get(k, f)
	}
}

func (l *ShardedLockedMap[K, V]) GetOrCreate(
	k K,
	create func() (V, error),
	get func(V, bool) error,
) error {
	switch i, isclosed := l.getOrCreateChild(k); {
	case isclosed:
		return ErrLockedMapClosed.WithStack()
	default:
		return i.GetOrCreate(k, create, func(v V, created bool) error {
			if created {
				atomic.AddInt64(&l.length, 1)
			}

			return get(v, created)
		})
	}
}

func (l *ShardedLockedMap[K, V]) GetOrRemove(
	k K,
	get func(V, bool) (bool, error),
) error {
	l.l.Lock()
	defer l.l.Unlock()

	switch i, found, isclosed := l.loadChild(k); {
	case isclosed:
		return ErrLockedMapClosed.WithStack()
	case !found:
		var v V

		_, err := get(v, false)

		return err
	default:
		var remove bool

		switch err := i.GetOrRemove(k, func(v V, found bool) (bool, error) {
			switch r, err := get(v, found); {
			case err != nil:
				return r, err
			default:
				remove = r

				return r, nil
			}
		}); {
		case err != nil:
			return err
		case remove:
			atomic.AddInt64(&l.length, -1)
		}

		return nil
	}
}

func (l *ShardedLockedMap[K, V]) Set(k K, f func(V, bool) (V, error)) (bool, error) {
	switch i, isclosed := l.getOrCreateChild(k); {
	case isclosed:
		return false, ErrLockedMapClosed.WithStack()
	default:
		created, err := i.Set(k, f)
		if err == nil && created {
			atomic.AddInt64(&l.length, 1)
		}

		return created, err
	}
}

func (l *ShardedLockedMap[K, V]) SetOrRemove(k K, f func(V, bool) (V, bool, error)) (bool, bool, error) {
	switch i, isclosed := l.getOrCreateChild(k); {
	case isclosed:
		return false, false, ErrLockedMapClosed.WithStack()
	default:
		switch created, removed, err := i.SetOrRemove(k, f); {
		case err != nil:
			return created, removed, err
		default:
			switch {
			case created:
				atomic.AddInt64(&l.length, 1)
			case removed:
				atomic.AddInt64(&l.length, -1)
			}

			return created, removed, nil
		}
	}
}

func (l *ShardedLockedMap[K, V]) Remove(k K, f func(V, bool) error) (bool, error) {
	switch i, found, isclosed := l.loadChild(k); {
	case isclosed:
		return false, ErrLockedMapClosed.WithStack()
	case !found:
		var v V

		switch err := f(v, false); {
		case errors.Is(err, ErrLockedSetIgnore):
			return false, nil
		default:
			return false, err
		}
	default:
		switch removed, err := i.Remove(k, f); {
		case err != nil:
			return removed, err
		default:
			if removed {
				atomic.AddInt64(&l.length, -1)
			}

			return removed, nil
		}
	}
}

func (l *ShardedLockedMap[K, V]) Lock(
	f func(
		get func(K) (_ V, found bool),
		set func(K, V) (created bool),
		remove func(K) (removed bool),
	) error,
) error {
	l.l.Lock()
	defer l.l.Unlock()

	return f(
		func(k K) (v V, found bool) {
			i, found, isclosed := l.unlockedloadChild(k)
			if isclosed || !found {
				return v, false
			}

			return i.Value(k)
		},
		func(k K, v V) (created bool) {
			i, isclosed := l.unlockedGetOrCreateChild(k)
			if isclosed {
				return false
			}

			return i.SetValue(k, v)
		},
		func(k K) (removed bool) {
			i, found, isclosed := l.unlockedloadChild(k)
			if isclosed || !found {
				return false
			}

			return i.RemoveValue(k)
		},
	)
}

func (l *ShardedLockedMap[K, V]) Seq2(yield func(K, V) bool) {
	l.l.RLock()
	defer l.l.RUnlock()

	if l.sharded == nil {
		return
	}

	for s := range slices.Values(l.sharded) {
		if s == nil {
			continue
		}

		for k, v := range s.Seq2 {
			if !yield(k, v) {
				return
			}
		}
	}
}

func NewLockedMap[K comparable, V any](
	newMapf func() (LockedMap[K, V], error),
	size, grow uint64,
) (LockedMap[K, V], error) {
	if newMapf == nil {
		newMapf = func() (LockedMap[K, V], error) {
			return NewSingleLockedMap[K, V](), nil
		}
	}

	switch {
	case size == 1:
		return newMapf()
	default:
		return NewShardedLockedMap[K, V](newMapf, size, grow)
	}
}

type MultiKeysLockedMap[K comparable, V any] struct {
	m       LockedMap[K, V]
	lockchs map[K]<-chan struct{}
	l       sync.Mutex
}

func NewMultiKeysLockedMap[K comparable, V any](m LockedMap[K, V]) *MultiKeysLockedMap[K, V] {
	return &MultiKeysLockedMap[K, V]{
		m:       m,
		lockchs: map[K](<-chan struct{}){},
	}
}

func (l *MultiKeysLockedMap[K, V]) Map() LockedMap[K, V] {
	return l.m
}

func (l *MultiKeysLockedMap[K, V]) Lock(keys []K) (_ LockedMap[K, V], unlock func(), _ error) {
	nkeys := compactLockedMapKeys(keys)
	if len(nkeys) < 1 {
		return nil, nil, nil
	}

	return l.lock(nkeys)
}

func (l *MultiKeysLockedMap[K, V]) LockFunc(keys []K, f func(LockedMap[K, V]) error) error {
	nkeys := compactLockedMapKeys(keys)
	if len(nkeys) < 1 {
		return nil
	}

	switch lm, unlock, err := l.lock(nkeys); {
	case err != nil:
		return err
	default:
		defer unlock()

		return f(lm)
	}
}

func (l *MultiKeysLockedMap[K, V]) Len() int {
	return l.m.Len()
}

func (l *MultiKeysLockedMap[K, V]) Seq2(yield func(K, V) bool) {
	l.m.Seq2(yield)
}

func (l *MultiKeysLockedMap[K, V]) lock(keys []K) (_ LockedMap[K, V], unlock func(), _ error) {
	l.l.Lock()
	defer l.l.Unlock()

	if l.lockchs == nil {
		return nil, nil, ErrLockedMapClosed
	}

	chs := make([]chan<- struct{}, len(keys))

	for i := range keys {
		k := keys[i]

		if c, found := l.lockchs[k]; found {
			<-c
		}

		ch := make(chan struct{})
		l.lockchs[k] = ch
		chs[i] = ch
	}

	lm := NewLimitedLockedMap(l.m, keys)

	return lm, func() {
		lm.Close()

		for i := range chs {
			close(chs[i])
		}

		chs = ClearSlice(chs)
	}, nil
}

func (l *MultiKeysLockedMap[K, V]) Close() {
	l.l.Lock()
	defer l.l.Unlock()

	clear(l.lockchs)
	l.lockchs = nil
}

type LimitedLockedMap[K comparable, V any] struct {
	LockedMap[K, V]
	allowedKeys []K
	allowKeyf   func(K) bool
	isclosed    bool
	l           sync.RWMutex
}

func NewLimitedLockedMap[K comparable, V any](m LockedMap[K, V], allowedKeys []K) *LimitedLockedMap[K, V] {
	var km map[K]struct{}
	for k := range slices.Values(allowedKeys) {
		if km == nil {
			km = map[K]struct{}{}
		}

		km[k] = struct{}{}
	}

	return &LimitedLockedMap[K, V]{
		LockedMap:   m,
		allowedKeys: allowedKeys,
		allowKeyf: func(k K) bool {
			_, found := km[k]

			return found
		},
	}
}

func (l *LimitedLockedMap[K, V]) Exists(k K) (found bool) {
	if ok, _ := l.allowKey(k); !ok {
		return false
	}

	return l.LockedMap.Exists(k)
}

func (l *LimitedLockedMap[K, V]) Value(k K) (v V, _ bool) {
	if ok, _ := l.allowKey(k); !ok {
		return v, false
	}

	return l.LockedMap.Value(k)
}

func (l *LimitedLockedMap[K, V]) SetValue(k K, v V) bool {
	if ok, _ := l.allowKey(k); !ok {
		return false
	}

	return l.LockedMap.SetValue(k, v)
}

func (l *LimitedLockedMap[K, V]) RemoveValue(k K) bool {
	if ok, _ := l.allowKey(k); !ok {
		return false
	}

	return l.LockedMap.RemoveValue(k)
}

func (l *LimitedLockedMap[K, V]) Get(k K, f func(V, bool) error) error {
	switch ok, err := l.allowKey(k); {
	case err != nil:
		return err
	case !ok:
		var v V

		return f(v, false)
	default:
		return l.LockedMap.Get(k, f)
	}
}

// GetOrCreate does not allow to set new key out of allowed keys.
func (l *LimitedLockedMap[K, V]) GetOrCreate(k K, create func() (V, error), get func(V, bool) error) error {
	switch ok, err := l.allowKey(k); {
	case err != nil, !ok:
		return err
	default:
		return l.LockedMap.GetOrCreate(k, create, get)
	}
}

func (l *LimitedLockedMap[K, V]) Set(k K, f func(V, bool) (V, error)) (bool, error) {
	switch ok, err := l.allowKey(k); {
	case err != nil, !ok:
		return false, err
	default:
		return l.LockedMap.Set(k, f)
	}
}

func (l *LimitedLockedMap[K, V]) Remove(k K, f func(V, bool) error) (bool, error) {
	switch ok, err := l.allowKey(k); {
	case err != nil, !ok:
		return false, err
	default:
		return l.LockedMap.Remove(k, f)
	}
}

func (l *LimitedLockedMap[K, V]) SetOrRemove(k K, f func(V, bool) (V, bool, error)) (bool, bool, error) {
	switch ok, err := l.allowKey(k); {
	case err != nil, !ok:
		return false, false, err
	default:
		return l.LockedMap.SetOrRemove(k, f)
	}
}

func (l *LimitedLockedMap[K, V]) Seq2(yield func(K, V) bool) {
	l.l.Lock()
	if l.isclosed {
		return
	}
	l.l.Unlock()

	for k := range slices.Values(l.allowedKeys) {
		if v, found := l.LockedMap.Value(k); found {
			if !yield(k, v) {
				return
			}
		}
	}
}

func (l *LimitedLockedMap[K, V]) Len() (count int) {
	l.l.Lock()
	if l.isclosed {
		return 0
	}
	l.l.Unlock()

	for k := range slices.Values(l.allowedKeys) {
		if _, found := l.LockedMap.Value(k); found {
			count++
		}
	}

	return count
}

func (l *LimitedLockedMap[K, V]) Map() map[K]V {
	l.l.Lock()
	if l.isclosed {
		return nil
	}
	l.l.Unlock()

	var m map[K]V

	for k := range slices.Values(l.allowedKeys) {
		if v, found := l.LockedMap.Value(k); found {
			if m == nil {
				m = map[K]V{}
			}

			m[k] = v
		}
	}

	return m
}

func (*LimitedLockedMap[K, V]) Empty() {}
func (l *LimitedLockedMap[K, V]) Close() {
	l.l.Lock()
	defer l.l.Unlock()

	l.isclosed = true
}

func (l *LimitedLockedMap[K, V]) allowKey(k K) (bool, error) {
	if func() bool {
		l.l.RLock()
		defer l.l.RUnlock()

		return l.isclosed
	}() {
		return false, ErrLockedMapClosed
	}

	return l.allowKeyf(k), nil
}

func defaultLockedMapIndex[K comparable](k K, stringIndexer func(s string) uint64) uint64 {
	var i uint64

	switch t := interface{}(k).(type) {
	case fmt.Stringer:
		i = stringIndexer(t.String())
	case string:
		i = stringIndexer(t)
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

func defaultLockedMapHashFunc[K comparable](seed uint64, k K, size uint64) uint64 {
	return defaultLockedMapIndex(k, func(s string) uint64 {
		return Murmur3(seed, s)
	}) % size
}

func compactLockedMapKeys[K comparable](ks []K) []K {
	var m map[K]struct{}
	defer clear(m)

	for k := range slices.Values(ks) {
		if _, found := m[k]; !found {
			if m == nil {
				m = map[K]struct{}{}
			}

			m[k] = struct{}{}
		}
	}

	return slices.Collect(maps.Keys(m))
}
