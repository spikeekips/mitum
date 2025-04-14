package util

import (
	"iter"
	"maps"
	"slices"
	"sync"
	"sync/atomic"

	"github.com/pkg/errors"
)

var (
	ErrLockedSetIgnore            = NewSError("locked ignore")
	ErrLockedMapClosed            = NewSError("locked map closed")
	ErrSetOnlyLockedMapNotAllowed = NewSError("set only locked map")
)

type mapIndexType interface {
	~string |
		~float32 | ~float64 |
		~int | ~int8 | ~int16 | ~int32 | ~int64 |
		~uint | ~uint8 | ~uint16 | ~uint32 | ~uint64 |
		~uintptr
}

type LockedMap[K mapIndexType /* key */, V any /* value */] interface { //nolint:interfacebloat //...
	Len() int
	Empty()
	Close()
	Map() map[K]V
	Exists(K) (found bool)
	Value(K) (_ V, found bool)
	SetValue(_ K, _ V) (created bool)
	RemoveValues(...K) (removed int)
	Get(_ K, _ func(_ V, found bool) error) error
	GetOrCreate(_ K, create func() (V, error), get func(_ V, created bool) error) error
	GetOrRemove(_ K, _ func(_ V, found bool) (remove bool, _ error)) error
	Set(_ K, _ func(_ V, found bool) (V, error)) (created bool, _ error)
	SetOrRemove(_ K, _ func(_ V, found bool) (_ V, remove bool, _ error)) (created, removed bool, _ error)
	Remove(_ K, _ func(_ V, found bool) error) (removed bool, _ error)
	Lock(func(
		get func(K) (_ V, found bool),
		set func(_ K, _ V) (created bool),
		remove func(K) (removed bool),
		traverse iter.Seq2[K, V],
	) error) error
	Seq2(yield func(K, V) bool)
	LenFunc(func(int) error) error
	CloseFunc(func())
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

	l.closef()
}

func (l *SingleLockedMap[K, V]) closef() {
	clear(l.m)
	l.m = nil
}

func (l *SingleLockedMap[K, V]) Exists(k K) bool {
	l.l.RLock()
	defer l.l.RUnlock()

	_, found := l.m[k]

	return found
}

func (l *SingleLockedMap[K, V]) Value(k K) (V, bool) {
	l.l.RLock()
	defer l.l.RUnlock()

	v, found := l.m[k]

	return v, found
}

func (l *SingleLockedMap[K, V]) SetValue(k K, v V) bool {
	l.l.Lock()
	defer l.l.Unlock()

	if l.m == nil {
		return false
	}

	_, found := l.m[k]

	l.m[k] = v

	return !found
}

func (l *SingleLockedMap[K, V]) RemoveValues(ks ...K) int {
	l.l.Lock()
	defer l.l.Unlock()

	var removed int

	for k := range slices.Values(ks) {
		_, found := l.m[k]
		if found {
			delete(l.m, k)

			removed++
		}
	}

	return removed
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
		return ErrLockedMapClosed
	}

	if i, found := l.m[k]; found {
		return get(i, false)
	}

	switch i, err := create(); err {
	case nil:
		l.m[k] = i

		return get(i, true)
	case ErrLockedSetIgnore: //nolint:errorlint
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
		return ErrLockedMapClosed
	}

	v, found := l.m[k]

	switch remove, err := get(v, found); err {
	case nil:
		if found && remove {
			delete(l.m, k)
		}

		return nil
	case ErrLockedSetIgnore: //nolint:errorlint
		return nil
	default:
		return err
	}
}

func (l *SingleLockedMap[K, V]) Set(
	k K,
	f func(_ V, found bool) (V, error),
) (bool, error) {
	l.l.Lock()
	defer l.l.Unlock()

	if l.m == nil {
		return false, ErrLockedMapClosed
	}

	i, found := l.m[k]

	switch j, err := f(i, found); err {
	case nil:
		l.m[k] = j

		return !found, nil
	case ErrLockedSetIgnore: //nolint:errorlint
		return false, nil
	default:
		return false, err
	}
}

func (l *SingleLockedMap[K, V]) SetOrRemove(
	k K,
	f func(V, bool) (_ V, remove bool, _ error),
) (bool, bool, error) {
	l.l.Lock()
	defer l.l.Unlock()

	if l.m == nil {
		return false, false, ErrLockedMapClosed
	}

	i, found := l.m[k]

	switch j, remove, err := f(i, found); {
	case err == ErrLockedSetIgnore: //nolint:errorlint
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
) (bool, error) {
	l.l.Lock()
	defer l.l.Unlock()

	if l.m == nil {
		return false, ErrLockedMapClosed
	}

	i, found := l.m[k]

	switch err := f(i, found); err {
	case nil:
		if found {
			delete(l.m, k)
		}

		return found, nil
	case ErrLockedSetIgnore: //nolint:errorlint
		return false, nil
	default:
		return false, err
	}
}

func (l *SingleLockedMap[K, V]) Lock(
	f func(
		get func(K) (V, bool),
		set func(K, V) bool,
		remove func(K) bool,
		traverse iter.Seq2[K, V],
	) error,
) error {
	l.l.Lock()
	defer l.l.Unlock()

	return f(
		func(k K) (V, bool) {
			v, found := l.m[k]

			return v, found
		},
		func(k K, v V) bool {
			_, found := l.m[k]

			l.m[k] = v

			return !found
		},
		func(k K) bool {
			if _, found := l.m[k]; !found {
				return false
			}

			delete(l.m, k)

			return true
		},
		func(yield func(K, V) bool) {
			for k, v := range maps.All(l.m) {
				if !yield(k, v) {
					return
				}
			}
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

func (l *SingleLockedMap[K, V]) LenFunc(f func(int) error) error {
	l.l.RLock()
	defer l.l.RUnlock()

	return f(len(l.m))
}

func (l *SingleLockedMap[K, V]) CloseFunc(f func()) {
	l.l.Lock()
	defer l.l.Unlock()

	if l.m == nil {
		return
	}

	f()

	l.closef()
}

type ShardedLockedMap[K mapIndexType, V any] struct {
	hashf   func(K) uint64
	newMapf func() (LockedMap[K, V], error)
	sharded []LockedMap[K, V]
	length  int64
	l       sync.RWMutex
}

func NewShardedLockedMap[K mapIndexType, V any](
	newMapf func() (LockedMap[K, V], error),
	size uint64,
) (*ShardedLockedMap[K, V], error) {
	switch {
	case size < 1:
		return nil, errors.New("new ShardedLockedMap; empty size")
	case size == 1:
		return nil, errors.New("new ShardedLockedMap; 1 size; use LockedMap")
	}

	if newMapf == nil {
		newMapf = func() (LockedMap[K, V], error) {
			return NewSingleLockedMap[K, V](), nil
		}
	}

	return &ShardedLockedMap[K, V]{
		sharded: make([]LockedMap[K, V], size),
		hashf:   newFuncDefaultLockedMapHash[K](size),
		newMapf: newMapf,
	}, nil
}

func (l *ShardedLockedMap[K, V]) loadChild(k K) (_ LockedMap[K, V], found, iscloed bool) { //nolint:nonamedreturns
	l.l.RLock()
	defer l.l.RUnlock()

	return l.unlockedloadChild(k)
}

func (l *ShardedLockedMap[K, V]) unlockedloadChild(k K) ( //nolint:nonamedreturns
	_ LockedMap[K, V], found, iscloed bool,
) {
	if l.sharded == nil {
		return nil, false, true
	}

	item, found, iscloed := l.unlockedloadChildByIndex(l.hashf(k))
	if !found || iscloed {
		return nil, false, false
	}

	if m, ok := item.(*ShardedLockedMap[K, V]); ok {
		return m.loadChild(k)
	}

	return item, true, false
}

func (l *ShardedLockedMap[K, V]) unlockedloadChildByIndex(index uint64) ( //nolint:nonamedreturns
	_ LockedMap[K, V], found, iscloed bool,
) {
	if l.sharded == nil {
		return nil, false, true
	}

	item := l.sharded[index]
	if item == nil {
		return nil, false, false
	}

	return item, true, false
}

func (l *ShardedLockedMap[K, V]) getOrCreateChild(k K) (_ LockedMap[K, V], isclosed bool) { //nolint:nonamedreturns
	l.l.Lock()
	defer l.l.Unlock()

	return l.unlockedGetOrCreateChild(k)
}

func (l *ShardedLockedMap[K, V]) unlockedGetOrCreateChild(k K) ( //nolint:nonamedreturns
	_ LockedMap[K, V], isclosed bool,
) {
	if l.sharded == nil {
		return nil, true
	}

	index := l.hashf(k)

	item := l.sharded[index]

	if item == nil {
		i, err := l.newMapf()
		if err != nil {
			l.closef()

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

func (l *ShardedLockedMap[K, V]) Map() map[K]V {
	l.l.RLock()
	defer l.l.RUnlock()

	var m map[K]V

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

	l.closef()
}

func (l *ShardedLockedMap[K, V]) closef() {
	for s := range slices.Values(l.sharded) {
		if s == nil {
			continue
		}

		s.Close()
	}

	ClearSlice(&l.sharded)
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

func (l *ShardedLockedMap[K, V]) Value(k K) (V, bool) {
	var v V

	switch i, found, isclosed := l.loadChild(k); {
	case isclosed, !found:
		return v, false
	default:
		return i.Value(k)
	}
}

func (l *ShardedLockedMap[K, V]) SetValue(k K, v V) bool {
	switch i, isclosed := l.getOrCreateChild(k); {
	case isclosed:
		return false
	default:
		created := i.SetValue(k, v)
		if created {
			atomic.AddInt64(&l.length, 1)
		}

		return created
	}
}

func (l *ShardedLockedMap[K, V]) RemoveValues(ks ...K) int {
	l.l.Lock()
	defer l.l.Unlock()

	m := map[uint64][]K{}
	defer clear(m)

	for k := range slices.Values(ks) {
		index := l.hashf(k)
		m[index] = append(m[index], k)
	}

	var removed int

	for index, iks := range maps.All(m) {
		if i, found, isclosed := l.unlockedloadChildByIndex(index); !isclosed && found {
			removed += i.RemoveValues(iks...)
		}
	}

	if removed > 0 {
		atomic.AddInt64(&l.length, int64(removed*-1))
	}

	return removed
}

func (l *ShardedLockedMap[K, V]) Get(k K, f func(V, bool) error) error {
	switch i, found, isclosed := l.loadChild(k); {
	case isclosed:
		return ErrLockedMapClosed
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
		return ErrLockedMapClosed
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

	switch i, found, isclosed := l.unlockedloadChild(k); {
	case isclosed:
		return ErrLockedMapClosed
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
		return false, ErrLockedMapClosed
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
		return false, false, ErrLockedMapClosed
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
		return false, ErrLockedMapClosed
	case !found:
		var v V

		switch err := f(v, false); err {
		case ErrLockedSetIgnore: //nolint:errorlint
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
		get func(K) (V, bool),
		set func(K, V) bool,
		remove func(K) bool,
		traverse iter.Seq2[K, V],
	) error,
) error {
	l.l.Lock()
	defer l.l.Unlock()

	return f(
		func(k K) (V, bool) {
			i, found, isclosed := l.unlockedloadChild(k)
			if isclosed || !found {
				var v V

				return v, false
			}

			return i.Value(k)
		},
		func(k K, v V) bool {
			i, isclosed := l.unlockedGetOrCreateChild(k)
			if isclosed {
				return false
			}

			return i.SetValue(k, v)
		},
		func(k K) bool {
			i, found, isclosed := l.unlockedloadChild(k)
			if isclosed || !found {
				return false
			}

			return i.RemoveValues(k) > 0
		},
		func(yield func(K, V) bool) {
			l.seq2(yield)
		},
	)
}

func (l *ShardedLockedMap[K, V]) Seq2(yield func(K, V) bool) {
	l.l.RLock()
	defer l.l.RUnlock()

	l.seq2(yield)
}

func (l *ShardedLockedMap[K, V]) seq2(yield func(K, V) bool) {
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

func (l *ShardedLockedMap[K, V]) LenFunc(f func(int) error) error {
	l.l.RLock()
	defer l.l.RUnlock()

	return f(int(atomic.LoadInt64(&l.length)))
}

func (l *ShardedLockedMap[K, V]) CloseFunc(f func()) {
	l.l.Lock()
	defer l.l.Unlock()

	if l.sharded == nil {
		return
	}

	l.closef()
}

func NewLockedMap[K mapIndexType, V any](
	newMapf func() (LockedMap[K, V], error),
	size uint64,
) (LockedMap[K, V], error) {
	if newMapf == nil {
		newMapf = func() (LockedMap[K, V], error) {
			return NewSingleLockedMap[K, V](), nil
		}
	}

	switch size {
	case 1:
		return newMapf()
	default:
		return NewShardedLockedMap[K, V](newMapf, size)
	}
}

type MultiKeysLockedMap[K mapIndexType, V any] struct {
	m       LockedMap[K, V]
	lockchs map[K]<-chan struct{}
	l       sync.Mutex
}

func NewMultiKeysLockedMap[K mapIndexType, V any](m LockedMap[K, V]) *MultiKeysLockedMap[K, V] {
	return &MultiKeysLockedMap[K, V]{
		m:       m,
		lockchs: map[K](<-chan struct{}){},
	}
}

func (l *MultiKeysLockedMap[K, V]) Map() LockedMap[K, V] {
	return l.m
}

func (l *MultiKeysLockedMap[K, V]) Lock(keys []K) (_ LockedMap[K, V], unlock func(), _ error) { //nolint:nonamedreturns
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

func (l *MultiKeysLockedMap[K, V]) lock(keys []K) (_ LockedMap[K, V], unlock func(), _ error) { //nolint:nonamedreturns
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

		ClearSlice(&chs)
	}, nil
}

func (l *MultiKeysLockedMap[K, V]) Close() {
	l.l.Lock()
	defer l.l.Unlock()

	clear(l.lockchs)
	l.lockchs = nil
}

type LimitedLockedMap[K mapIndexType, V any] struct {
	LockedMap[K, V]
	allowKeyf   func(K) bool
	allowedKeys []K
	l           sync.RWMutex
	isclosed    bool
}

func NewLimitedLockedMap[K mapIndexType, V any](m LockedMap[K, V], allowedKeys []K) *LimitedLockedMap[K, V] {
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

func (l *LimitedLockedMap[K, V]) Exists(k K) bool {
	if ok, _ := l.allowKey(k); !ok {
		return false
	}

	return l.LockedMap.Exists(k)
}

func (l *LimitedLockedMap[K, V]) Value(k K) (V, bool) {
	if ok, _ := l.allowKey(k); !ok {
		var v V

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

func (l *LimitedLockedMap[K, V]) RemoveValues(ks ...K) int {
	nks := make([]K, 0, len(ks))

	for k := range slices.Values(ks) {
		if ok, _ := l.allowKey(k); ok {
			nks = append(nks, k)
		}
	}

	return l.LockedMap.RemoveValues(nks...)
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

func (l *LimitedLockedMap[K, V]) Len() int {
	l.l.Lock()
	if l.isclosed {
		return 0
	}
	l.l.Unlock()

	var count int

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

type SetOnlyLockedMap[K mapIndexType, V any] struct {
	LockedMap[K, V]
	keys []K
	grow uint64
	l    sync.RWMutex
}

func NewSetOnlyLockedMap[K mapIndexType, V any](l LockedMap[K, V], grow uint64) *SetOnlyLockedMap[K, V] {
	return &SetOnlyLockedMap[K, V]{
		LockedMap: l,
		keys:      make([]K, 0, grow),
		grow:      grow,
	}
}

func (l *SetOnlyLockedMap[K, V]) ResetKeys() {
	l.l.Lock()
	defer l.l.Unlock()

	switch {
	case l.Len() < 1 && len(l.keys) < 1:
		return
	case l.Len() < 1:
		l.keys = make([]K, 0, l.grow)

		return
	}

	l.keys = make([]K, l.Len(), SafeIntToUint64(l.Len())+l.grow)

	var i int
	for k := range l.LockedMap.Seq2 {
		l.keys[i] = k
		i++
	}
}

func (l *SetOnlyLockedMap[K, V]) Empty() {
	l.l.Lock()
	defer l.l.Unlock()

	l.LockedMap.Empty()
	l.keys = make([]K, 0, l.grow)
}

func (l *SetOnlyLockedMap[K, V]) Close() {
	l.l.Lock()
	defer l.l.Unlock()

	l.LockedMap.Close()
	ClearSlice(&l.keys)
}

func (l *SetOnlyLockedMap[K, V]) CloseFunc(f func()) {
	l.l.Lock()
	defer l.l.Unlock()

	l.LockedMap.CloseFunc(f)
	ClearSlice(&l.keys)
}

func (l *SetOnlyLockedMap[K, V]) SetValue(k K, v V) bool {
	l.l.Lock()
	defer l.l.Unlock()

	var created bool

	if created = l.LockedMap.SetValue(k, v); created {
		l.setKey(k)
	}

	return created
}

func (l *SetOnlyLockedMap[K, V]) Set(k K, f func(V, bool) (V, error)) (bool, error) {
	l.l.Lock()
	defer l.l.Unlock()

	created, err := l.LockedMap.Set(k, f)
	if err == nil && created {
		l.setKey(k)
	}

	return created, err
}

func (l *SetOnlyLockedMap[K, V]) GetOrCreate(k K, create func() (V, error), get func(V, bool) error) error {
	l.l.Lock()
	defer l.l.Unlock()

	return l.LockedMap.GetOrCreate(k, create,
		func(v V, created bool) error {
			if created {
				l.setKey(k)
			}

			return get(v, created)
		})
}

func (l *SetOnlyLockedMap[K, V]) RemoveValues(...K) int {
	return 0
}

func (l *SetOnlyLockedMap[K, V]) GetOrRemove(k K, f func(V, bool) (bool, error)) error {
	l.l.Lock()
	defer l.l.Unlock()

	return l.LockedMap.GetOrRemove(
		k,
		func(prev V, found bool) (bool, error) {
			if found {
				return false, ErrSetOnlyLockedMapNotAllowed
			}

			return f(prev, found)
		},
	)
}

func (l *SetOnlyLockedMap[K, V]) SetOrRemove(k K, f func(V, bool) (V, bool, error)) (bool, bool, error) {
	l.l.Lock()
	defer l.l.Unlock()

	return l.LockedMap.SetOrRemove(
		k,
		func(prev V, found bool) (V, bool, error) {
			if found {
				return prev, false, ErrSetOnlyLockedMapNotAllowed
			}

			return f(prev, found)
		},
	)
}

func (l *SetOnlyLockedMap[K, V]) Remove(k K, f func(V, bool) error) (bool, error) {
	l.l.Lock()
	defer l.l.Unlock()

	return l.LockedMap.Remove(
		k,
		func(prev V, found bool) error {
			if found {
				return ErrSetOnlyLockedMapNotAllowed
			}

			return f(prev, found)
		},
	)
}

func (l *SetOnlyLockedMap[K, V]) Lock(f func(
	get func(K) (V, bool),
	set func(K, V) bool,
	remove func(K) bool,
	traverse iter.Seq2[K, V],
) error,
) error {
	return l.LockedMap.Lock(func(
		get func(K) (V, bool),
		set func(K, V) bool,
		remove func(K) bool,
		traverse iter.Seq2[K, V],
	) error {
		l.l.Lock()
		defer l.l.Unlock()

		return f(
			get,
			func(k K, v V) bool {
				created := set(k, v)
				if created {
					l.setKey(k)
				}

				return created
			},
			func(K) bool { return false },
			traverse,
		)
	})
}

func (l *SetOnlyLockedMap[K, V]) NSeq2(n uint64) iter.Seq[iter.Seq2[K, V]] {
	l.l.RLock()
	defer l.l.RUnlock()

	size := uint64(len(l.keys))

	return func(yield func(iter.Seq2[K, V]) bool) {
		if n < 1 || size < 1 {
			return
		}

		var s uint64

		for {
			e := s + n
			if e > size {
				e = size
			}

			if !yield(func(yield2 func(K, V) bool) {
				for k := range slices.Values(l.keys[s:e]) {
					v, found := l.LockedMap.Value(k)
					if !found {
						continue
					}

					if !yield2(k, v) {
						return
					}
				}
			}) {
				return
			}

			if e == size {
				return
			}

			s = e
		}
	}
}

func (l *SetOnlyLockedMap[K, V]) setKey(k K) {
	l.keys = append(l.keys, k)
}

var murmur3Seed = NewHashSeed()

func newFuncDefaultLockedMapHash[K mapIndexType](size uint64) InFunc[K, uint64] { //nolint:funlen
	var intf func(i K) uint64

	var k K

	switch any(k).(type) {
	case string:
		intf = func(i K) uint64 {
			return Murmur3(murmur3Seed, any(i).(string)) //nolint:forcetypeassert
		}
	case float32:
		intf = func(i K) uint64 { return uint64(any(i).(float64)) } //nolint:forcetypeassert
	case float64:
		intf = func(i K) uint64 { return uint64(any(i).(float64)) } //nolint:forcetypeassert
	case int:
		intf = func(i K) uint64 { return uint64(any(i).(int)) } //nolint:forcetypeassert,gosec
	case int8:
		intf = func(i K) uint64 { return uint64(any(i).(int8)) } //nolint:forcetypeassert,gosec
	case int16:
		intf = func(i K) uint64 { return uint64(any(i).(int16)) } //nolint:forcetypeassert,gosec
	case int32:
		intf = func(i K) uint64 { return uint64(any(i).(int32)) } //nolint:forcetypeassert,gosec
	case int64:
		intf = func(i K) uint64 { return uint64(any(i).(int64)) } //nolint:forcetypeassert,gosec
	case uint:
		intf = func(i K) uint64 { return uint64(any(i).(uint)) } //nolint:forcetypeassert
	case uint8:
		intf = func(i K) uint64 { return uint64(any(i).(uint8)) } //nolint:forcetypeassert
	case uint16:
		intf = func(i K) uint64 { return uint64(any(i).(uint16)) } //nolint:forcetypeassert
	case uint32:
		intf = func(i K) uint64 { return uint64(any(i).(uint32)) } //nolint:forcetypeassert
	case uint64:
		intf = func(i K) uint64 { return any(i).(uint64) } //nolint:forcetypeassert
	case uintptr:
		intf = func(i K) uint64 { return uint64(any(i).(uintptr)) } //nolint:forcetypeassert
	}

	return func(i K) uint64 {
		return intf(i) % size
	}
}

func compactLockedMapKeys[K mapIndexType](ks []K) []K {
	n := make([]K, len(ks))
	copy(n, ks)
	slices.Sort(n)

	return slices.Compact(n)
}

func NewOnceFunc(f InErrorFunc[error]) func(error) (bool /* called */, error) {
	var once sync.Once
	var gerr error

	return func(err error) (bool, error) {
		var called bool

		once.Do(func() {
			called = true

			gerr = f(err)
		})

		return called, gerr
	}
}
