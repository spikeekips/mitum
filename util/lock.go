package util

import (
	"sync"
)

type NilLockedValue struct{}

type Locked struct {
	sync.RWMutex
	value interface{}
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

	if IsNilLockedValue(l.value) {
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

func (l *Locked) Get(f func() (interface{}, error)) (interface{}, error) {
	l.Lock()
	defer l.Unlock()

	if !IsNilLockedValue(l.value) {
		return l.value, nil
	}

	switch i, err := f(); {
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

func (l *Locked) Set(f func(interface{}) (interface{}, error)) (interface{}, error) {
	l.Lock()
	defer l.Unlock()

	i := l.value
	if IsNilLockedValue(l.value) {
		i = nil
	}

	switch j, err := f(i); {
	case err != nil:
		return nil, err
	default:
		l.value = j

		return j, nil
	}
}

type LockedMap struct {
	sync.RWMutex
	m map[interface{}]interface{}
}

func NewLockedMap() *LockedMap {
	return &LockedMap{
		m: map[interface{}]interface{}{},
	}
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

func (l *LockedMap) Get(k interface{}, f func() (interface{}, error)) (interface{}, bool, error) {
	l.Lock()
	defer l.Unlock()

	if i, found := l.m[k]; found {
		return i, true, nil
	}

	switch i, err := f(); {
	case err != nil:
		return nil, false, err
	default:
		l.m[k] = i
		return i, false, nil
	}
}

func (l *LockedMap) Set(k interface{}, f func(interface{}) (interface{}, error)) (interface{}, error) {
	l.Lock()
	defer l.Unlock()

	var i interface{} = NilLockedValue{}
	if j, found := l.m[k]; found {
		i = j
	}

	switch j, err := f(i); {
	case err != nil:
		return nil, err
	default:
		l.m[k] = j

		return j, nil
	}
}

func (l *LockedMap) Remove(k interface{}, f func(interface{}) error) error {
	l.Lock()
	defer l.Unlock()

	if f != nil {
		var i interface{} = NilLockedValue{}
		if j, found := l.m[k]; found {
			i = j
		}
		if err := f(i); err != nil {
			return err
		}
	}

	delete(l.m, k)

	return nil
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

func IsNilLockedValue(i interface{}) bool {
	_, ok := i.(NilLockedValue)

	return ok
}
