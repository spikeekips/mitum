package util

import (
	"sync"
)

type Locked struct {
	sync.RWMutex
	value interface{}
}

func NewLocked(defaultValue interface{}) *Locked {
	return &Locked{value: defaultValue}
}

func (l *Locked) Value() interface{} {
	l.RLock()
	defer l.RUnlock()

	return l.value
}

func (l *Locked) SetValue(i interface{}) *Locked {
	l.Lock()
	defer l.Unlock()

	l.value = i

	return l
}

func (l *Locked) Get(f func() (interface{}, error)) (interface{}, bool, error) {
	l.Lock()
	defer l.Unlock()

	if l.value != nil {
		return l.value, true, nil
	}

	i, err := f()
	if err != nil {
		return nil, false, err
	}

	l.value = i

	return i, false, nil
}

func (l *Locked) Set(f func(interface{}) (interface{}, error)) error {
	l.Lock()
	defer l.Unlock()

	value, err := f(l.value)
	if err != nil {
		return err
	}

	l.value = value

	return nil
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

func (l *LockedMap) Value(k interface{}) (interface{}, bool) {
	l.RLock()
	defer l.RUnlock()

	i, found := l.m[k]

	return i, found
}

func (l *LockedMap) SetValue(k interface{}, v interface{}) bool {
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

	j, err := f()
	if err != nil {
		return nil, false, err
	}

	l.m[k] = j

	return j, false, nil
}

func (l *LockedMap) Set(k interface{}, f func() (interface{}, error)) error {
	l.Lock()
	defer l.Unlock()

	j, err := f()
	if err != nil {
		return err
	}

	l.m[k] = j

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
