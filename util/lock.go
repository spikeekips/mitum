package util

import "sync"

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
