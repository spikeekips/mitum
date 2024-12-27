package util

import (
	"sync"

	"github.com/pkg/errors"
	"github.com/spikeekips/mitum/util"
)

func TraverseSlice[T any](s []T, f func(int, T) (bool, error)) error {
	if len(s) < 1 {
		return nil
	}

	for i := range s {
		switch keep, err := f(i, s[i]); {
		case err != nil:
			return err
		case !keep:
			return nil
		}
	}

	return nil
}

func TraverseMap[K comparable, V any](m map[K]V, f func(K, V) (bool, error)) error {
	for k := range m {
		if keep, err := f(k, m[k]); err != nil || !keep {
			return err
		}
	}

	return nil
}

type (
	AppendSliceAddFunc[T any]  func(T, ...T) (added, left uint64)
	AppendSliceDoneFunc[T any] func(func([]T) error) error
)

func AppendSlice[T any](size, grow uint64) (
	add AppendSliceAddFunc[T],
	done AppendSliceDoneFunc[T],
	_ error,
) {
	if size < 1 {
		if grow < 1 {
			return nil, nil, errors.Errorf("size and grow under 1")
		}

		size = grow
	}

	s := make([]T, size)
	lastSize := size

	growf := func(uint64) bool { return false }

	if grow > 0 {
		growf = func(n uint64) bool {
			if n < grow {
				n = grow
			}

			i := make([]T, uint64(len(s))+n)
			copy(i, s)

			s = i
			lastSize = uint64(len(s))

			return true
		}
	}

	var start uint64
	var lock sync.RWMutex

	count := func(n uint64) (_ uint64, prevStart uint64, afterStart uint64, afterLastSize uint64) {
		if n+start > lastSize && !growf(n+start-lastSize) {
			n = lastSize - start
		}

		if n < 1 {
			return 0, 0, start, lastSize
		}

		prevStart = start
		start += n

		return n, prevStart, start, lastSize
	}

	insert := func(t T, ts []T) (_ uint64, afterStart uint64, afterLastSize uint64) {
		lock.Lock()
		defer lock.Unlock()

		n, si, st, l := count(uint64(len(ts) + 1))
		if n < 1 {
			return n, st, l
		}

		s[si] = t

		for i := range ts[:n-1] {
			s[si+uint64(i+1)] = ts[i]
		}

		return n, st, l
	}

	return func(t T, ts ...T) (uint64, uint64) {
			n, st, l := insert(t, ts)
			if n < 1 {
				return 0, l - st
			}

			left := l - st
			if left < 1 && grow > 0 {
				left = grow
			}

			return n, left
		},
		func(f func([]T) error) error {
			lock.Lock()

			defer func() {
				clear(s)
				s = nil
				start = 0
				lastSize = 0

				lock.Unlock()
			}()

			if f == nil {
				return nil
			}

			if start < 1 {
				return f(nil)
			}

			return f(s[:start])
		},
		nil
}

func UniqueAppendSlice[T any, K comparable](size, grow uint64, key func(T) K) (
	AppendSliceAddFunc[T],
	AppendSliceDoneFunc[T],
	error,
) {
	add, done, err := AppendSlice[T](size, grow)
	if err != nil {
		return nil, nil, err
	}

	m := map[K]struct{}{}

	filterOne := func(a T) bool {
		k := key(a)

		if _, found := m[k]; found {
			return false
		}

		m[k] = struct{}{}

		return true
	}

	var lock sync.RWMutex

	filter := func(t T, ts []T) []T {
		lock.Lock()
		defer lock.Unlock()

		as := make([]T, len(ts)+1)

		var n int

		if filterOne(t) {
			as[n] = t
			n++
		}

		for i := range ts {
			if filterOne(ts[i]) {
				as[n] = ts[i]
				n++
			}
		}

		return as[:n]
	}

	return func(t T, ts ...T) (uint64, uint64) {
			n := filter(t, ts)
			if len(n) < 1 {
				return 0, 0
			}

			return add(n[0], n[1:]...)
		}, func(f func([]T) error) error {
			lock.Lock()
			defer lock.Unlock()

			ClearMap(m)

			return done(f)
		}, nil
}

func RAppendSlice[T any](
	size,
	grow uint64,
	add, done interface{},
) error {
	switch a, d, err := AppendSlice[T](size, grow); {
	case err != nil:
		return err
	default:
		if err := util.ReflectSetInterfaceValue(a, add); err != nil {
			return err
		}

		return util.ReflectSetInterfaceValue(d, done)
	}
}

func RUniqueAppendSlice[T any, K comparable](
	size,
	grow uint64,
	key func(T) K,
	add, done interface{},
) error {
	switch a, d, err := UniqueAppendSlice[T](size, grow, key); {
	case err != nil:
		return err
	default:
		if err := util.ReflectSetInterfaceValue(a, add); err != nil {
			return err
		}

		return util.ReflectSetInterfaceValue(d, done)
	}
}

func ClearMap[K comparable, V any](m map[K]V) {
	if len(m) < 1 {
		return
	}

	clear(m)
	m = nil
}
