package util

import (
	"iter"
	"sync"

	"github.com/pkg/errors"
)

func FilterSlice[T any](s []T, f func(T) bool) iter.Seq[T] {
	return func(yield func(T) bool) {
	end:
		for i := range s {
			switch {
			case !f(s[i]):
				continue end
			case !yield(s[i]):
				break end
			}
		}
	}
}

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

type AppendSliceAddFunc[T any] func(...T) (added, left uint64)

func TrueFilter[T any](T) bool { return true }

func newBaseAppendSlice[T any](size, grow uint64, filter func(T) bool) (
	add AppendSliceAddFunc[T],
	done OutFunc[[]T],
	_ error,
) {
	if size < 1 && grow < 1 {
		return nil, nil, errors.Errorf("size and grow under 1")
	}

	var s []T

	slen := func() uint64 {
		return uint64(len(s))
	}

	var next uint64

	var growf func() bool

	switch {
	case grow < 1:
		growf = func() bool {
			if size > 0 && s == nil {
				s = make([]T, size)

				return true
			}

			return false
		}
	default:
		growf = func() bool {
			i := make([]T, slen()+grow)
			copy(i, s)
			s = i

			return true
		}
	}

	var closed bool

	return func(ts ...T) (added, left uint64) {
			if closed {
				return 0, 0
			}

		end:
			for i := range ts {
				if !filter(ts[i]) {
					continue end
				}

				if next == slen() {
					switch {
					case size > 0 && s == nil:
						s = make([]T, size)
					case !growf():
						break end
					}
				}

				s[next] = ts[i]
				next++
				added++
			}

			switch {
			case s == nil:
				left = size - next
			default:
				left = slen() - next
			}

			return added, left
		},
		func() []T {
			if closed {
				return s
			}

			defer func() {
				closed = true
			}()

			if next < 1 {
				return nil
			}

			s = s[:next]

			return s
		},
		nil
}

func NewAppendSlice[T any](size, grow uint64) (
	AppendSliceAddFunc[T],
	OutFunc[[]T],
	error,
) {
	return newBaseAppendSlice[T](size, grow, TrueFilter)
}

func NewUniqueAppendSlice[T any, K comparable](size, grow uint64, keyf func(T) K) (
	AppendSliceAddFunc[T],
	OutFunc[[]T],
	error,
) {
	var m map[K]struct{}

	add, done, err := newBaseAppendSlice[T](size, grow, func(i T) bool {
		k := keyf(i)

		if m == nil {
			m = map[K]struct{}{k: {}}

			return true
		}

		switch _, found := m[k]; {
		case found:
			return false
		default:
			m[k] = struct{}{}

			return true
		}
	})
	if err != nil {
		return nil, nil, err
	}

	return add,
		func() []T {
			defer clear(m)

			return done()
		},
		nil
}

func NewComparableUniqueAppendSlice[T comparable](size, grow uint64) (
	AppendSliceAddFunc[T],
	OutFunc[[]T],
	error,
) {
	return NewUniqueAppendSlice[T, T](size, grow, func(i T) T { return i })
}

func NewLockAppendSlice[T any](size, grow uint64) (
	AppendSliceAddFunc[T],
	OutFunc[[]T],
	error,
) {
	add, done, err := newBaseAppendSlice[T](size, grow, TrueFilter)
	if err != nil {
		return nil, nil, err
	}

	var l sync.Mutex

	return func(ts ...T) (added, left uint64) {
			l.Lock()
			defer l.Unlock()

			return add(ts...)
		},
		func() []T {
			l.Lock()
			defer l.Unlock()

			return done()
		},
		nil
}

func NewLockUniqueAppendSlice[T any, K comparable](size, grow uint64, keyf func(T) K) (
	AppendSliceAddFunc[T],
	OutFunc[[]T],
	error,
) {
	add, done, err := NewUniqueAppendSlice[T](size, grow, keyf)
	if err != nil {
		return nil, nil, err
	}

	var l sync.Mutex

	return func(ts ...T) (added, left uint64) {
			l.Lock()
			defer l.Unlock()

			return add(ts...)
		},
		func() []T {
			l.Lock()
			defer l.Unlock()

			return done()
		},
		nil
}

func NewLockComparableUniqueAppendSlice[T comparable](size, grow uint64) (
	AppendSliceAddFunc[T],
	OutFunc[[]T],
	error,
) {
	return NewLockUniqueAppendSlice[T, T](size, grow, func(i T) T { return i })
}

func ClearSlice[T any](s []T) {
	clear(s)
	s = s[:0]
}

func ClearMap[K comparable, V any](m map[K]V) {
	if len(m) < 1 {
		return
	}

	clear(m)
	m = nil
}
