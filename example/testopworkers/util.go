package testopsworkers

import "cmp"

func CompactAddSlice[T any](size int) (
	add func(T),
	done func() []T,
) {
	s := make([]T, size)

	var i int

	return func(t T) {
			s[i] = t

			i++
		}, func() []T {
			return s[:i]
		}
}

func UniqCompactAddSlice[T cmp.Ordered](size int) (
	add func(T),
	done func() []T,
) {
	s := make([]T, size)

	var i int

	m := map[T]struct{}{}
	defer clear(m)

	return func(t T) {
			if _, found := m[t]; found {
				return
			}

			s[i] = t
			m[t] = struct{}{}

			i++
		}, func() []T {
			return s[:i]
		}
}
