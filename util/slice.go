package util

func InSlice[T comparable](s []T, n T) int {
	for i := range s {
		if n == s[i] {
			return i
		}
	}

	return -1
}

func InSliceFunc[T any](s []T, f func(T) bool) int {
	for i := range s {
		if f(s[i]) {
			return i
		}
	}

	return -1
}

func IsDuplicatedSlice[T any](s []T, keyf func(T) (bool, string)) (map[string]T, bool) {
	if len(s) < 1 {
		return nil, false
	}

	m := map[string]T{}

	for i := range s {
		var stop bool
		var k string

		if (interface{})(s[i]) == nil {
			k = ""
		} else {
			var keep bool

			keep, k = keyf(s[i])
			stop = !keep
		}

		if _, found := m[k]; found {
			return nil, true
		}

		if stop {
			return m, false
		}

		m[k] = s[i]
	}

	return m, false
}

func FilterMap[T any, Y comparable](s map[Y]T, f func(Y, T) bool) []T {
	if len(s) < 1 {
		return nil
	}

	ns := make([]T, len(s))
	var index int

	for i := range s {
		if !f(i, s[i]) {
			continue
		}

		ns[index] = s[i]
		index++
	}

	return ns[:index]
}

func FilterSlice[T any](s []T, f func(T) bool) []T {
	if len(s) < 1 {
		return nil
	}

	ns := make([]T, len(s))
	var index int

	for i := range s {
		if !f(s[i]) {
			continue
		}

		ns[index] = s[i]
		index++
	}

	return ns[:index]
}

func CountFilteredSlice[T any](s []T, f func(T) bool) (n int) {
	if len(s) < 1 {
		return 0
	}

	for i := range s {
		if !f(s[i]) {
			continue
		}

		n++
	}

	return n
}

func Filter2Slices[T any, Y any](a []T, b []Y, f func(T, Y) bool) []T {
	if len(a) < 1 {
		return nil
	}

	if len(b) < 1 {
		return a
	}

	nb := make([]T, len(a))

	var n int

	for i := range a {
		var found bool

		for j := range b {
			if f(a[i], b[j]) {
				found = true

				break
			}
		}

		if found {
			continue
		}

		nb[n] = a[i]
		n++
	}

	return nb[:n]
}
