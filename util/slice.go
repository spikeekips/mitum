package util

func InSlice[T comparable](s []T, n T) int {
	for i := range s {
		if n == s[i] {
			return i
		}
	}

	return -1
}

func InSliceFunc[T any](s []T, f func(interface{}, int) bool) int {
	for i := range s {
		if f((interface{})(s[i]), i) {
			return i
		}
	}

	return -1
}

func CheckSliceDuplicated[T any](s []T, keyf func(interface{}, int) string) (map[string]T, bool) {
	if len(s) < 1 {
		return nil, false
	}

	m := map[string]T{}

	for i := range s {
		var k string
		if (interface{})(s[i]) == nil {
			k = ""
		} else {
			k = keyf(s[i], i)
		}

		if _, found := m[k]; found {
			return nil, true
		}

		m[k] = s[i]
	}

	return m, false
}

func FilterSlice[T any](s []T, f func(interface{}, int) bool) []T {
	if len(s) < 1 {
		return nil
	}

	ns := make([]T, len(s))
	var index int

	for i := range s {
		if !f(s[i], i) {
			continue
		}

		ns[index] = s[i]
		index++
	}

	return ns[:index]
}

func CountFilteredSlice[T any](s []T, f func(interface{}, int) bool) (n int) {
	if len(s) < 1 {
		return 0
	}

	for i := range s {
		if !f(s[i], i) {
			continue
		}

		n++
	}

	return n
}

func Filter2Slices[T any, Y any](a []T, b []Y, f func(interface{}, interface{}, int, int) bool) []T {
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
			if f(a[i], b[j], i, j) {
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
