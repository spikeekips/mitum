package util

import (
	"crypto/rand"
	"math/big"

	"github.com/pkg/errors"
)

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
		keep, k := keyf(s[i])
		if _, found := m[k]; found {
			return nil, true
		}

		if !keep {
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

func RemoveDuplicatedSlice[T any](s []T, f func(T) (string, error)) ([]T, error) {
	if len(s) < 1 {
		return nil, nil
	}

	ns := make([]T, len(s))
	var index int

	m := map[string]struct{}{}

	for i := range s {
		switch k, err := f(s[i]); {
		case err != nil:
			return nil, err
		case len(k) < 1:
			continue
		default:
			_, found := m[k]
			if found {
				continue
			}

			m[k] = struct{}{}
		}

		ns[index] = s[i]
		index++
	}

	return ns[:index], nil
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

func TraverseSlice[T any](s []T, f func(int, T) error) error {
	if len(s) < 1 {
		return nil
	}

	for i := range s {
		if err := f(i, s[i]); err != nil {
			return err
		}
	}

	return nil
}

func randInt64(max int64) (int64, error) {
	if max <= 0 {
		return 0, errors.Errorf("invalid max")
	}

	b, err := rand.Int(rand.Reader, big.NewInt(max))
	if err != nil {
		return -1, errors.WithMessage(err, "random int64")
	}

	return b.Int64(), nil
}

// RandomChoiceSlice creates new n-sized slice randomly.
func RandomChoiceSlice[T any](a []T, size int64) ([]T, error) {
	asize := int64(len(a))
	csize := size

	if csize > asize {
		csize = asize
	}

	switch {
	case csize < 1:
		return nil, errors.Errorf("zero size")
	case csize == asize:
		return a, nil
	case csize == 1:
		switch i, err := randInt64(asize); {
		case err != nil:
			return nil, err
		default:
			return []T{a[i]}, nil
		}
	}

	choosed := make([]interface{}, asize)
	n := make([]T, csize)

	for i := range n {
		var c int64

	inside:
		for {
			switch j, err := randInt64(asize); {
			case err != nil:
				return nil, err
			case choosed[j] != nil:
				continue inside
			default:
				choosed[j] = struct{}{}
				c = j

				break inside
			}
		}

		n[i] = a[c]
	}

	return n, nil
}
