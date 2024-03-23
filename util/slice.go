package util

import (
	"cmp"
	"crypto/rand"
	"math/big"
	"sort"
	"sync"

	"github.com/pkg/errors"
)

func IsDuplicatedSlice[T any](s []T, keyf func(T) (bool, string)) bool {
	m, isdup := IsDuplicatedSliceWithMap(s, keyf)
	clear(m)

	return isdup
}

func IsDuplicatedSliceWithMap[T any](s []T, keyf func(T) (bool, string)) (map[string]T, bool) {
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

	add, done := CompactAppendSlice[T](len(s))

	for i := range s {
		if !f(i, s[i]) {
			continue
		}

		add(s[i])
	}

	return done()
}

func FilterSlice[T any](s []T, f func(T) bool) []T {
	if len(s) < 1 {
		return nil
	}

	add, done := CompactAppendSlice[T](len(s))

	for i := range s {
		if !f(s[i]) {
			continue
		}

		add(s[i])
	}

	return done()
}

func RemoveDuplicatedSlice[T any](s []T, f func(T) (string, error)) ([]T, error) {
	if len(s) < 1 {
		return nil, nil
	}

	add, done := CompactAppendSlice[T](len(s))

	m := map[string]struct{}{}
	defer clear(m)

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

		add(s[i])
	}

	return done(), nil
}

func RemoveDuplicatedOrderedSlice[T cmp.Ordered](keys []T) []T {
	if len(keys) < 1 {
		return nil
	}

	ns := make([]T, len(keys))
	var index int

	m := map[T]struct{}{}
	defer clear(m)

	for i := range keys {
		k := keys[i]

		_, found := m[k]
		if found {
			continue
		}

		m[k] = struct{}{}

		ns[index] = k
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

	add, done := CompactAppendSlice[T](len(a))

	for i := range a {
		var found bool

		for j := range b {
			if f(a[i], b[j]) {
				found = true

				break
			}
		}

		if !found {
			add(a[i])
		}
	}

	return done()
}

func TraverseSlice[T any](s []T, f func(int, T) error) error { // FIXME use TraverseSliceV1
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

func TraverseSliceV1[T any](s []T, f func(int, T) (bool, error)) error {
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

func randInt64(max int64) (int64, error) {
	if max <= 0 {
		return 0, errors.Errorf("invalid max")
	}

	b, err := rand.Int(rand.Reader, big.NewInt(maxv))
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

func CompactAppendSlice[T any](size int) (
	add func(...T) (isFull bool),
	done func() []T,
) {
	s := make([]T, size)

	var lock sync.RWMutex

	var i int

	index := func() int {
		lock.RLock()
		defer lock.RUnlock()

		return i
	}

	count := func() (bool, int) {
		lock.Lock()
		defer lock.Unlock()

		if i >= size {
			return true, i
		}

		j := i
		i++

		return false, j
	}

	addOne := func(t T) bool {
		if size < 1 {
			return true
		}

		switch isOver, i := count(); {
		case isOver:
			return true
		default:
			s[i] = t

			// FIXME test
			return i == size-1
		}
	}

	return func(ts ...T) bool {
			for i := range ts {
				if addOne(ts[i]) {
					return true
				}
			}

			return false
		}, func() []T {
			if size < 1 {
				return nil
			}

			switch i := index(); {
			case i < 1:
				clear(s)

				return nil
			default:
				return s[:i]
			}
		}
}

func SortCompactAppendSlice[T any](size int, less func(T, T) bool) (
	add func(...T) bool,
	done func() []T,
) {
	addf, donef := CompactAppendSlice[T](size)

	return addf, func() []T {
		if size < 1 {
			return nil
		}

		d := donef()
		if len(d) < 1 {
			clear(d)

			return nil
		}

		sort.Slice(d, func(i, j int) bool {
			return less(d[i], d[j])
		})

		return d
	}
}

func UniqCompactAppendSlice[T cmp.Ordered](size int) (
	add func(...T) bool,
	done func() []T,
) {
	addf, done := CompactAppendSlice[T](size)

	m := map[T]struct{}{}
	defer clear(m)

	return func(ts ...T) bool {
		n := FilterSlice(ts, func(i T) bool {
			_, found := m[i]

			return !found
		})
		if len(n) < 1 {
			return false
		}

		if addf(n...) {
			return true
		}

		for i := range n {
			m[n[i]] = struct{}{}
		}

		return false
	}, done
}

func SortUniqCompactAppendSlice[T cmp.Ordered](size int, less func(T, T) bool) (
	add func(...T) bool,
	done func() []T,
) {
	addf, donef := UniqCompactAppendSlice[T](size)

	return addf, func() []T {
		if size < 1 {
			return nil
		}

		d := donef()
		if len(d) < 1 {
			clear(d)

			return nil
		}

		sort.Slice(d, func(i, j int) bool {
			return less(d[i], d[j])
		})

		return d
	}
}
