package main

import (
	"cmp"
	"slices"
	"sync"

	"github.com/spikeekips/mitum/util"
)

func ConcatSlice[S ~[]E, E any](s ...S) S {
	size := 0
	for _, s := range s {
		size += len(s)
		if size < 0 {
			panic("len out of range")
		}
	}
	newslice := slices.Grow[S](nil, size)
	for _, s := range s {
		newslice = append(newslice, s...)
	}
	return newslice
}

func UniqueCompactAppendSliceV1[T cmp.Ordered](size uint64, grow ...uint64) (
	util.CompactAppendSliceAddFunc[T],
	util.CompactAppendSliceDoneFunc[T],
	error,
) {
	add, done, err := util.CompactAppendSliceV1[T](size, grow...)
	if err != nil {
		return nil, nil, err
	}

	m := map[T]struct{}{}

	filterOne := func(a T) bool {
		if _, found := m[a]; found {
			return false
		}

		m[a] = struct{}{}

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

			clear(m)

			return done(f)
		}, nil
}
