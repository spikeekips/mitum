package main

import (
	"cmp"
	"context"
	"slices"
	"sync"

	"github.com/pkg/errors"
	"github.com/spikeekips/mitum/util"
)

func TraverseMap[K cmp.Ordered, V any](m map[K]V, f func(K, V) (bool, error)) error {
	for k := range m {
		if keep, err := f(k, m[k]); err != nil || !keep {
			return err
		}
	}

	return nil
}

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

func NewContextDoneFunc(
	ctx context.Context,
	f func(context.Context) error,
) (
	_ context.Context,
	done func() error,
	cancel func(error),
) {
	errDone := errors.New(util.UUID().String())
	cctx, cctxCancel := context.WithCancelCause(ctx)

	var once sync.Once

	return cctx,
		func() error {
			once.Do(func() {
				go func() {
					if err := f(cctx); err != nil {
						cctxCancel(err)

						return
					}

					cctxCancel(errDone)
				}()
			})

			<-cctx.Done()

			switch err := context.Cause(cctx); {
			case errors.Is(err, errDone):
				return nil
			default:
				return err
			}
		},
		cctxCancel
}
