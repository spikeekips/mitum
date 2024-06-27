package main

import (
	"cmp"
	"context"
	"crypto/rand"
	"fmt"
	"math"
	"math/big"
	mathrand "math/rand"
	"slices"
	"sync"
	"time"

	"github.com/pkg/errors"
	"github.com/rs/zerolog"
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
	done func() error,
) {
	errDone := errors.New(util.UUID().String())
	cctx, cctxCancel := context.WithCancelCause(ctx)

	var once sync.Once

	return func() error {
		defer cctxCancel(nil)

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
	}
}

func newDjb2Seed() uint32 {
	max := big.NewInt(0).SetUint64(uint64(math.MaxUint32))

	switch rnd, err := rand.Int(rand.Reader, max); {
	case err != nil:
		return mathrand.Uint32()
	default:
		return uint32(rnd.Uint64())
	}
}

// stringdjb2 derived from
// https://github.com/patrickmn/go-cache/blob/master/sharded.go .
func stringdjb2(seed uint32, k string) uint64 {
	var (
		l = uint32(len(k))
		d = 5381 + seed + l
		i = uint32(0)
	)

	if l >= 4 { //nolint:gomnd //...
		for i < l-4 {
			d = (d * 33) ^ uint32(k[i])   //nolint:gomnd //...
			d = (d * 33) ^ uint32(k[i+1]) //nolint:gomnd //...
			d = (d * 33) ^ uint32(k[i+2]) //nolint:gomnd //...
			d = (d * 33) ^ uint32(k[i+3]) //nolint:gomnd //...
			i += 4
		}
	}

	switch l - i {
	case 1:
	case 2:
		d = (d * 33) ^ uint32(k[i]) //nolint:gomnd //...
	case 3: //nolint:gomnd //...
		d = (d * 33) ^ uint32(k[i])   //nolint:gomnd //...
		d = (d * 33) ^ uint32(k[i+1]) //nolint:gomnd //...
	case 4: //nolint:gomnd //...
		d = (d * 33) ^ uint32(k[i])   //nolint:gomnd //...
		d = (d * 33) ^ uint32(k[i+1]) //nolint:gomnd //...
		d = (d * 33) ^ uint32(k[i+2]) //nolint:gomnd //...
	}

	return uint64(d ^ (d >> 16)) //nolint:gomnd //...
}

func FuncErrorLog(l *zerolog.Logger, format string, a ...any) func(error) error {
	started := time.Now()

	msg := fmt.Sprintf(format, a...)
	l.Debug().CallerSkipFrame(1).Msg("> " + msg)

	return func(err error) error {
		ll := l.With().Dur("elapsed", time.Since(started)).Logger()

		e := ll.Debug().CallerSkipFrame(1)

		if err != nil {
			e = ll.Error().Err(err)
		}

		e.Msg(fmt.Sprintf("< %s", msg))

		return err
	}
}
