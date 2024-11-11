package main

import (
	"cmp"
	"context"
	"sync"
	"time"

	"github.com/pkg/errors"
	"github.com/rs/zerolog"
	"github.com/spikeekips/mitum/util"
	"github.com/spikeekips/mitum/util/logging"
)

const contextLoggerKey = "context-logger"

var nopLogger = zerolog.Nop()

func setContextValueLogger(ctx context.Context, v *zerolog.Logger) context.Context {
	return context.WithValue(ctx, contextLoggerKey, v)
}

func loadContextValueLogger(ctx context.Context, v **zerolog.Logger) bool {
	if i := ctx.Value(contextLoggerKey); i != nil {
		if util.SetInterfaceValue(i, v) == nil {
			return true
		}
	}

	_ = util.SetInterfaceValue(&nopLogger, v)

	return false
}

func traverseSlice[T any](s []T, f func(int, T) (bool, error)) error {
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

func traverseMap[K cmp.Ordered, V any](m map[K]V, f func(K, V) (bool, error)) error {
	for k := range m {
		if keep, err := f(k, m[k]); err != nil || !keep {
			return err
		}
	}

	return nil
}

type (
	compactAppendSliceAddFunc[T any]  func(T, ...T) (added, left uint64)
	compactAppendSliceDoneFunc[T any] func(func([]T) error) error
)

func compactAppendSlice[T any](size, grow uint64) (
	add compactAppendSliceAddFunc[T],
	done compactAppendSliceDoneFunc[T],
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

func uniqueCompactAppendSlice[T cmp.Ordered](size, grow uint64) (
	compactAppendSliceAddFunc[T],
	compactAppendSliceDoneFunc[T],
	error,
) {
	add, done, err := compactAppendSlice[T](size, grow)
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

func rCompactAppendSlice[T any](
	size,
	grow uint64,
	add, done interface{},
) error {
	switch a, d, err := compactAppendSlice[T](size, grow); {
	case err != nil:
		return err
	default:
		if err := util.ReflectSetInterfaceValue(a, add); err != nil {
			return err
		}

		return util.ReflectSetInterfaceValue(d, done)
	}
}

func rUniqueCompactAppendSlice[T cmp.Ordered](
	size,
	grow uint64,
	add, done interface{},
) error {
	switch a, d, err := uniqueCompactAppendSlice[T](size, grow); {
	case err != nil:
		return err
	default:
		if err := util.ReflectSetInterfaceValue(a, add); err != nil {
			return err
		}

		return util.ReflectSetInterfaceValue(d, done)
	}
}

func newModuleLogging(module string, f func(zctx zerolog.Context) zerolog.Context) *logging.Logging {
	if f == nil {
		f = func(zctx zerolog.Context) zerolog.Context { return zctx }
	}

	return logging.NewLogging(func(zctx zerolog.Context) zerolog.Context {
		zctx = zctx.Str("module", module)

		if f == nil {
			return f(zctx)
		}

		return zctx
	})
}

func onceFunc(f func() error) func() error {
	var gerr error

	var once sync.Once

	return func() error {
		once.Do(func() {
			gerr = f()
		})

		return gerr
	}
}

type errorFunc func(error) error

func dummyErrorFunc(err error) error {
	return err
}

func ifErrErrorFunc(f errorFunc) errorFunc {
	return func(err error) error {
		if err != nil {
			return f(err)
		}

		return nil
	}
}

func contextWithCancelCause(ctx context.Context) (context.Context, errorFunc) {
	cctx, cancel := context.WithCancelCause(ctx)

	var called bool
	var gerr error
	var once sync.Once

	return cctx, func(err error) error {
		if cerr := context.Cause(cctx); cerr != nil {
			return errors.WithStack(cerr)
		}

		once.Do(func() {
			cancel(err)

			switch {
			case err == nil:
				gerr = context.Canceled
			default:
				gerr = err
			}

			called = true
		})

		if err == nil && called {
			return nil
		}

		return gerr
	}
}

func contextCauseError(ctx context.Context, ef errorFunc, f func(context.Context) error) error {
	if err := context.Cause(ctx); err != nil {
		return errors.WithStack(err)
	}

	return ef(f(ctx))
}

type emptyGCache[K cmp.Ordered, V any] struct{}

func newEmptyGCache[K cmp.Ordered, V any]() emptyGCache[K, V] { return emptyGCache[K, V]{} }

func (emptyGCache[K, V]) Exists(K) bool            { return false }
func (emptyGCache[K, V]) Get(K) (v V, _ bool)      { return v, false }
func (emptyGCache[K, V]) Set(K, V, time.Duration)  {}
func (emptyGCache[K, V]) Remove(K) bool            { return false }
func (emptyGCache[K, V]) Close()                   {}
func (emptyGCache[K, V]) Purge()                   {}
func (emptyGCache[K, V]) Traverse(func(K, V) bool) {}
