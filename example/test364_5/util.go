package main

import (
	"context"
	stderrors "errors"
	"sync"
	"time"

	"github.com/pkg/errors"
	"github.com/rs/zerolog"
	"github.com/spikeekips/mitum/util"
)

const contextLoggerKey = "context-logger"

var nopLogger = zerolog.Nop()

func setContextValueLogger(ctx context.Context, v *zerolog.Logger) context.Context {
	return context.WithValue(ctx, contextLoggerKey, v)
}

func loadContextValueLogger(ctx context.Context) *zerolog.Logger {
	switch t := ctx.Value(contextLoggerKey).(type) {
	case *zerolog.Logger:
		return t
	default:
		return &nopLogger
	}
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

func traverseMap[K comparable, V any](m map[K]V, f func(K, V) (bool, error)) error {
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

func uniqueCompactAppendSlice[T comparable](size, grow uint64) (
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

			clearMap(m)

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

func rUniqueCompactAppendSlice[T comparable](
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

func newModuleLogger(logger *zerolog.Logger, module string, f func(zctx zerolog.Context) zerolog.Context) *zerolog.Logger {
	if f == nil {
		f = func(zctx zerolog.Context) zerolog.Context { return zctx }
	}

	zctx := logger.With()

	zctx = zctx.Str("module", module)
	if f != nil {
		zctx = f(zctx)
	}

	l := zctx.Logger()

	return &l
}

func newModuleLoggerFromContext(ctx context.Context, module string, f func(zctx zerolog.Context) zerolog.Context) *zerolog.Logger {
	logger := loadContextValueLogger(ctx)
	if logger == nil {
		return &nopLogger
	}

	return newModuleLogger(logger, module, f)
}

type (
	inErrorFunc[T any] func(T) error
	errorFunc          func() error
)

func dummyErrorInFunc(err error) error {
	return err
}

func ifErrErrorInFunc(f inErrorFunc[error]) inErrorFunc[error] {
	return func(err error) error {
		if err != nil {
			return f(err)
		}

		return nil
	}
}

func contextWithCancelCause(ctx context.Context) (context.Context, inErrorFunc[error]) {
	cctx, cancel := context.WithCancelCause(ctx)

	var called bool
	var gerr error
	var once sync.Once

	return cctx, func(err error) error {
		if cerr := context.Cause(cctx); cerr != nil {
			return errors.WithStack(cerr)
		}

		once.Do(func() {
			switch {
			case err == nil:
				gerr = context.Canceled

				cancel(nil)
			default:
				gerr = stderrors.Join(err, context.Canceled)

				cancel(gerr)
			}

			called = true
		})

		if err == nil && called {
			return nil
		}

		return gerr
	}
}

func contextCauseError(ctx context.Context, ef inErrorFunc[error], f inErrorFunc[context.Context]) error {
	if err := context.Cause(ctx); err != nil {
		return errors.WithStack(err)
	}

	return ef(f(ctx))
}

type emptyGCache[K, V any] struct{}

func newEmptyGCache[K, V any]() emptyGCache[K, V] { return emptyGCache[K, V]{} }

func (emptyGCache[K, V]) Exists(K) bool            { return false }
func (emptyGCache[K, V]) Get(K) (v V, _ bool)      { return v, false }
func (emptyGCache[K, V]) Set(K, V, time.Duration)  {}
func (emptyGCache[K, V]) Remove(K) bool            { return false }
func (emptyGCache[K, V]) Close()                   {}
func (emptyGCache[K, V]) Purge()                   {}
func (emptyGCache[K, V]) Traverse(func(K, V) bool) {}

func newDoneContext(ctx context.Context) (
	context.Context,
	inErrorFunc[error],
	func() error,
) {
	errDone := errors.New(util.UUID().String())
	cctx, cctxCancel := context.WithCancelCause(ctx)

	var once sync.Once

	return cctx, func(err error) error {
			once.Do(func() {
				if err == nil {
					cctxCancel(errDone)

					return
				}

				cctxCancel(err)
			})

			return err
		},
		func() error {
			<-cctx.Done()

			switch err := context.Cause(cctx); {
			case errors.Is(err, errDone):
				return nil
			default:
				return err
			}
		}
}

func clearMap[K comparable, V any](m map[K]V) {
	clear(m)
	m = nil
}
