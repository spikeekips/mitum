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
	"github.com/spikeekips/mitum/util/logging"
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

func newDoneContextFunc(ctx context.Context) (
	context.Context,
	errorFunc,
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

func NewFuncContextDoneFunc(
	ctx context.Context,
	f func(context.Context) error,
) func() error {
	cctx, done, wait := newDoneContextFunc(ctx)

	var once sync.Once

	return func() error {
		once.Do(func() {
			_ = done(f(cctx))
		})

		return wait()
	}
}

func newDjb2Seed() uint32 {
	maxv := big.NewInt(0).SetUint64(uint64(math.MaxUint32))

	switch rnd, err := rand.Int(rand.Reader, maxv); {
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

func funcErrorLogger(l *zerolog.Logger, format string, a ...any) errorFunc {
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

func funcErrorLog(e *zerolog.Event, format string, a ...any) errorFunc {
	started := time.Now()

	msg := fmt.Sprintf(format, a...)
	e.CallerSkipFrame(1).Msg("> " + msg)

	return func(err error) error {
		e.CallerSkipFrame(1).Dur("elapsed", time.Since(started))

		if err != nil {
			e.Err(err)
		}

		e.Msg(fmt.Sprintf("< %s", msg))

		return err
	}
}

func OnceFunc(f func() error) func() error {
	var gerr error

	var once sync.Once

	return func() error {
		once.Do(func() {
			gerr = f()
		})

		return gerr
	}
}

func OnceFuncCallback[T any](f func(T) error) func(T) error {
	var gerr error
	var w T

	var once sync.Once

	return func(t T) error {
		once.Do(func() {
			w = t

			gerr = f(w)
		})

		return gerr
	}
}

func contextWithCancelCause(ctx context.Context) (context.Context, errorFunc) {
	cctx, cancel := context.WithCancelCause(ctx)

	var called bool
	var gerr error
	var once sync.Once

	return cctx, func(err error) error {
		if cerr := context.Cause(cctx); cerr != nil {
			return cerr
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

type errorFunc func(error) error

func dummyErrorFunc() errorFunc {
	return func(err error) error {
		return err
	}
}

func ifErrErrorFunc(f errorFunc) errorFunc {
	return func(err error) error {
		if err != nil {
			return f(err)
		}

		return err
	}
}

func contextCauseError(ctx context.Context, ef errorFunc, f func(context.Context) error) error {
	if err := context.Cause(ctx); err != nil {
		return errors.WithStack(err)
	}

	return ef(f(ctx))
}

func dummyContextErrorFunc(ctx context.Context, ef errorFunc) errorFunc {
	return func(err error) error {
		if cerr := context.Cause(ctx); cerr != nil {
			return errors.WithStack(cerr)
		}

		return ef(err)
	}
}

func contextCauseErrorWithNewJob(ctx context.Context, cancelf errorFunc, newJob func(ContextWorkerCallback) error,
	e func(context.Context) error,
) error {
	return contextCauseError(ctx, cancelf, func(context.Context) error {
		return newJob(func(ctx context.Context) error {
			return e(ctx)
		})
	})
}

type (
	checkEnoughFailedWorkerFunc   func(worker string, _ func() error) (ok, enough, failed bool, _ error)
	checkEnoughFailedWorkersFuncs func(minEnough, maxFailed uint64) (checkEnoughFailedWorkerFunc, func(), error)
)

func minEnoughMaxFailed(workers, share uint64) (minEnough, maxFailed, newShare uint64, _ error) {
	if share < 1 {
		return 0, 0, 0, util.ErrNotFound.Errorf("zero share")
	}

	if share > workers {
		share = workers
	}

	return workers - share + 1, share, share, nil
}

func newCheckEnoughFailedWorkers(
	minEnough, maxFailed uint64,
) (
	checkEnoughFailedWorkerFunc,
	func(),
	error,
) {
	if minEnough < 1 {
		return nil, nil, errors.Errorf("zero min enough")
	}

	var m util.LockedMap[string, struct{}]

	switch i, err := util.NewLockedMap[string, struct{}](minEnough+maxFailed, nil); {
	case err != nil:
		return nil, nil, err
	default:
		m = i
	}

	count := func() func(string, error) (_, _, _ bool) {
		var gIsClosed bool
		var enoughCount *uint64
		var failedCount *uint64
		var l sync.RWMutex

		f := func() (enough, failed bool) {
			if enoughCount != nil {
				enough = *enoughCount >= minEnough
			}

			if failedCount != nil {
				failed = *failedCount >= maxFailed
			}

			if failed && enough {
				enough = false
			}

			return enough, failed
		}

		c := func() (_, _, _ bool) {
			l.RLock()
			defer l.RUnlock()

			enough, failed := f()

			return gIsClosed, enough, failed
		}

		return func(_ string, err error) (isClosed, enough, failed bool) {
			if isClosed, enough, failed = c(); isClosed {
				return isClosed, enough, failed
			}

			l.Lock()
			defer l.Unlock()

			switch {
			case err != nil:
				if failedCount == nil {
					failedCount = new(uint64)
				}

				*failedCount++
			default:
				if enoughCount == nil {
					enoughCount = new(uint64)
				}

				*enoughCount++
			}

			enough, failed = f()

			if enough || failed {
				gIsClosed = true
			}

			return false, enough, failed
		}
	}()

	return func(worker string, f func() error) (ok, enough, failed bool, gerr error) {
			var isClosed bool

			switch _, _, err := m.Set(worker, func(_ struct{}, found bool) (struct{}, error) {
				if found {
					return struct{}{}, util.ErrLockedSetIgnore
				}

				if f != nil {
					gerr = f()
				}

				switch isClosed, enough, failed = count(worker, gerr); {
				case isClosed:
					return struct{}{}, util.ErrLockedSetIgnore
				default:
					ok = true

					return struct{}{}, nil
				}
			}); {
			case errors.Is(err, util.ErrLockedMapClosed):
				return false, enough, failed, nil
			default:
				if isClosed {
					m.Close()
				}

				return ok, enough, failed, gerr
			}
		},
		m.Close,
		nil
}

func LoggingWith(l *logging.Logging, f func(zerolog.Context) zerolog.Context) *logging.Logging {
	return logging.NewLogging(f).SetLogging(l)
}

func NewModuleLogging(module string, f func(zctx zerolog.Context) zerolog.Context) *logging.Logging {
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
