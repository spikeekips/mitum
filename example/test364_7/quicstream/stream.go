package quicstream

import (
	"context"
	"fmt"
	"io"
	"runtime"
	"sync"
	"sync/atomic"
	"time"

	"github.com/pkg/errors"
	dutil "github.com/spikeekips/mitum/example/test364_5/util"
	"github.com/spikeekips/mitum/example/test364_7/frame"
	"github.com/spikeekips/mitum/util"
	"golang.org/x/sync/semaphore"
)

type subStreamsRequestUID [16]byte

type (
	subStreamHandlerkFunc             = func(context.Context, uint64, Stream) error
	openStreamFunc                    = func(context.Context) (Stream, error)
	requestSubStreamCallbackFunc      = func(uint64) (subStreamHandlerkFunc, error)
	requestSubStreamsFunc             = func(_ context.Context, controlStream Stream, n uint64, _ requestSubStreamCallbackFunc) error
	requestSubStreamsAcceptStreamFunc = func() (subStreamsRequestUID, Stream, error)
	subStremasPool                    = *dutil.SingleLockedMap[subStreamsRequestUID, dutil.In2ErrorFunc[context.Context, Stream]]
)

func defaultRequestSubStreams(
	ctx context.Context,
	connf func() (context.Context, openStreamFunc),
	acceptTimeout time.Duration,
) (context.Context, requestSubStreamsFunc, dutil.InErrorFunc[error]) {
	pool := dutil.NewSingleLockedMap[subStreamsRequestUID, dutil.In2ErrorFunc[context.Context, Stream]]()

	return newRequestSubStreamsWithPool(ctx, connf, acceptTimeout, int64(runtime.GOMAXPROCS(0)), pool)
}

func newRequestSubStreamsWithPool(
	ctx context.Context,
	connf func() (context.Context, openStreamFunc),
	acceptTimeout time.Duration,
	workSize int64,
	pool subStremasPool,
) (context.Context, requestSubStreamsFunc, dutil.InErrorFunc[error]) {
	log := dutil.LoadContextValueLogger(ctx)

	cctx, acceptStream := connf()

	pctx, pctxCancel := joinContextWithCancelCause(ctx, cctx)

	var cancelOnce sync.Once
	cancel := func(err error) error {
		cancelOnce.Do(func() {
			_ = pctxCancel(err)

			pool.Close()
		})

		return err
	}

	go func() {
		err := cancel(acceptStreams(pctx, acceptStream, acceptTimeout, workSize, pool))

		log.Trace().Err(err).Msg("request sub streams stopped")
	}()

	return pctx,
		func(ctx context.Context, cstream Stream, n uint64, f requestSubStreamCallbackFunc) error {
			nctx := joinContext(pctx, ctx)

			var uid subStreamsRequestUID
			var nn uint64

			if err := contextEnsure(nctx, func(context.Context) error {
				switch u, i, err := initialRequestSubStreams(ctx, cstream, n); {
				case err != nil:
					return err
				default:
					uid = u
					nn = i

					return nil
				}
			}); err != nil {
				return err
			}

			var done dutil.InErrorFunc[error]
			var wait dutil.ErrorFunc

			switch d, w, err := doRequestSubStreamHandlers(nctx, pool, cstream, f, nn, uid); {
			case err != nil, nn < 1:
				return writeSubStreamError(cstream, err)
			default:
				done = d
				wait = w
			}

			serrch := make(chan error, 1)
			go func() {
				var gerr error

				switch serr, err := readSubStreamError(cstream); {
				case err != nil:
					gerr = err
				default:
					gerr = serr
				}

				if gerr != nil {
					_ = done(gerr)
				}

				serrch <- gerr
			}()

			if err := writeSubStreamError(cstream, wait()); err != nil {
				return err
			}

			select {
			case <-nctx.Done():
				return contextCause(nctx)
			case err := <-serrch:
				return err
			}
		},
		cancel
}

func acceptSubStream(ctx context.Context, accept openStreamFunc, timeout time.Duration) (
	requestSubStreamsAcceptStreamFunc,
	error,
) {
	tctx, tctxCancel := context.WithTimeout(ctx, timeout)
	defer tctxCancel()

	switch stream, err := accept(tctx); {
	case err != nil:
		return nil, nil
	default:
		return func() (subStreamsRequestUID, Stream, error) {
				uid, err := readSubStreamRequestUID(stream)
				if err != nil {
					if err := closeStream(stream, err); err != nil {
						return uid, nil, err
					}

					return uid, nil, err
				}

				return uid, stream, nil
			},
			nil
	}
}

func acceptStreams(
	ctx context.Context,
	acceptStream openStreamFunc,
	acceptTimeout time.Duration,
	workSize int64,
	pool subStremasPool,
) error {
	doHandler := acceptStreamHandlers(ctx, workSize, pool)

	for {
		if ctx.Err() != nil {
			return contextCause(ctx)
		}

		switch f, err := acceptSubStream(ctx, acceptStream, acceptTimeout); {
		case err != nil:
			return errors.WithMessage(err, "accept sub stream")
		case f == nil:
		default:
			if err := doHandler(f); err != nil {
				return errors.WithMessage(err, "sem acquire")
			}
		}
	}
}

func initialRequestSubStreams(ctx context.Context, cstream Stream, n uint64) (uid subStreamsRequestUID, _ uint64, _ error) {
	copy(uid[:], util.UUID().Bytes())

	if _, err := frame.WriteBytes(cstream, uid[:]); err != nil {
		return uid, 0, err
	}

	if _, err := frame.WriteUint64(cstream, n); err != nil {
		return uid, 0, err
	}

	switch nn, _, err := frame.ReadUint64(cstream); {
	case err != nil:
		return uid, 0, err
	case nn < 1:
		return uid, 0, nil
	default:
		return uid, nn, nil
	}
}

func doRequestSubStreamHandlers(
	ctx context.Context,
	pool subStremasPool,
	cstream Stream,
	f requestSubStreamCallbackFunc,
	n uint64,
	uid subStreamsRequestUID,
) (dutil.InErrorFunc[error], dutil.ErrorFunc, error) {
	var rf subStreamHandlerkFunc

	if f != nil {
		switch i, err := f(n); {
		case err != nil:
			return nil, nil, err
		case n > 0:
			rf = i
		}
	}

	if n < 1 {
		return func(err error) error { return err },
			func() error { return nil },
			nil
	}

	if rf == nil {
		rf = func(context.Context, uint64, Stream) error { return nil }
	}

	rctx, d, wait := contextDoneWithContext(ctx)
	done := func(err error) error {
		_ = pool.RemoveValues(uid)

		return d(err)
	}

	_, _ = pool.Set(uid, func(_ dutil.In2ErrorFunc[context.Context, Stream], found bool) (dutil.In2ErrorFunc[context.Context, Stream], error) {
		if found {
			return nil, dutil.ErrLockedSetIgnore
		}

		var enough, si uint64

		return func(ctx context.Context, stream Stream) error {
				defer func() {
					if atomic.AddUint64(&enough, 1) == n {
						_ = done(nil)
					}
				}()

				if rctx.Err() != nil {
					return contextCause(rctx)
				}

				if err := rf(joinContext(ctx, rctx), atomic.AddUint64(&si, 1)-1, stream); err != nil {
					return done(err)
				}

				return nil
			},
			nil
	})

	return done,
		func() error {
			if err := writeSubStreamError(cstream, nil); err != nil {
				return done(err)
			}

			defer func() {
				_ = pool.RemoveValues(uid)
			}()

			return wait()
		},
		nil
}

func readSubStreamRequestUID(r io.Reader) (uid subStreamsRequestUID, _ error) {
	switch _, b, err := frame.ReadBytes(r); {
	case err != nil && !errors.Is(err, io.EOF):
		return uid, err
	case len(b) < 1:
		return uid, errors.New("substream request uid: empty")
	default:
		if len(b) != len(uid) {
			return uid, errors.New("substream request uid: wrong length")
		}

		copy(uid[:], b)

		return uid, nil
	}
}

func doSem(ctx context.Context, size int64) dutil.InErrorFunc[dutil.ErrorFunc] {
	sem := semaphore.NewWeighted(size)

	return func(f dutil.ErrorFunc) error {
		if err := sem.Acquire(ctx, 1); err != nil {
			return err
		}

		go func() {
			defer sem.Release(1)

			_ = f()
		}()

		return nil
	}
}

func writeSubStreamError(stream Stream, err error) error {
	var errb []byte

	if err != nil {
		if errb = []byte(err.Error()); len(errb) < 1 {
			errb = []byte(fmt.Sprintf("wait: %T", err))
		}
	}

	if _, err := frame.WriteBytes(stream, errb); err != nil {
		return err
	}

	return nil
}

func readSubStreamError(stream Stream) (error, error) {
	switch _, b, err := frame.ReadBytes(stream); {
	case err != nil:
		return nil, err
	case len(b) < 1:
		return nil, nil
	default:
		return errors.New(string(b)), nil
	}
}

func acceptStreamHandlers(
	ctx context.Context,
	workSize int64,
	pool subStremasPool,
) dutil.InErrorFunc[requestSubStreamsAcceptStreamFunc] {
	doHandler := doSem(ctx, workSize)

	return func(f requestSubStreamsAcceptStreamFunc) error {
		// this error is not from inside function, it is semaphore acquire error
		return doHandler(func() error {
			switch uid, stream, err := f(); {
			case err != nil:
				return err
			default:
				var cerr error

				switch r, found := pool.Value(uid); {
				case !found:
					cerr = errors.Errorf("unknown uid, %v", uid)
				default:
					cerr = r(ctx, stream)
				}

				return closeStream(stream, cerr)
			}
		})
	}
}
