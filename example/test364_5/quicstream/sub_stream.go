package quicstream

import (
	"context"
	stderrors "errors"
	"io"
	"iter"
	"math"
	"sync"
	"sync/atomic"
	"time"

	"github.com/pkg/errors"
	"github.com/quic-go/quic-go"
	"github.com/spikeekips/mitum/example/test364_5/frame"
	dutil "github.com/spikeekips/mitum/example/test364_5/util"
	"github.com/spikeekips/mitum/util"
)

func AcceptSubStreams( //nolint:funlen
	ctx context.Context,
	conn quic.Connection,
	stream quic.Stream,
	maxSize uint16,
) (
	dutil.InErrorFunc[dutil.InErrorFunc[[]quic.Stream]],
	error,
) {
	var id string

	switch b, _, err := frame.ReadFixed(stream); {
	case err != nil:
		return nil, err
	case len(b) < 1:
		return nil, errors.New("empty id")
	default:
		id = string(b)
	}

	var size uint16

	switch i, err := readSubStreamsSize(stream, maxSize); {
	case err != nil:
		return nil, err
	default:
		size = i
	}

	if size < 1 {
		if err := frame.WriteSize(stream, 0); err != nil {
			return nil, err
		}

		return func(f dutil.InErrorFunc[[]quic.Stream]) error { return f(nil) }, nil
	}

	var controlStream quic.Stream

	switch s, err := openSubStreamSync(ctx, conn, id); {
	case err != nil:
		return nil, errors.WithMessage(err, "control stream")
	default:
		controlStream = s
	}

	if size < 2 { //nolint:mnd
		return func(f dutil.InErrorFunc[[]quic.Stream]) error {
				err := frame.WriteSize(controlStream, 1)
				if err == nil {
					err = f([]quic.Stream{controlStream})
				}

				_ = closeStream(controlStream, err)

				return err
			},
			nil
	}

	var add dutil.AppendSliceAddFunc[quic.Stream]
	var done dutil.OutFunc[[]quic.Stream]

	switch a, d, err := dutil.NewLockAppendSlice[quic.Stream](uint64(size), 0); {
	case err != nil:
		_ = closeStream(controlStream, err)

		return nil, err
	default:
		add, done = a, d
	}

	_, _ = add(controlStream)

	var newJob dutil.JobWorkerNewJobFunc
	var jobDone func()
	var jobWait dutil.InErrorFunc[error]

	switch n, d, w, err := dutil.NewFuncJobWorker(ctx, int64(size-1)); {
	case err != nil:
		_ = closeStream(controlStream, err)

		return nil, err
	default:
		newJob, jobDone, jobWait = n, d, w
	}

	return func(f dutil.InErrorFunc[[]quic.Stream]) error {
			for range size - 1 {
				if err := newJob(func(ctx context.Context) error {
					switch stream, err := openSubStreamSync(ctx, conn, id); {
					case err != nil:
						return err
					default:
						_, _ = add(stream)

						return nil
					}
				}); err != nil {
					break
				}
			}

			jobDone()

			_ = jobWait(nil)

			streams := done()
			defer dutil.ClearSlice(&streams)

			if err := frame.WriteSize(controlStream, dutil.SafeIntToUint32(len(streams))); err != nil {
				return err
			}

			var gerr error

			if f != nil {
				gerr = f(streams)
			}

			_ = closeStreams(streams, gerr)

			return gerr
		},
		nil
}

func OpenSubStreams(
	ctx context.Context,
	open openConnSubStreamsFunc,
	conn quic.Connection,
	stream quic.Stream,
	id string,
	size uint16,
) (
	dutil.InErrorFunc[dutil.InErrorFunc[[]quic.Stream]],
	error,
) {
	if id == "" {
		return nil, errors.New("empty id")
	}

	var wait func() ([]quic.Stream, error)
	var closef dutil.InErrorFunc[error]

	if size < 1 {
		wait = func() ([]quic.Stream, error) {
			switch rsize, err := readSubStreamsSize(stream, 1); {
			case err != nil:
				return nil, err
			case rsize > 0:
				return nil, errors.Errorf("zero size requested, but %d", rsize)
			default:
				return nil, nil
			}
		}
		closef = func(err error) error { return err }
	} else {
		switch w, c, err := open(ctx, conn, id, size); {
		case err != nil:
			return nil, err
		default:
			wait, closef = w, c
		}
	}

	switch streams, err := waitOpenSubStreams(stream, wait, id, size); {
	case err != nil:
		return nil, closef(errors.WithMessagef(err, "failed to wait requested sub streams"))
	default:
		return func(f dutil.InErrorFunc[[]quic.Stream]) error {
				return closef(f(streams))
			},
			nil
	}
}

func waitOpenSubStreams(
	stream quic.Stream,
	waitStreams func() ([]quic.Stream, error),
	id string,
	size uint16,
) ([]quic.Stream, error) {
	if err := frame.WriteFixed(stream, []byte(id)); err != nil {
		return nil, err
	}

	if err := frame.WriteSize(stream, uint32(size)); err != nil {
		return nil, err
	}

	return waitStreams()
}

func openSubStreamSync(ctx context.Context, conn quic.Connection, id string) (quic.Stream, error) {
	switch stream, err := conn.OpenStreamSync(ctx); {
	case err != nil:
		return nil, errors.WithStack(err)
	default:
		// NOTE send id for streams bonding first
		if err := frame.WriteFixed(stream, []byte(id)); err != nil {
			_ = closeStream(stream, err)

			return nil, err
		}

		return stream, nil
	}
}

func loadQuicConnectionFromContext(ctx context.Context) (quic.Connection, error) {
	switch i := ctx.Value(QuicConnectionContextKey); i {
	case nil:
		return nil, util.ErrNotFound.Errorf("context key, %q not found", QuicConnectionContextKey)
	default:
		conn, ok := i.(quic.Connection)
		if !ok {
			return nil, util.ErrNotFound.Errorf("expected quic.Connection, but %T", i)
		}

		return conn, nil
	}
}

type (
	openConnSubStreamsFunc = func(_ context.Context, _ quic.Connection, id string, size uint16) (
		func() ([]quic.Stream, error),
		dutil.InErrorFunc[error],
		error,
	)
	requestSubStreamsFunc = func(_ context.Context, id string, size uint16) (
		func() ([]quic.Stream, error),
		dutil.InErrorFunc[error],
		error,
	)
	addSubStreamFunc = func(quic.Stream) (enough bool)
)

var errConnAcceptSubStreamsClosed = errors.New("ConnAcceptSubStreamsClosed")

func newConnAcceptSubStreams(
	ctx context.Context,
	conn quic.Connection,
) (requestSubStreamsFunc, dutil.OutFunc[bool]) {
	var l sync.Mutex
	ids := map[string]addSubStreamFunc{}

	gctx, gctxCancel := dutil.ContextWithCancelCause(ctx)

	handleStream := func(stream quic.Stream) {
		var id string

		switch b, n, err := frame.ReadFixed(stream); {
		case err != nil && !errors.Is(err, io.EOF),
			n < 1 && errors.Is(err, io.EOF):
			_ = closeStream(stream, errors.Wrap(err, "empty id"))

			return
		default:
			id = string(b)
		}

		l.Lock()
		defer l.Unlock()

		switch add, found := ids[id]; {
		case !found:
			_ = closeStream(stream, errors.Errorf("unknown id, %q", id))
		case add(stream):
			delete(ids, id)
		}
	}

	go func() {
		for {
			switch stream, err := conn.AcceptStream(gctx); {
			case err != nil:
				_ = gctxCancel(errors.WithStack(err))

				return
			default:
				handleStream(stream)
			}
		}
	}()

	return func(ctx context.Context, id string, size uint16) (
			func() ([]quic.Stream, error),
			dutil.InErrorFunc[error],
			error,
		) {
			switch err := context.Cause(gctx); {
			case err == nil:
			case errors.Is(err, errConnAcceptSubStreamsClosed):
				return nil, nil, err //nolint:wrapcheck
			default:
				return nil, nil, stderrors.Join(errConnAcceptSubStreamsClosed, err)
			}

			l.Lock()
			defer l.Unlock()

			switch {
			case ids == nil:
				return nil, nil, errors.WithStack(errConnAcceptSubStreamsClosed)
			case ids[id] != nil:
				return nil, nil, util.ErrFound.Errorf("known id, %q", id)
			}

			var addStream dutil.AppendSliceAddFunc[quic.Stream]
			var doneStream dutil.OutFunc[[]quic.Stream]

			switch a, d, err := dutil.NewLockAppendSlice[quic.Stream](uint64(size), 0); {
			case err != nil:
				return nil, nil, err
			default:
				addStream, doneStream = a, d
			}

			jctx := dutil.JoinContext(gctx, ctx)

			var controlOnce sync.Once
			var controlStream quic.Stream

			controlDone, controlWait := dutil.NewContextDone(jctx) //nolint:contextcheck
			_, streamsDone, _, streamsWait := dutil.NewContextWait(jctx)

			rsize := uint64(size)
			left := uint64(size)

			ids[id] = func(stream quic.Stream) bool {
				var ok bool

				switch _, l := addStream(stream); {
				case l < 1, uint64(size)-l >= atomic.LoadUint64(&rsize):
					_ = streamsDone(nil)

					ok = true
				default:
					_ = atomic.SwapUint64(&left, l)
				}

				controlOnce.Do(func() {
					controlStream = stream

					_ = controlDone(nil)
				})

				return ok
			}

			var closeStreamsOnce sync.Once

			return func() ([]quic.Stream, error) {
					if err := controlWait(); err != nil {
						return nil, streamsDone(err)
					}

					var nsize uint16

					switch i, err := readSubStreamsSize(controlStream, size); {
					case err != nil:
						return nil, err
					case i < 2: //nolint:mnd
						_ = streamsDone(nil)

						return doneStream(), nil
					default:
						nsize = i
					}

					if nsize != size {
						if !atomic.CompareAndSwapUint64(&rsize, uint64(size), uint64(nsize)) {
							return nil, errors.New("already called")
						}
					}

					if l := atomic.LoadUint64(&left); l < 1 || uint64(size)-l >= uint64(nsize) {
						_ = streamsDone(nil)
					}

					if err := streamsWait(); err != nil {
						return nil, err
					}

					return doneStream(), nil
				},
				func(err error) error {
					l.Lock()
					defer l.Unlock()

					closeStreamsOnce.Do(func() {
						_ = closeStreams(doneStream(), err)
					})

					if _, found := ids[id]; found {
						delete(ids, id)
						_ = controlDone(err)
						_ = streamsDone(err)
					}

					return err
				},
				nil
		},
		func() bool {
			l.Lock()
			defer l.Unlock()

			if ctx.Err() != nil || gctx.Err() != nil {
				clear(ids)
				ids = nil

				return true
			}

			if len(ids) < 1 {
				_ = gctxCancel(nil)

				return true
			}

			return false
		}
}

func NewConnsAcceptSubStreams(
	ctx context.Context,
	removeInactiveAfter time.Duration,
) (
	openConnSubStreamsFunc,
	func(),
	dutil.LockedMap[string, [2]interface{}],
) {
	pool := dutil.NewSingleLockedMap[
		string,
		[2]interface{}, /* {openSubStreamsFunc, dutil.OutFunc[bool]} */
	]()
	lastCalled := map[string]time.Time{}

	gctx, gctxCancel := context.WithCancel(ctx)

	removeInactiveTicker := time.NewTicker(removeInactiveAfter)

	closef := func() {
		pool.CloseFunc(func() {
			gctxCancel()

			removeInactiveTicker.Stop()
			clear(lastCalled)
		})
	}

	go func() {
		for {
			select {
			case <-gctx.Done():
				closef()

				return
			case <-removeInactiveTicker.C:
			}

			_ = pool.Lock(func(
				_ func(string) ([2]interface{}, bool),
				_ func(string, [2]interface{}) bool,
				remove func(string) bool,
				traverse iter.Seq2[string, [2]interface{}],
			) error {
				for k, v := range traverse {
					if t, found := lastCalled[k]; !found || t.Add(removeInactiveAfter).Before(time.Now()) {
						if connClose, ok := v[1].(dutil.OutFunc[bool]); !ok || connClose() {
							remove(k)
							delete(lastCalled, k)
						}
					}
				}

				return nil
			})
		}
	}()

	return func(ctx context.Context, conn quic.Connection, id string, size uint16) (
			func() ([]quic.Stream, error),
			dutil.InErrorFunc[error],
			error,
		) {
			var waitStreams func() ([]quic.Stream, error)
			var doneStream dutil.InErrorFunc[error]

			connID := conn.LocalAddr().String() + "," + conn.RemoteAddr().String()
			if _, err := pool.Set(connID, func(v [2]interface{}, _ bool) ([2]interface{}, error) {
				if newID, ok := v[0].(requestSubStreamsFunc); ok {
					switch w, d, err := newID(ctx, id, size); {
					case err == nil:
						waitStreams, doneStream = w, d
						lastCalled[connID] = time.Now()

						return v, dutil.ErrLockedSetIgnore
					case !errors.Is(err, errConnAcceptSubStreamsClosed):
						return v, err
					}
				}

				newID, closef := newConnAcceptSubStreams(gctx, conn)
				lastCalled[connID] = time.Now()

				switch w, d, err := newID(ctx, id, size); {
				case err != nil:
					return v, err
				default:
					waitStreams, doneStream = w, d

					return [2]interface{}{newID, closef}, nil
				}
			}); err != nil {
				return nil, nil, err
			}

			return waitStreams, doneStream, nil
		},
		closef,
		pool
}

func readSubStreamsSize(stream quic.Stream, size uint16) (uint16, error) {
	var nrsize uint32

	switch _, err := frame.ReadSize(stream, &nrsize); {
	case err != nil && !errors.Is(err, io.EOF):
		return 0, errors.Wrap(err, "received size")
	case size < 1, nrsize < 1:
		return 0, nil
	case nrsize > math.MaxUint16:
		return 0, errors.New("received size > max uint16")
	default:
		return min(size, uint16(nrsize)), nil
	}
}
