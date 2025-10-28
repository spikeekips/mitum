package quicstream

import (
	"context"
	"fmt"
	"io"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"github.com/pkg/errors"
	"github.com/quic-go/quic-go"
	dutil "github.com/spikeekips/mitum/example/test364_5/util"
	"github.com/spikeekips/mitum/example/test364_7/frame"
	"github.com/spikeekips/mitum/util"
	"golang.org/x/sync/errgroup"
)

type (
	SubStreamsHandlerFunc  = func(context.Context, []*quic.Stream) (context.Context, error)
	RSubStreamsHandlerFunc = func(context.Context, SubStreamsHandlerFunc) (context.Context, error)
	RequestSubStreamsFunc  = func(context.Context, *quic.Stream, uint64) (RSubStreamsHandlerFunc, error)
	ServeSubStreamsFunc    = func(context.Context, *quic.Stream) (RSubStreamsHandlerFunc, error)
)

type subStreamsRequestUID [16]byte

type subStreamPoolItem struct {
	streams []*quic.Stream
	set     func(*quic.Stream) bool
	done    func(error)
}

func newSubStreamPoolItem(n uint64, done func(error)) *subStreamPoolItem {
	item := subStreamPoolItemPool.Get().(*subStreamPoolItem)
	item.streams = make([]*quic.Stream, 0, n)

	item.set = func(stream *quic.Stream) bool {
		item.streams = append(item.streams, stream)

		return len(item.streams) >= cap(item.streams)
	}

	item.done = done

	return item
}

var subStreamPoolItemPool = sync.Pool{
	New: func() any {
		return &subStreamPoolItem{}
	},
}

func subStreamPoolItemPoolPut(item *subStreamPoolItem) {
	clear(item.streams)
	item.streams = nil
	item.set = nil
	item.done = nil

	subStreamPoolItemPool.Put(item)
}

func newRequestSubStreams(ctx context.Context, conn QuicConn, acceptTimeout time.Duration) (
	RequestSubStreamsFunc,
	dutil.InErrorFunc[error],
) {
	pool := dutil.NewSingleLockedMap[subStreamsRequestUID, *subStreamPoolItem]()

	return newRequestSubStreamsWithPool(ctx, conn, acceptTimeout, pool)
}

func newRequestSubStreamsWithPool(
	ctx context.Context,
	conn QuicConn,
	acceptTimeout time.Duration,
	pool *dutil.SingleLockedMap[subStreamsRequestUID, *subStreamPoolItem],
) (
	RequestSubStreamsFunc,
	dutil.InErrorFunc[error],
) {
	log := dutil.LoadContextValueLogger(ctx)

	pctx, pctxCancel := dutil.ContextWithCancelCause(ctx)

	var cancelOnce sync.Once
	cancel := func(err error) error {
		cancelOnce.Do(func() {
			_ = pctxCancel(err)

			pool.Close()
		})

		return err
	}

	go func() {
		for {
			if pctx.Err() != nil {
				err := context.Cause(pctx)
				_ = cancel(err)

				log.Error().Err(err).Msg("request sub streams stopped")

				return
			}

			if err := requestAcceptSubStream(pctx, conn, pool, acceptTimeout); err != nil {
				_ = cancel(err)

				log.Error().Err(err).Msg("request sub streams stopped")

				return
			}
		}
	}()

	return func(ctx context.Context, cstream *quic.Stream, n uint64) (RSubStreamsHandlerFunc, error) {
			nctx := dutil.JoinContext(pctx, ctx)
			if nctx.Err() != nil {
				return nil, context.Cause(nctx)
			}

			streams, gerr := contextEnsureWait(nctx, func(ctx context.Context) ([]*quic.Stream, error) {
				var uid subStreamsRequestUID
				copy(uid[:], util.UUID().Bytes())

				streams, gerr := requestSubStreams(ctx, conn, cstream, n, pool, uid)
				if err := endRequestSubStreams(nctx, cstream); err != nil {
					gerr = err
				}

				return streams, gerr
			})

			if gerr != nil {
				if err := closeStreams(streams, gerr); err != nil {
					gerr = err
				}

				return nil, gerr
			}

			return func(ctx context.Context, handler SubStreamsHandlerFunc) (context.Context, error) {
					nctx := ctx
					var gerr error

					if handler != nil {
						nctx, gerr = handler(ctx, streams)
					}

					if err := closeStreams(streams, gerr); err != nil {
						gerr = err
					}

					return nctx, gerr
				},
				nil
		},
		cancel
}

func requestSubStreams(
	ctx context.Context,
	conn QuicConn,
	cstream *quic.Stream,
	n uint64,
	pool *dutil.SingleLockedMap[subStreamsRequestUID, *subStreamPoolItem],
	uid subStreamsRequestUID,
) ([]*quic.Stream, error) {
	if _, err := frame.WriteUint64(cstream, n); err != nil {
		return nil, err
	}

	nn := n

	switch i, _, err := frame.ReadUint64(cstream); {
	case err != nil:
		return nil, err
	case i < 1:
		return nil, nil
	default:
		nn = i
	}

	if _, err := frame.WriteBytes(cstream, uid[:]); err != nil {
		return nil, err
	}

	done, wait := newContextDone(ctx)
	defer done(nil)

	if _, err := pool.Set(uid, func(*subStreamPoolItem, bool) (*subStreamPoolItem, error) {
		return newSubStreamPoolItem(nn, func(err error) {
			_ = done(err)
		}), nil
	}); err != nil {
		return nil, err
	}

	gerr := wait()

	var streams []*quic.Stream

	_, _ = pool.Remove(uid, func(item *subStreamPoolItem, found bool) error {
		if !found {
			return dutil.ErrLockedSetIgnore
		}

		if gerr == nil {
			streams = make([]*quic.Stream, len(item.streams))
			copy(streams, item.streams)
		}

		subStreamPoolItemPoolPut(item)

		return nil
	})

	if gerr != nil {
		if err := closeStreams(streams, gerr); err != nil {
			gerr = err
		}

		clear(streams)
		streams = nil
	}

	return streams, gerr
}

func endRequestSubStreams(ctx context.Context, cstream *quic.Stream) error {
	switch _, i, err := frame.ReadBytes(cstream); {
	case err != nil && !errors.Is(err, io.EOF):
		return err
	case i == nil:
		return nil
	default:
		b, err := io.ReadAll(i)
		if err != nil {
			return errors.WithStack(err)
		}

		return errors.New(string(b))
	}
}

func newServeSubStreams(conn QuicConn, limit uint64) ServeSubStreamsFunc {
	return func(ctx context.Context, cstream *quic.Stream) (RSubStreamsHandlerFunc, error) {
		streams, gerr := contextEnsureWait(ctx, func(ctx context.Context) ([]*quic.Stream, error) {
			streams, gerr := serveSubStreams(ctx, conn, limit, cstream)

			var errb []byte
			if gerr != nil {
				if errb = []byte(gerr.Error()); len(errb) < 1 {
					errb = []byte(fmt.Sprintf("wait: %T", gerr))
				}
			}

			if _, err := frame.WriteBytes(cstream, errb); err != nil {
				gerr = err
			}

			return streams, gerr
		})

		if gerr != nil {
			if err := closeStreams(streams, gerr); err != nil {
				gerr = err
			}

			clear(streams)
			streams = nil

			return nil, gerr
		}

		return func(ctx context.Context, handler SubStreamsHandlerFunc) (context.Context, error) {
				nctx := ctx
				var gerr error

				if handler != nil {
					nctx, gerr = handler(ctx, streams)
				}

				if err := closeStreams(streams, gerr); err != nil {
					gerr = err
				}

				return nctx, gerr
			},
			nil
	}
}

func serveSubStreams(ctx context.Context, conn QuicConn, limit uint64, cstream *quic.Stream) ([]*quic.Stream, error) {
	var n uint64

	switch i, _, err := frame.ReadUint64(cstream); {
	case err != nil:
		return nil, err
	default:
		n = min(i, limit)
	}

	if _, err := frame.WriteUint64(cstream, n); err != nil {
		return nil, err
	}

	if n < 1 {
		return nil, nil
	}

	var uid subStreamsRequestUID

	switch i, err := readSubStreamRequestUID(cstream); {
	case err != nil:
		return nil, err
	default:
		uid = i
	}

	streams := make([]*quic.Stream, n)

	eg, ectx := errgroup.WithContext(ctx)

	for i := range n {
		eg.Go(func() error {
			switch stream, err := openSubStream(ectx, conn, uid); {
			case err != nil:
				return err
			default:
				streams[i] = stream

				return nil
			}
		})
	}

	gerr := eg.Wait()

	if gerr != nil {
		if err := closeStreams(streams, gerr); err != nil {
			gerr = err
		}

		clear(streams)
		streams = nil
	}

	return streams, gerr
}

func requestAcceptSubStream(
	ctx context.Context,
	conn QuicConn,
	pool *dutil.SingleLockedMap[subStreamsRequestUID, *subStreamPoolItem],
	timeout time.Duration,
) error {
	tctx, tctxCancel := context.WithTimeout(ctx, timeout)
	defer tctxCancel()

	var uv atomic.Value

	stream, err := contextEnsureWait(tctx, func(ctx context.Context) (*quic.Stream, error) {
		switch stream, u, err := acceptSubStream(ctx, conn); {
		case err != nil:
			return nil, err
		default:
			uv.Store(u)

			return stream, nil
		}
	})
	if err != nil {
		var operr *net.OpError

		switch {
		case errors.Is(err, quic.ErrTransportClosed):
		case errors.As(err, &operr):
		default:
			return nil // ignore error
		}

		return err
	}

	_ = pool.Get(uv.Load().(subStreamsRequestUID), func(item *subStreamPoolItem, found bool) error {
		if !found {
			return nil
		}

		if ok := item.set(stream); ok {
			item.done(nil)
		}

		return nil
	})

	return nil
}

func acceptSubStream(ctx context.Context, conn QuicConn) (
	_ *quic.Stream,
	uid subStreamsRequestUID,
	_ error,
) {
	switch stream, err := conn.AcceptStream(ctx); {
	case err != nil:
		return nil, uid, err
	default:
		u, err := readSubStreamRequestUID(stream)
		if err != nil {
			_ = closeStream(stream, err)

			return nil, u, err
		}

		return stream, u, nil
	}
}

func openSubStream(ctx context.Context, conn QuicConn, uid subStreamsRequestUID) (*quic.Stream, error) {
	switch stream, err := conn.OpenStreamSync(ctx); {
	case err != nil:
		return nil, errors.WithStack(err)
	default:
		if _, err := frame.WriteBytes(stream, uid[:]); err != nil {
			return nil, closeStream(stream, err)
		}

		return stream, err
	}
}

// readSubStreamRequestUID reads uid value([]byte) from stream. io.EOF is ignored.
func readSubStreamRequestUID(r io.Reader) (uid subStreamsRequestUID, _ error) {
	switch _, i, err := frame.ReadBytes(r); {
	case err != nil && !errors.Is(err, io.EOF):
		return uid, err
	case i == nil:
		return uid, errors.New("substream request uid: empty")
	default:
		b, err := io.ReadAll(i)
		if err != nil {
			return uid, errors.WithStack(err)
		}

		if len(b) != len(uid) {
			return uid, errors.New("substream request uid: wrong length")
		}

		copy(uid[:], b)

		return uid, nil
	}
}

func contextEnsureWait[T any](ctx context.Context, f func(context.Context) (T, error)) (T, error) {
	var t T

	if ctx.Err() != nil {
		return t, context.Cause(ctx)
	}

	done, wait := newContextDone(ctx)
	defer done(nil)

	var p atomic.Pointer[T]

	go func() {
		i, err := f(ctx)
		if err == nil {
			p.Store(&i)
		}

		_ = done(err)
	}()

	if err := wait(); err != nil {
		return t, err
	}

	if i := p.Load(); i != nil {
		t = *i
	}

	return t, nil
}

func TimeoutSubStreamsHandler(timeout time.Duration, next SubStreamsHandlerFunc) SubStreamsHandlerFunc {
	return func(ctx context.Context, streams []*quic.Stream) (context.Context, error) {
		tctx, tctxCancel := context.WithTimeout(ctx, timeout)
		defer tctxCancel()

		return next(tctx, streams)
	}
}

func newContextDone(ctx context.Context) (
	dutil.InErrorFunc[error],
	dutil.ErrorFunc,
) {
	done := make(chan struct{})

	var gerr atomic.Pointer[error]

	return func(err error) error {
			if gerr.CompareAndSwap(nil, &err) {
				close(done)
			}

			return err
		},
		func() error {
			select {
			case <-ctx.Done():
				if i := gerr.Load(); i != nil {
					return *i
				}

				return context.Cause(ctx)
			case <-done:
				if i := gerr.Load(); i != nil {
					return *i
				}

				return nil
			}
		}
}
