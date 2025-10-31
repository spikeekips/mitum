//go:build failed

/*
this implementation can not handle the huge amount of sub streams.
*/

package quicstream

import (
	"context"
	"fmt"
	"io"
	"net"
	"sync"
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

		if uint64(len(item.streams)) == n {
			item.done(nil)

			return true
		}

		return false
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

	pctx, pctxCancel := contextWithCancelCause(joinContext(ctx, conn.Context()))

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
				err := contextCause(pctx)
				_ = cancel(err)

				log.Trace().Err(err).Msg("request sub streams stopped")

				return
			}

			switch f, err := requestAcceptSubStream(pctx, conn.AcceptStream, acceptTimeout); {
			case err != nil:
				_ = cancel(err)

				log.Trace().Err(err).Msg("request sub streams stopped; accept stream")

				return
			case f != nil:
				go func() { // FIXME use semaphore
					_ = f(pool)
				}()
			}
		}
	}()

	return newFuncRequestSubStreams(pctx, pool), cancel
}

func startRequestSubStreams(cstream *quic.Stream, n uint64, uid subStreamsRequestUID) (uint64, error) {
	if _, err := frame.WriteUint64(cstream, n); err != nil {
		return 0, err
	}

	nn := n

	switch i, _, err := frame.ReadUint64(cstream); {
	case err != nil:
		return 0, err
	case i < 1:
		return 0, nil
	default:
		nn = i
	}

	if _, err := frame.WriteBytes(cstream, uid[:]); err != nil {
		return 0, err
	}

	return nn, nil
}

func newFuncRequestSubStreams(
	pctx context.Context,
	pool *dutil.SingleLockedMap[subStreamsRequestUID, *subStreamPoolItem],
) RequestSubStreamsFunc {
	return func(ctx context.Context, cstream *quic.Stream, n uint64) (RSubStreamsHandlerFunc, error) {
		nctx := joinContext(pctx, ctx)
		if nctx.Err() != nil {
			return nil, contextCause(nctx)
		}

		streams, gerr := contextEnsureWaitT(nctx, func(ctx context.Context) ([]*quic.Stream, error) {
			var uid subStreamsRequestUID
			copy(uid[:], util.UUID().Bytes())

			var streams []*quic.Stream
			var gerr error

			switch nn, err := startRequestSubStreams(cstream, n, uid); {
			case err != nil:
				return nil, err
			case nn > 0:
				streams, gerr = doRequestSubStreams(ctx, cstream, pool, nn, uid)
			}

			if err := endRequestSubStreams(nctx, cstream); err != nil {
				gerr = err
			}

			return streams, gerr
		})

		if gerr != nil {
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

func doRequestSubStreams(
	ctx context.Context,
	cstream *quic.Stream,
	pool *dutil.SingleLockedMap[subStreamsRequestUID, *subStreamPoolItem],
	n uint64,
	uid subStreamsRequestUID,
) ([]*quic.Stream, error) {
	done, wait := newContextDone(ctx)
	defer done(nil)

	if _, err := pool.Set(uid, func(*subStreamPoolItem, bool) (*subStreamPoolItem, error) {
		return newSubStreamPoolItem(n, func(err error) {
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

		switch {
		case gerr == nil:
			streams = make([]*quic.Stream, len(item.streams))
			copy(streams, item.streams)
		default:
			if err := closeStreams(item.streams, gerr); err != nil {
				gerr = err
			}
		}

		subStreamPoolItemPoolPut(item)

		return nil
	})

	return streams, gerr
}

func endRequestSubStreams(ctx context.Context, cstream *quic.Stream) error {
	switch _, b, err := frame.ReadBytes(cstream); {
	case err != nil && !errors.Is(err, io.EOF):
		return err
	case len(b) < 1:
		return nil
	default:
		return errors.New(string(b))
	}
}

func newServeSubStreams(open OpenStreamFunc, limit uint64) ServeSubStreamsFunc {
	return func(ctx context.Context, cstream *quic.Stream) (RSubStreamsHandlerFunc, error) {
		streams, gerr := contextEnsureWaitT(ctx, func(ctx context.Context) ([]*quic.Stream, error) {
			var streams []*quic.Stream
			var gerr error

			switch n, uid, err := startServeSubStreams(limit, cstream); {
			case err != nil:
				return nil, err
			case n > 0:
				streams, gerr = doServeSubStreams(ctx, open, cstream, n, uid)
			}

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

func startServeSubStreams(limit uint64, cstream *quic.Stream) (_ uint64, uid subStreamsRequestUID, _ error) {
	var n uint64

	switch i, _, err := frame.ReadUint64(cstream); {
	case err != nil:
		return 0, uid, err
	default:
		n = min(i, limit)
	}

	if _, err := frame.WriteUint64(cstream, n); err != nil {
		return 0, uid, err
	}

	if n < 1 {
		return 0, uid, nil
	}

	i, err := readSubStreamRequestUID(cstream)

	return n, i, err
}

func doServeSubStreams(ctx context.Context, open OpenStreamFunc, cstream *quic.Stream, n uint64, uid subStreamsRequestUID) ([]*quic.Stream, error) {
	streams := make([]*quic.Stream, n)

	eg, ectx := errgroup.WithContext(ctx)

	for i := range n {
		eg.Go(func() error {
			switch stream, err := openSubStream(ectx, open, uid); {
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
	accept OpenStreamFunc,
	timeout time.Duration,
) (
	func(*dutil.SingleLockedMap[subStreamsRequestUID, *subStreamPoolItem]) error,
	error,
) {
	tctx, tctxCancel := context.WithTimeout(ctx, timeout)
	defer tctxCancel()

	var stream *quic.Stream

	var ierr *quic.IdleTimeoutError
	var operr *net.OpError

	switch i, err := accept(tctx); {
	case err != nil:
		switch {
		case errors.Is(err, quic.ErrTransportClosed),
			errors.As(err, &ierr),
			errors.As(err, &operr):
			return nil, errors.WithStack(err)
		default:
			return nil, nil // ignore error
		}
	default:
		stream = i
	}

	return func(pool *dutil.SingleLockedMap[subStreamsRequestUID, *subStreamPoolItem]) error {
			uid, err := readSubStreamRequestUID(stream)
			if err != nil {
				_ = closeStream(stream, err)

				return err
			}

			_, _ = pool.Set(uid, func(item *subStreamPoolItem, found bool) (*subStreamPoolItem, error) {
				if !found {
					return nil, dutil.ErrLockedSetIgnore
				}

				_ = item.set(stream)

				return item, nil
			})

			return nil
		},
		nil
}

func acceptSubStream(ctx context.Context, accept OpenStreamFunc) (
	_ *quic.Stream,
	uid subStreamsRequestUID,
	_ error,
) {
	switch stream, err := accept(ctx); {
	case err != nil:
		return nil, uid, errors.WithStack(err)
	default:
		u, err := readSubStreamRequestUID(stream)
		if err != nil {
			_ = closeStream(stream, err)

			return nil, u, err
		}

		return stream, u, nil
	}
}

func openSubStream(ctx context.Context, open OpenStreamFunc, uid subStreamsRequestUID) (*quic.Stream, error) {
	switch stream, err := open(ctx); {
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
