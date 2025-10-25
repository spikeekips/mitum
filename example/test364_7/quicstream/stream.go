package quicstream

import (
	"context"
	"fmt"
	"io"
	"sync"

	"github.com/pkg/errors"
	"github.com/quic-go/quic-go"
	dutil "github.com/spikeekips/mitum/example/test364_5/util"
	"github.com/spikeekips/mitum/example/test364_7/frame"
	"github.com/spikeekips/mitum/util"
	"golang.org/x/sync/errgroup"
)

type (
	SubStreamsHandlerFunc = dutil.In2ErrorFunc[[]*quic.Stream, error]
	RequestSubStreamsFunc = func(context.Context, *quic.Stream, uint64, SubStreamsHandlerFunc) error
	WaitSubStreamsFunc    = func(context.Context, *quic.Stream, SubStreamsHandlerFunc) error
)

type subStreamPoolItem struct {
	streams []*quic.Stream
	done    func([]*quic.Stream, error)
}

func newSubStreamPoolItem(n uint64, done func([]*quic.Stream, error)) *subStreamPoolItem {
	var doneOnce sync.Once

	return &subStreamPoolItem{
		streams: make([]*quic.Stream, 0, n),
		done: func(streams []*quic.Stream, err error) {
			doneOnce.Do(func() {
				done(streams, err)
			})
		},
	}
}

func newFuncRequestSubStreams(ctx context.Context, conn QuicConn, poolSize uint64) (
	RequestSubStreamsFunc,
	dutil.InErrorFunc[error],
	error,
) {
	pool := dutil.NewSingleLockedMap[string, *subStreamPoolItem]()

	return newFuncRequestSubStreamsWithPool(ctx, conn, pool)
}

func newFuncRequestSubStreamsWithPool(ctx context.Context, conn QuicConn, pool dutil.LockedMap[string, *subStreamPoolItem]) (
	RequestSubStreamsFunc,
	dutil.InErrorFunc[error],
	error,
) {
	sctx, sctxCancel := dutil.ContextWithCancelCause(ctx)

	cancel := func(err error) error {
		for _, item := range pool.Seq2 {
			closeSubStreamPoolItem(item, err)
		}

		_ = sctxCancel(err)
		pool.Close()

		return err
	}

	go func() {
	end:
		for {
			switch stream, err := conn.AcceptStream(sctx); {
			case err != nil:
				// FIXME logging

				_ = cancel(err)

				return
			default:
				var uid []byte

				switch i, err := readStreamUID(stream); {
				case err != nil:
					// FIXME logging
					continue end
				default:
					uid = i
				}

				_, _, _ = pool.SetOrRemove(string(uid), func(item *subStreamPoolItem, found bool) (*subStreamPoolItem, bool, error) {
					if !found {
						return nil, false, dutil.ErrLockedSetIgnore
					}

					item.streams = append(item.streams, stream)

					if len(item.streams) == cap(item.streams) {
						closeSubStreamPoolItem(item, nil)

						return nil, true, nil
					}

					return item, false, nil
				})
			}
		}
	}()

	return func(ctx context.Context, stream *quic.Stream, n uint64, f SubStreamsHandlerFunc) error {
			switch {
			case ctx.Err() != nil:
				return ctx.Err()
			case sctx.Err() != nil:
				return sctx.Err()
			}

			var fonce sync.Once

			of := func(streams []*quic.Stream, err error) (gerr error) {
				fonce.Do(func() {
					gerr = f(streams, err)
				})

				return gerr
			}

			uid := util.UUID().Bytes()

			done, wait := dutil.NewContextDone(ctx)
			defer done(nil)

			go func() {
				added, err := requestSubStreams(ctx, pool, stream, uid, n, of)
				if added {
					_ = removeSubStreamPoolItem(pool, uid, err)
				}

				_ = done(err)
			}()

			cerr := wait()

			if err := of(nil, cerr); err != nil {
				return err
			}

			return cerr
		},
		func(err error) error {
			return cancel(errors.New("sub streams pool closed"))
		},
		nil
}

func requestSubStreams(
	ctx context.Context,
	pool dutil.LockedMap[string, *subStreamPoolItem],
	stream *quic.Stream,
	uid []byte,
	n uint64,
	f SubStreamsHandlerFunc,
) (added bool, _ error) {
	if _, err := frame.WriteUint64(stream, n); err != nil {
		return false, err
	}

	switch i, _, err := frame.ReadUint64(stream); {
	case err != nil:
		return false, err
	case i < 1:
		return false, f(nil, nil)
	default:
		n = i
	}

	if _, err := pool.Set(string(uid), func(*subStreamPoolItem, bool) (*subStreamPoolItem, error) {
		return newSubStreamPoolItem(n, func(streams []*quic.Stream, err error) {
			fmt.Println("f")
			_ = f(streams, err)
		}), nil
	}); err != nil {
		return false, err
	}

	return true, endRequestSubStreams(stream, uid)
}

func newFuncWaitSubStreams(conn QuicConn, limit uint64) WaitSubStreamsFunc {
	return func(ctx context.Context, stream *quic.Stream, f SubStreamsHandlerFunc) error {
		var fonce sync.Once
		of := func(streams []*quic.Stream, err error) (gerr error) {
			fonce.Do(func() {
				gerr = f(streams, err)
			})

			return gerr
		}

		done, wait := dutil.NewContextDone(ctx)
		defer done(nil)

		go func() {
			_ = done(waitFuncWaitSubStreams(ctx, conn, limit, stream, of))
		}()

		gerr := wait()
		if err := of(nil, gerr); err != nil {
			gerr = err
		}

		var errb []byte
		if gerr != nil {
			if errb = []byte(gerr.Error()); len(errb) < 1 {
				errb = []byte(fmt.Sprintf("wait: %T", gerr))
			}
		}

		if _, err := frame.WriteBytes(stream, errb); err != nil {
			gerr = err
		}

		return gerr
	}
}

func waitFuncWaitSubStreams(ctx context.Context, conn QuicConn, limit uint64, stream *quic.Stream, f SubStreamsHandlerFunc) error {
	if err := ctx.Err(); err != nil {
		return err
	}

	var n uint64

	switch i, _, err := frame.ReadUint64(stream); {
	case err != nil:
		return err
	default:
		n = min(i, limit)
	}

	if _, err := frame.WriteUint64(stream, n); err != nil {
		return err
	}

	if n < 1 {
		return nil
	}

	var uid []byte

	switch i, err := readStreamUID(stream); {
	case err != nil && !errors.Is(err, io.EOF):
		return err
	default:
		uid = i
	}

	streams := make([]*quic.Stream, n)
	defer func() {
		closeStreams(streams, nil)
		clear(streams)
	}()

	eg, ectx := errgroup.WithContext(ctx)

	for i := range n {
		eg.Go(func() error {
			switch stream, err := waitOpenSubStream(ectx, conn, uid); {
			case err != nil:
				return err
			default:
				streams[i] = stream

				return nil
			}
		})
	}

	gerr := eg.Wait()
	if err := f(streams, gerr); err != nil {
		gerr = err
	}

	return gerr
}

func readStreamUID(r io.Reader) ([]byte, error) {
	switch _, i, err := frame.ReadBytes(r); {
	case err != nil && !errors.Is(err, io.EOF):
		return nil, err
	default:
		b, err := io.ReadAll(i)
		return b, errors.WithStack(err)
	}
}

func waitOpenSubStream(ctx context.Context, conn QuicConn, uid []byte) (*quic.Stream, error) {
	switch stream, err := conn.OpenStreamSync(ctx); {
	case err != nil:
		return nil, err
	default:
		_, err := frame.WriteBytes(stream, uid)

		return stream, err
	}
}

func endRequestSubStreams(stream *quic.Stream, uid []byte) error {
	if _, err := frame.WriteBytes(stream, uid); err != nil {
		return err
	}

	switch _, i, err := frame.ReadBytes(stream); {
	case err != nil && !errors.Is(err, io.EOF):
		return err
	case i == nil:
		return nil
	default:
		b, err := io.ReadAll(i)
		if err != nil {
			return err
		}

		return errors.New(string(b))
	}
}

func closeSubStreamPoolItem(item *subStreamPoolItem, err error) {
	switch {
	case err != nil:
		item.done(nil, err)
	default:
		item.done(item.streams, nil)
	}

	closeStreams(item.streams, err)

	clear(item.streams)
	item.streams = nil
}

func removeSubStreamPoolItem(pool dutil.LockedMap[string, *subStreamPoolItem], uid []byte, err error) error {
	_, _ = pool.Remove(string(uid), func(item *subStreamPoolItem, found bool) error {
		if !found {
			return dutil.ErrLockedSetIgnore
		}

		closeSubStreamPoolItem(item, err)

		return nil
	})

	return err
}
