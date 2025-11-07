package quicstream

import (
	"bytes"
	"context"
	"fmt"
	"net"
	"os"
	"runtime"
	"slices"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/pkg/errors"
	"github.com/quic-go/quic-go"
	dutil "github.com/spikeekips/mitum/example/test364_5/util"
	"github.com/spikeekips/mitum/example/test364_7/frame"
	"github.com/spikeekips/mitum/util"
	"github.com/stretchr/testify/suite"
	"golang.org/x/sync/errgroup"
)

type testSubStreamsRequest struct {
	BaseTest
}

func (t *testSubStreamsRequest) Run(a string, f func()) {
	t.BaseTest.RunWithGoLeak(a, f)
}

func (t *testSubStreamsRequest) openConnections(
	reqCtx, serveCtx context.Context,
	reqQuicConfig, serveQuicConfig *quic.Config,
) (net.PacketConn, net.PacketConn, Conn, Conn, func()) {
	if reqCtx == nil {
		reqCtx = context.Background()
	}
	if serveCtx == nil {
		serveCtx = context.Background()
	}

	if reqQuicConfig == nil {
		reqQuicConfig = DefaultQuicConfig()
	}
	if serveQuicConfig == nil {
		serveQuicConfig = DefaultQuicConfig()
	}

	reqpconn := t.RandomPacketConn()
	servepconn, l := t.RandomListener(nil, serveQuicConfig)

	servech := make(chan Conn, 1)
	go func() {
		conn, err := l.Accept(serveCtx)
		t.PrintIfErrPanic(err, "accept")

		servech <- conn
	}()

	reqconn, err := dialWithPacketConn(reqCtx, reqpconn, servepconn.LocalAddr(), t.NewTLSConfig(), reqQuicConfig)
	t.PrintIfErrPanic(err, "req dial")

	serveconn := <-servech

	return reqpconn, servepconn, reqconn, serveconn, sync.OnceFunc(func() {
		err = reqpconn.Close()
		t.PrintIfErrCaller(err, 2, "req: PacketConn closed")
		err := servepconn.Close()
		t.PrintIfErrCaller(err, 2, "serve: PacketConn closed")

		closeConnection(reqconn, nil)
		closeConnection(serveconn, nil)
	})
}

func (t *testSubStreamsRequest) openControlStreams(ctx context.Context, reqconn, serveconn Conn) (
	Stream, Stream,
	func(),
) {
	if ctx == nil {
		ctx = context.Background()
	}

	sctx, sctxCancel := context.WithTimeout(ctx, time.Second*3)
	defer sctxCancel()

	servestreamch := make(chan Stream, 1)
	go func() {
		stream, err := contextEnsureT(sctx, func(ctx context.Context) (Stream, error) {
			stream, err := serveconn.AcceptStream(ctx)
			if err != nil {
				return nil, err
			}
			t.PrintIfErr(err, "serve conn: AcceptStream")

			_, _, err = frame.ReadBytes(stream)
			t.PrintIfErr(err, "serve stream: read")

			return stream, err
		})
		t.IfErrPanicf(err, "serve")

		servestreamch <- stream
	}()

	reqstream, err := reqconn.OpenStream()
	t.IfErrPanicf(err, "req conn: OpenStream")
	_, err = frame.WriteBytes(reqstream, []byte{0})
	t.IfErrPanicf(err, "req stream: write")

	var servestream Stream

	select {
	case <-sctx.Done():
		t.PrintIfErrPanic(err, "wait serve stream")
	case servestream = <-servestreamch:
	}

	return reqstream, servestream, sync.OnceFunc(func() {
		closeStream(reqstream, nil)
		closeStream(servestream, nil)
	})
}

type dummyStream struct {
	w         *bytes.Buffer
	ctx       context.Context
	ctxCancel func()
	rerr,
	werr error
	onceCancelRead,
	onceCancelWrite sync.Once
	isClosed int64
}

var _ Stream = &dummyStream{}

func newDmmyStream(ctx context.Context) *dummyStream {
	s := &dummyStream{}

	s.w = bytes.NewBuffer(nil)

	s.ctx, s.ctxCancel = context.WithCancel(ctx)

	return s
}

func (s *dummyStream) CancelRead(c quic.StreamErrorCode) {
	s.onceCancelRead.Do(func() {
		s.rerr = &quic.StreamError{ErrorCode: c}
	})
}

func (s *dummyStream) CancelWrite(c quic.StreamErrorCode) {
	s.onceCancelWrite.Do(func() {
		s.werr = &quic.StreamError{ErrorCode: c}
	})
}

func (s *dummyStream) Close() error {
	if !atomic.CompareAndSwapInt64(&s.isClosed, 0, 1) {
		return &net.OpError{Op: "closed", Err: errors.New("write on closed stream")}
	}

	s.ctxCancel()

	return nil
}
func (s *dummyStream) Context() context.Context { return s.ctx }
func (s *dummyStream) Read(p []byte) (int, error) {
	if s.ctx.Err() != nil {
		return 0, &net.OpError{Op: "closed", Err: errors.New("read on closed stream")}
	}

	return s.w.Read(p)
}
func (s *dummyStream) SetDeadline(time.Time) error      { return nil }
func (s *dummyStream) SetReadDeadline(time.Time) error  { return nil }
func (s *dummyStream) SetWriteDeadline(time.Time) error { return nil }
func (s *dummyStream) StreamID() quic.StreamID          { return 0 }
func (s *dummyStream) Write(b []byte) (int, error) {
	if s.ctx.Err() != nil {
		return 0, &net.OpError{Op: "closed", Err: errors.New("write on closed stream")}
	}

	return s.w.Write(b)
}

func (t *testSubStreamsRequest) rawRequest() (
	context.Context, dutil.InErrorFunc[error],
	context.Context, dutil.InErrorFunc[error],
	context.Context, dutil.InErrorFunc[error],
	Conn, Conn,
	Stream, Stream,
	func(time.Duration) (subStremasPool, context.Context, requestSubStreamsFunc, dutil.InErrorFunc[error]),
	openStreamFunc,
	func(),
) {
	ctx, ctxCancel := t.Ctx()

	rctx, rctxCancel := contextWithCancelCause(ctx)
	sctx, sctxCancel := contextWithCancelCause(ctx)

	_, _, rconn, sconn, closef := t.openConnections(rctx, sctx, nil, nil)
	rcstream, scstream, cclosef := t.openControlStreams(context.Background(), rconn, sconn)

	return ctx, ctxCancel,
		rctx, rctxCancel,
		sctx, sctxCancel,
		rconn, sconn,
		rcstream, scstream,
		func(timeout time.Duration) (subStremasPool, context.Context, requestSubStreamsFunc, dutil.InErrorFunc[error]) {
			pool := dutil.NewSingleLockedMap[subStreamsRequestUID, dutil.In2ErrorFunc[context.Context, Stream]]()

			pctx, requestf, cancel := newRequestSubStreamsWithPool(
				rctx,
				func() (context.Context, openStreamFunc) {
					return rconn.Context(), t.newFuncOpenStream(rconn.AcceptStream)
				},
				timeout,
				int64(runtime.GOMAXPROCS(0)),
				pool,
			)

			return pool, pctx, requestf, cancel
		},
		t.newFuncOpenStream(sconn.OpenStreamSync),
		func() {
			ctxCancel(nil)
			rctxCancel(nil)
			sctxCancel(nil)

			cclosef()
			closef()
		}
}

func (t *testSubStreamsRequest) serveOpenStream(
	ctx context.Context,
	openStream openStreamFunc,
	n uint64,
	uid subStreamsRequestUID,
	handler subStreamHandlerkFunc,
) (dutil.ErrorFunc, error) {
	eg, ectx := errgroup.WithContext(ctx)

	for si := range n {
		stream, err := openStream(ectx)
		if err != nil {
			return nil, err
		}

		eg.Go(func() error {
			if _, err := frame.WriteBytes(stream, uid[:]); err != nil {
				return closeStream(stream, err)
			}

			gerr := handler(ectx, si, stream)
			if err := closeStream(stream, gerr); err != nil {
				return err
			}

			return gerr
		})
	}

	return eg.Wait, nil
}

func (t *testSubStreamsRequest) initialServeSubStream(limit uint64, stream Stream) (uid subStreamsRequestUID, _ uint64, _ error) {
	u, err := readSubStreamRequestUID(stream)
	if err != nil {
		return uid, 0, err
	}
	uid = u

	n, _, err := frame.ReadUint64(stream)
	if err != nil {
		return uid, 0, err
	}

	nn := min(limit, n)

	_, err = frame.WriteUint64(stream, nn)
	if err != nil {
		return uid, 0, err
	}

	return uid, nn, nil
}

func (t *testSubStreamsRequest) newFuncOpenStream(f func(context.Context) (*quic.Stream, error)) func(context.Context) (Stream, error) {
	return func(ctx context.Context) (Stream, error) {
		s, err := f(ctx)

		return s, err
	}
}

func (t *testSubStreamsRequest) TestOK() {
	var gn uint64 = 3

	sentb := make([]string, gn)
	for i := range sentb {
		sentb[i] = util.UUID().String()
	}
	slices.Sort(sentb)

	t.Run("n=3", func() {
		n := gn

		_, _, rctx, rctxCancel, sctx, sctxCancel, _, _, rcstream, scstream, requestf, openStream, closef := t.rawRequest()
		defer closef()

		pool, _, request, _ := requestf(time.Minute)

		go func() {
			sctxCancel(contextEnsure(sctx, func(ctx context.Context) error {
				uid, _, err := t.initialServeSubStream(n, scstream)
				if err != nil {
					return err
				}

				rerr, err := readSubStreamError(scstream)
				if err != nil {
					return err
				}
				if rerr != nil {
					return rerr
				}

				swait, err := t.serveOpenStream(ctx, openStream, n, uid, func(_ context.Context, si uint64, stream Stream) error {
					_, err := frame.WriteBytes(stream, []byte(sentb[si]))
					return err
				})
				if err != nil {
					return err
				}

				err = func() error {
					if err := swait(); err != nil {
						return err
					}

					rerr, err = readSubStreamError(scstream)
					if err != nil {
						return err
					}
					if rerr != nil {
						return rerr
					}

					return nil
				}()

				return writeSubStreamError(scstream, err)
			}))
		}()

		var recvb []string

		t.NoError(request(rctx, rcstream, n, func(nn uint64) (subStreamHandlerkFunc, error) {
			if nn != n {
				return nil, errors.Errorf("nn != n; %d != %d", nn, n)
			}

			var l sync.Mutex

			return func(_ context.Context, _ uint64, stream Stream) error {
					_, b, err := frame.ReadBytes(stream)
					if err != nil {
						return err
					}

					l.Lock()
					recvb = append(recvb, string(b))
					l.Unlock()

					return nil
				},
				nil
		}))
		t.Equal(0, pool.Len())
		rctxCancel(nil)

		slices.Sort(recvb)
		t.Equal(sentb, recvb)

		select {
		case <-time.After(time.Second * 3):
			t.Fail("wait serve done")
		case <-sctx.Done():
			t.NoError(sctxCancel(nil))
		}
	})

	t.Run("n > nn", func() {
		var n, nn uint64 = 4, gn

		_, _, rctx, rctxCancel, sctx, sctxCancel, _, _, rcstream, scstream, requestf, openStream, closef := t.rawRequest()
		defer closef()

		pool, _, request, _ := requestf(time.Minute)

		go func() {
			sctxCancel(contextEnsure(sctx, func(ctx context.Context) error {
				uid, err := readSubStreamRequestUID(scstream)
				if err != nil {
					return err
				}

				rn, _, err := frame.ReadUint64(scstream)
				if err != nil {
					return err
				}
				if rn != n {
					return errors.Errorf("wrong request n, %d", rn)
				}

				_, err = frame.WriteUint64(scstream, nn)
				if err != nil {
					return err
				}

				rerr, err := readSubStreamError(scstream)
				if err != nil {
					return err
				}
				if rerr != nil {
					return rerr
				}

				swait, err := t.serveOpenStream(ctx, openStream, nn, uid, func(_ context.Context, si uint64, stream Stream) error {
					_, err := frame.WriteBytes(stream, []byte(sentb[si]))
					return err
				})
				if err != nil {
					return err
				}

				err = func() error {
					if err := swait(); err != nil {
						return err
					}

					rerr, err = readSubStreamError(scstream)
					if err != nil {
						return err
					}
					if rerr != nil {
						return rerr
					}

					return nil
				}()

				return writeSubStreamError(scstream, err)
			}))
		}()

		var recvb []string

		t.NoError(request(rctx, rcstream, n, func(sn uint64) (subStreamHandlerkFunc, error) {
			if nn != sn {
				return nil, errors.Errorf("nn != n; %d != %d", nn, sn)
			}

			var l sync.Mutex

			return func(_ context.Context, _ uint64, stream Stream) error {
					_, b, err := frame.ReadBytes(stream)
					if err != nil {
						return err
					}

					l.Lock()
					recvb = append(recvb, string(b))
					l.Unlock()

					return nil
				},
				nil
		}))
		t.Equal(0, pool.Len())
		rctxCancel(nil)

		slices.Sort(recvb)
		t.Equal(sentb, recvb)

		select {
		case <-time.After(time.Second * 3):
			t.Fail("wait serve done")
		case <-sctx.Done():
			t.NoError(sctxCancel(nil))
		}
	})

	t.Run("n=0 || nn=0", func() {
		t.Run("n=0", func() {
			var n uint64 = 0

			_, _, rctx, rctxCancel, sctx, sctxCancel, _, _, rcstream, scstream, _, _, closef := t.rawRequest()
			defer closef()

			go func() {
				sctxCancel(contextEnsure(sctx, func(ctx context.Context) error {
					_, _, err := t.initialServeSubStream(n, scstream)
					if err != nil {
						return err
					}

					rerr, err := readSubStreamError(scstream)
					if err != nil {
						return err
					}
					if rerr != nil {
						return rerr
					}

					return nil
				}))
			}()

			pool := dutil.NewSingleLockedMap[subStreamsRequestUID, dutil.In2ErrorFunc[context.Context, Stream]]()
			_, request, _ := newRequestSubStreamsWithPool(
				rctx,
				func() (context.Context, openStreamFunc) {
					return rctx, func(context.Context) (Stream, error) {
						return nil, errors.New("request AcceptStream called")
					}
				},
				time.Minute,
				3,
				pool,
			)

			t.NoError(request(rctx, rcstream, n, func(nn uint64) (subStreamHandlerkFunc, error) {
				if nn != n {
					return nil, errors.Errorf("nn != n; %d != %d", nn, n)
				}

				return nil, nil
			}))
			t.Equal(0, pool.Len())
			rctxCancel(nil)

			select {
			case <-time.After(time.Second * 3):
				t.Fail("wait serve done")
			case <-sctx.Done():
				t.NoError(sctxCancel(nil))
			}
		})

		t.Run("n=3 nn=0", func() {
			var n, nn uint64 = gn, 0

			_, _, rctx, rctxCancel, sctx, sctxCancel, _, _, rcstream, scstream, _, _, closef := t.rawRequest()
			defer closef()

			go func() {
				sctxCancel(contextEnsure(sctx, func(ctx context.Context) error {
					if _, err := readSubStreamRequestUID(scstream); err != nil {
						return err
					}

					rn, _, err := frame.ReadUint64(scstream)
					if err != nil {
						return err
					}
					if rn != n {
						return errors.Errorf("wrong request n, %d", rn)
					}

					_, err = frame.WriteUint64(scstream, nn)
					if err != nil {
						return err
					}

					rerr, err := readSubStreamError(scstream)
					if err != nil {
						return err
					}
					if rerr != nil {
						return rerr
					}

					return nil
				}))
			}()

			pool := dutil.NewSingleLockedMap[subStreamsRequestUID, dutil.In2ErrorFunc[context.Context, Stream]]()
			_, request, _ := newRequestSubStreamsWithPool(
				rctx,
				func() (context.Context, openStreamFunc) {
					return rctx, func(context.Context) (Stream, error) {
						return nil, errors.New("request AcceptStream called")
					}
				},
				time.Minute,
				3,
				pool,
			)

			t.NoError(request(rctx, rcstream, n, func(sn uint64) (subStreamHandlerkFunc, error) {
				if sn != nn {
					return nil, errors.Errorf("nn != n; %d != %d", sn, n)
				}

				return nil, nil
			}))
			t.Equal(0, pool.Len())
			rctxCancel(nil)

			select {
			case <-time.After(time.Second * 3):
				t.Fail("wait serve done")
			case <-sctx.Done():
				t.NoError(sctxCancel(nil))
			}
		})
	})
}

func (t *testSubStreamsRequest) TestRequestConnClosed() {
	t.Run("before new request", func() {
		_, _, rctx, _, _, _, rconn, _, rcstream, _, requestf, _, closef := t.rawRequest()
		defer closef()

		closeConn, checkConnErr := t.closeConnWithError(rconn)
		t.NoError(closeConn())

		_, pctx, request, pctxCancel := requestf(time.Minute)
		defer pctxCancel(nil)

		err := request(rctx, rcstream, 3, nil)

		t.NoError(checkConnErr(false, err))

		select {
		case <-time.After(time.Second):
			t.Fail("wait request loop stopped")
		case <-pctx.Done():
			t.ErrorIs(pctx.Err(), context.Canceled)
		}
	})

	t.Run("before serve", func() {
		var n uint64 = 3

		_, _, rctx, _, sctx, _, rconn, _, rcstream, _, requestf, _, closef := t.rawRequest()
		defer closef()

		closeConn, checkConnErr := t.closeConnWithError(rconn)

		pool, pctx, request, _ := requestf(time.Minute)

		go func() {
			_ = contextEnsure(sctx, func(ctx context.Context) error {
				t.IfErrPanicf(closeConn(), "close request conn")

				return nil
			})
		}()

		err := request(rctx, rcstream, n, func(nn uint64) (subStreamHandlerkFunc, error) {
			if nn != n {
				return nil, errors.Errorf("nn != n; %d != %d", nn, n)
			}

			return func(context.Context, uint64, Stream) error { return nil }, nil
		})
		t.Equal(0, pool.Len())
		t.NoError(checkConnErr(false, err))

		select {
		case <-time.After(time.Second):
			t.Fail("wait request loop stopped")
		case <-pctx.Done():
			t.ErrorIs(pctx.Err(), context.Canceled)
		}
	})

	t.Run("before serve OpenStream", func() {
		var n uint64 = 3

		_, _, rctx, _, sctx, _, rconn, _, rcstream, scstream, requestf, _, closef := t.rawRequest()
		defer closef()

		pool, pctx, request, _ := requestf(time.Minute)

		closeConn, checkConnErr := t.closeConnWithError(rconn)

		go func() {
			_ = contextEnsure(sctx, func(ctx context.Context) error {
				_, _, err := t.initialServeSubStream(n, scstream)
				if err != nil {
					return err
				}

				rerr, err := readSubStreamError(scstream)
				if err != nil {
					return err
				}
				if rerr != nil {
					return rerr
				}

				t.IfErrPanicf(closeConn(), "close request conn")

				return nil
			})
		}()

		var handlerCalled int64

		err := request(rctx, rcstream, n, func(nn uint64) (subStreamHandlerkFunc, error) {
			if nn != n {
				return nil, errors.Errorf("nn != n; %d != %d", nn, n)
			}

			return func(context.Context, uint64, Stream) error {
					atomic.AddInt64(&handlerCalled, 1)

					return nil
				},
				nil
		})
		t.Equal(0, pool.Len())
		checkConnErr(false, err)

		t.EqualValues(0, atomic.LoadInt64(&handlerCalled))

		select {
		case <-time.After(time.Second):
			t.Fail("wait request loop stopped")
		case <-pctx.Done():
			t.ErrorIs(pctx.Err(), context.Canceled)
		}
	})

	t.Run("during serve OpenStream", func() {
		var n uint64 = 3

		_, _, rctx, _, sctx, _, rconn, _, rcstream, scstream, requestf, openStream, closef := t.rawRequest()
		defer closef()

		pool, pctx, request, _ := requestf(time.Minute)

		closeConn, checkConnErr := t.closeConnWithError(rconn)

		go func() {
			_ = contextEnsure(sctx, func(ctx context.Context) error {
				uid, nn, err := t.initialServeSubStream(n, scstream)
				if err != nil {
					return err
				}

				rerr, err := readSubStreamError(scstream)
				if err != nil {
					return err
				}
				if rerr != nil {
					return rerr
				}

				var i int64
				swait, err := t.serveOpenStream(ctx,
					func(ctx context.Context) (Stream, error) {
						if atomic.AddInt64(&i, 1) == 2 {
							t.IfErrPanicf(closeConn(), "close request conn")
						}

						return openStream(ctx)
					},
					nn, uid,
					func(context.Context, uint64, Stream) error {
						return nil
					})
				if err != nil {
					return err
				}

				err = func() error {
					if err := swait(); err != nil {
						return err
					}

					rerr, err = readSubStreamError(scstream)
					if err != nil {
						return err
					}
					if rerr != nil {
						return rerr
					}

					return nil
				}()

				return writeSubStreamError(scstream, err)
			})
		}()

		err := request(rctx, rcstream, n, func(nn uint64) (subStreamHandlerkFunc, error) {
			if nn != n {
				return nil, errors.Errorf("nn != n; %d != %d", nn, n)
			}

			return func(context.Context, uint64, Stream) error { return nil }, nil
		})
		t.Equal(0, pool.Len())
		t.NoError(checkConnErr(false, err))

		select {
		case <-time.After(time.Second):
			t.Fail("wait request loop stopped")
		case <-pctx.Done():
			t.ErrorIs(pctx.Err(), context.Canceled)
		}
	})

	t.Run("before serve last response", func() {
		var n uint64 = 3

		_, _, rctx, _, sctx, _, rconn, _, rcstream, scstream, requestf, openStream, closef := t.rawRequest()
		defer closef()

		pool, pctx, request, _ := requestf(time.Minute)

		closeConn, checkConnErr := t.closeConnWithError(rconn)

		go func() {
			_ = contextEnsure(sctx, func(ctx context.Context) error {
				uid, nn, err := t.initialServeSubStream(n, scstream)
				if err != nil {
					return err
				}

				rerr, err := readSubStreamError(scstream)
				if err != nil {
					return err
				}
				if rerr != nil {
					return rerr
				}

				swait, err := t.serveOpenStream(ctx, openStream, nn, uid, func(context.Context, uint64, Stream) error {
					return nil
				})
				if err != nil {
					return err
				}

				err = func() error {
					if err := swait(); err != nil {
						return err
					}

					rerr, err = readSubStreamError(scstream)
					if err != nil {
						return err
					}
					if rerr != nil {
						return rerr
					}

					return nil
				}()

				t.IfErrPanicf(closeConn(), "close request conn")

				return writeSubStreamError(scstream, err)
			})
		}()

		var handlerCalled int64
		err := request(rctx, rcstream, n, func(nn uint64) (subStreamHandlerkFunc, error) {
			if nn != n {
				return nil, errors.Errorf("nn != n; %d != %d", nn, n)
			}

			return func(context.Context, uint64, Stream) error {
				atomic.AddInt64(&handlerCalled, 1)

				return nil
			}, nil
		})
		t.Equal(0, pool.Len())
		t.NoError(checkConnErr(false, err))

		t.EqualValues(n, atomic.LoadInt64(&handlerCalled))

		select {
		case <-time.After(time.Second):
			t.Fail("wait request loop stopped")
		case <-pctx.Done():
			t.ErrorIs(pctx.Err(), context.Canceled)
		}
	})

	t.Run("before requested", func() {
		var n uint64 = 3

		_, _, rctx, _, sctx, _, rconn, _, rcstream, scstream, requestf, openStream, closef := t.rawRequest()
		defer closef()

		pool, pctx, request, _ := requestf(time.Minute)

		go func() {
			_ = contextEnsure(sctx, func(ctx context.Context) error {
				uid, nn, err := t.initialServeSubStream(n, scstream)
				if err != nil {
					return err
				}

				rerr, err := readSubStreamError(scstream)
				if err != nil {
					return err
				}
				if rerr != nil {
					return rerr
				}

				swait, err := t.serveOpenStream(ctx, openStream, nn, uid, func(context.Context, uint64, Stream) error {
					return nil
				})
				if err != nil {
					return err
				}

				err = func() error {
					if err := swait(); err != nil {
						return err
					}

					rerr, err = readSubStreamError(scstream)
					if err != nil {
						return err
					}
					if rerr != nil {
						return rerr
					}

					return nil
				}()

				return writeSubStreamError(scstream, err)
			})
		}()

		var handlerCalled int64
		t.NoError(request(rctx, rcstream, n, func(nn uint64) (subStreamHandlerkFunc, error) {
			if nn != n {
				return nil, errors.Errorf("nn != n; %d != %d", nn, n)
			}

			return func(context.Context, uint64, Stream) error {
				atomic.AddInt64(&handlerCalled, 1)

				return nil
			}, nil
		}))
		t.EqualValues(n, atomic.LoadInt64(&handlerCalled))

		t.NoError(pctx.Err())

		closeConn, checkConnErr := t.closeConnWithError(rconn)
		t.NoError(closeConn())

		handlerCalled = 0
		err := request(rctx, rcstream, n, func(nn uint64) (subStreamHandlerkFunc, error) {
			if nn != n {
				return nil, errors.Errorf("nn != n; %d != %d", nn, n)
			}

			return func(context.Context, uint64, Stream) error {
				atomic.AddInt64(&handlerCalled, 1)

				return nil
			}, nil
		})
		t.Equal(0, pool.Len())
		t.NoError(checkConnErr(false, err))

		t.EqualValues(0, atomic.LoadInt64(&handlerCalled))

		select {
		case <-time.After(time.Second):
			t.Fail("wait request loop stopped")
		case <-pctx.Done():
			t.ErrorIs(pctx.Err(), context.Canceled)
		}
	})
}

func (t *testSubStreamsRequest) TestMainContextCanceled() {
	t.Run("before new request", func() {
		_, ctxCancel, rctx, _, _, _, _, _, rcstream, _, requestf, _, closef := t.rawRequest()
		defer closef()

		_ = ctxCancel(nil)

		_, pctx, request, _ := requestf(time.Minute)
		t.ErrorIs(pctx.Err(), context.Canceled)

		err := request(rctx, rcstream, 3, nil)
		t.ErrorIs(err, context.Canceled)

		select {
		case <-time.After(time.Second):
			t.Fail("wait request loop stopped")
		case <-pctx.Done():
			t.ErrorIs(pctx.Err(), context.Canceled)
		}
	})

	t.Run("before serve", func() {
		var n uint64 = 3

		_, ctxCancel, rctx, _, sctx, _, _, _, rcstream, _, requestf, _, closef := t.rawRequest()
		defer closef()

		pool, pctx, request, _ := requestf(time.Minute)

		go func() {
			_ = contextEnsure(sctx, func(ctx context.Context) error {
				_ = ctxCancel(nil)

				return nil
			})
		}()

		err := request(rctx, rcstream, n, func(nn uint64) (subStreamHandlerkFunc, error) {
			if nn != n {
				return nil, errors.Errorf("nn != n; %d != %d", nn, n)
			}

			return func(context.Context, uint64, Stream) error { return nil }, nil
		})
		t.Equal(0, pool.Len())
		t.ErrorIs(err, context.Canceled)

		select {
		case <-time.After(time.Second):
			t.Fail("wait request loop stopped")
		case <-pctx.Done():
			t.ErrorIs(pctx.Err(), context.Canceled)
		}
	})

	t.Run("during serve", func() {
		var n uint64 = 3

		_, ctxCancel, rctx, _, sctx, _, _, _, rcstream, scstream, requestf, _, closef := t.rawRequest()
		defer closef()

		pool, pctx, request, _ := requestf(time.Minute)

		go func() {
			_ = contextEnsure(sctx, func(ctx context.Context) error {
				_, _, err := t.initialServeSubStream(n, scstream)
				if err != nil {
					return err
				}

				rerr, err := readSubStreamError(scstream)
				if err != nil {
					return err
				}
				if rerr != nil {
					return rerr
				}

				ctxCancel(nil)

				return nil
			})
		}()

		var handlerCalled int64

		err := request(rctx, rcstream, n, func(nn uint64) (subStreamHandlerkFunc, error) {
			if nn != n {
				return nil, errors.Errorf("nn != n; %d != %d", nn, n)
			}

			return func(context.Context, uint64, Stream) error {
					atomic.AddInt64(&handlerCalled, 1)

					return nil
				},
				nil
		})
		t.Equal(0, pool.Len())
		t.ErrorIs(err, context.Canceled)

		t.EqualValues(0, atomic.LoadInt64(&handlerCalled))

		select {
		case <-time.After(time.Second):
			t.Fail("wait request loop stopped")
		case <-pctx.Done():
			t.ErrorIs(pctx.Err(), context.Canceled)
		}
	})

	t.Run("before requested", func() {
		var n uint64 = 3

		_, ctxCancel, rctx, _, sctx, _, _, _, rcstream, scstream, requestf, openStream, closef := t.rawRequest()
		defer closef()

		pool, pctx, request, _ := requestf(time.Minute)

		go func() {
			_ = contextEnsure(sctx, func(ctx context.Context) error {
				uid, nn, err := t.initialServeSubStream(n, scstream)
				if err != nil {
					return err
				}

				rerr, err := readSubStreamError(scstream)
				if err != nil {
					return err
				}
				if rerr != nil {
					return rerr
				}

				swait, err := t.serveOpenStream(ctx, openStream, nn, uid, func(context.Context, uint64, Stream) error {
					return nil
				})
				if err != nil {
					return err
				}

				err = func() error {
					if err := swait(); err != nil {
						return err
					}

					rerr, err = readSubStreamError(scstream)
					if err != nil {
						return err
					}
					if rerr != nil {
						return rerr
					}

					return nil
				}()

				return writeSubStreamError(scstream, err)
			})
		}()

		var handlerCalled int64
		t.NoError(request(rctx, rcstream, n, func(nn uint64) (subStreamHandlerkFunc, error) {
			if nn != n {
				return nil, errors.Errorf("nn != n; %d != %d", nn, n)
			}

			return func(context.Context, uint64, Stream) error {
				atomic.AddInt64(&handlerCalled, 1)

				return nil
			}, nil
		}))
		t.EqualValues(n, atomic.LoadInt64(&handlerCalled))

		t.NoError(pctx.Err())

		ctxCancel(nil)

		handlerCalled = 0
		err := request(rctx, rcstream, n, func(nn uint64) (subStreamHandlerkFunc, error) {
			if nn != n {
				return nil, errors.Errorf("nn != n; %d != %d", nn, n)
			}

			return func(context.Context, uint64, Stream) error {
				atomic.AddInt64(&handlerCalled, 1)

				return nil
			}, nil
		})
		t.Equal(0, pool.Len())
		t.ErrorIs(err, context.Canceled)

		t.EqualValues(0, atomic.LoadInt64(&handlerCalled))

		select {
		case <-time.After(time.Second):
			t.Fail("wait request loop stopped")
		case <-pctx.Done():
			t.ErrorIs(pctx.Err(), context.Canceled)
		}
	})
}

func (t *testSubStreamsRequest) TestContextCanceled() {
	t.Run("before new request", func() {
		_, _, rctx, rctxCancel, _, _, _, _, rcstream, _, requestf, _, closef := t.rawRequest()
		defer closef()

		_ = rctxCancel(nil)

		_, pctx, request, _ := requestf(time.Minute)
		t.ErrorIs(pctx.Err(), context.Canceled)

		err := request(rctx, rcstream, 3, nil)
		t.ErrorIs(err, context.Canceled)

		select {
		case <-time.After(time.Second):
			t.Fail("wait request loop stopped")
		case <-pctx.Done():
			t.ErrorIs(pctx.Err(), context.Canceled)
		}
	})

	t.Run("before serve", func() {
		var n uint64 = 3

		_, _, rctx, rctxCancel, sctx, _, _, _, rcstream, _, requestf, _, closef := t.rawRequest()
		defer closef()

		pool, pctx, request, _ := requestf(time.Minute)

		go func() {
			_ = contextEnsure(sctx, func(ctx context.Context) error {
				_ = rctxCancel(nil)

				return nil
			})
		}()

		err := request(rctx, rcstream, n, func(nn uint64) (subStreamHandlerkFunc, error) {
			if nn != n {
				return nil, errors.Errorf("nn != n; %d != %d", nn, n)
			}

			return func(context.Context, uint64, Stream) error { return nil }, nil
		})
		t.Equal(0, pool.Len())
		t.ErrorIs(err, context.Canceled)

		select {
		case <-time.After(time.Second):
			t.Fail("wait request loop stopped")
		case <-pctx.Done():
			t.ErrorIs(pctx.Err(), context.Canceled)
		}
	})

	t.Run("during serve", func() {
		var n uint64 = 3

		_, _, rctx, rctxCancel, sctx, _, _, _, rcstream, scstream, requestf, _, closef := t.rawRequest()
		defer closef()

		pool, pctx, request, _ := requestf(time.Minute)

		go func() {
			_ = contextEnsure(sctx, func(ctx context.Context) error {
				_, _, err := t.initialServeSubStream(n, scstream)
				if err != nil {
					return err
				}

				rerr, err := readSubStreamError(scstream)
				if err != nil {
					return err
				}
				if rerr != nil {
					return rerr
				}

				rctxCancel(nil)

				return nil
			})
		}()

		var handlerCalled int64

		err := request(rctx, rcstream, n, func(nn uint64) (subStreamHandlerkFunc, error) {
			if nn != n {
				return nil, errors.Errorf("nn != n; %d != %d", nn, n)
			}

			return func(context.Context, uint64, Stream) error {
					atomic.AddInt64(&handlerCalled, 1)

					return nil
				},
				nil
		})
		t.Equal(0, pool.Len())
		t.ErrorIs(err, context.Canceled)

		t.EqualValues(0, atomic.LoadInt64(&handlerCalled))

		select {
		case <-time.After(time.Second):
			t.Fail("wait request loop stopped")
		case <-pctx.Done():
			t.ErrorIs(pctx.Err(), context.Canceled)
		}
	})
}

func (t *testSubStreamsRequest) TestWriteError() {
	t.Run("n callback error", func() {
		var n uint64 = 3

		_, _, rctx, _, sctx, sctxCancel, _, _, rcstream, scstream, requestf, _, closef := t.rawRequest()
		defer closef()

		_, pctx, request, _ := requestf(time.Minute)

		errmsg := "showme"
		var rerrmsg string

		go func() {
			err := contextEnsure(sctx, func(ctx context.Context) error {
				_, _, err := t.initialServeSubStream(n, scstream)
				if err != nil {
					return err
				}

				rerr, err := readSubStreamError(scstream)
				if err != nil {
					return err
				}
				if rerr != nil {
					rerrmsg = rerr.Error()

					return rerr
				}

				_, _ = readSubStreamError(scstream)

				return nil
			})

			sctxCancel(err)
			writeSubStreamError(scstream, err)
		}()

		t.NoError(request(rctx, rcstream, n, func(uint64) (subStreamHandlerkFunc, error) {
			return nil, errors.New(errmsg)
		}))

		select {
		case <-time.After(time.Second * 3):
			t.Fail("wait serve done")
		case <-sctx.Done():
			t.ErrorContains(contextCause(sctx), errmsg)
		}

		t.Equal(errmsg, rerrmsg)

		select {
		case <-time.After(time.Second):
		case <-pctx.Done():
			t.Fail("unexpected request loop stopped")
		}
	})

	t.Run("handler error", func() {
		var n uint64 = 3

		_, _, rctx, _, sctx, sctxCancel, _, _, rcstream, scstream, requestf, openStream, closef := t.rawRequest()
		defer closef()

		_, pctx, request, _ := requestf(time.Minute)

		errmsg := "showme"
		var rerrmsg string

		go func() {
			err := contextEnsure(sctx, func(ctx context.Context) error {
				uid, _, err := t.initialServeSubStream(n, scstream)
				if err != nil {
					return err
				}

				rerr, err := readSubStreamError(scstream)
				if err != nil {
					return err
				}
				if rerr != nil {
					return rerr
				}

				swait, err := t.serveOpenStream(ctx, openStream, n, uid, func(context.Context, uint64, Stream) error {
					return nil
				})
				if err != nil {
					return err
				}

				if err := swait(); err != nil {
					return err
				}

				rerr, err = readSubStreamError(scstream)
				if err != nil {
					return err
				}

				if rerr != nil {
					rerrmsg = rerr.Error()
					return rerr
				}

				return nil
			})

			sctxCancel(err)
			writeSubStreamError(scstream, err)
		}()

		err := request(rctx, rcstream, n, func(nn uint64) (subStreamHandlerkFunc, error) {
			return func(_ context.Context, i uint64, _ Stream) error {
					if i == nn-1 {
						return errors.New(errmsg)
					}

					return nil
				},
				nil
		})
		t.ErrorContains(err, errmsg)

		select {
		case <-time.After(time.Second * 3):
			t.Fail("wait serve done")
		case <-sctx.Done():
			t.ErrorContains(contextCause(sctx), errmsg)
		}

		t.Equal(errmsg, rerrmsg)

		select {
		case <-time.After(time.Second):
		case <-pctx.Done():
			t.Fail("unexpected request loop stopped")
		}
	})
}

func (t *testSubStreamsRequest) TestServeError() {
	t.Run("handler error", func() {
		var n uint64 = 3

		_, _, rctx, _, sctx, sctxCancel, _, _, rcstream, scstream, requestf, openStream, closef := t.rawRequest()
		defer closef()

		_, pctx, request, _ := requestf(time.Minute)

		errmsg := "showme"

		go func() {
			err := contextEnsure(sctx, func(ctx context.Context) error {
				uid, _, err := t.initialServeSubStream(n, scstream)
				if err != nil {
					return err
				}

				rerr, err := readSubStreamError(scstream)
				if err != nil {
					return err
				}
				if rerr != nil {
					return rerr
				}

				swait, err := t.serveOpenStream(ctx, openStream, n, uid, func(_ context.Context, i uint64, _ Stream) error {
					if i == n-1 {
						return errors.New(errmsg)
					}

					return nil
				})
				if err != nil {
					return err
				}

				if err := swait(); err != nil {
					return err
				}

				rerr, err = readSubStreamError(scstream)
				if err != nil {
					return err
				}

				if rerr != nil {
					return rerr
				}

				return nil
			})

			sctxCancel(err)
			writeSubStreamError(scstream, err)
		}()

		err := request(rctx, rcstream, n, func(uint64) (subStreamHandlerkFunc, error) {
			return func(context.Context, uint64, Stream) error { return nil }, nil
		})
		t.ErrorContains(err, errmsg)

		select {
		case <-time.After(time.Second * 3):
			t.Fail("wait serve done")
		case <-sctx.Done():
			t.ErrorContains(contextCause(sctx), errmsg)
		}

		select {
		case <-time.After(time.Second):
		case <-pctx.Done():
			t.Fail("unexpected request loop stopped")
		}
	})

	t.Run("serve error", func() {
		var n uint64 = 3

		_, _, rctx, _, sctx, sctxCancel, _, _, rcstream, scstream, requestf, openStream, closef := t.rawRequest()
		defer closef()

		_, pctx, request, _ := requestf(time.Minute)

		errmsg := "showme"

		go func() {
			err := contextEnsure(sctx, func(ctx context.Context) error {
				uid, _, err := t.initialServeSubStream(n, scstream)
				if err != nil {
					return err
				}

				rerr, err := readSubStreamError(scstream)
				if err != nil {
					return err
				}
				if rerr != nil {
					return rerr
				}

				swait, err := t.serveOpenStream(ctx, openStream, n, uid, func(_ context.Context, i uint64, _ Stream) error {
					return nil
				})
				if err != nil {
					return err
				}

				if err := swait(); err != nil {
					return err
				}

				rerr, err = readSubStreamError(scstream)
				if err != nil {
					return err
				}

				if rerr != nil {
					return rerr
				}

				return nil
			})

			sctxCancel(err)
			writeSubStreamError(scstream, errors.New(errmsg))
		}()

		err := request(rctx, rcstream, n, func(uint64) (subStreamHandlerkFunc, error) {
			return func(context.Context, uint64, Stream) error { return nil }, nil
		})
		t.ErrorContains(err, errmsg)

		select {
		case <-time.After(time.Second * 3):
			t.Fail("wait serve done")
		case <-sctx.Done():
		}

		select {
		case <-time.After(time.Second):
		case <-pctx.Done():
			t.Fail("unexpected request loop stopped")
		}
	})
}

func (t *testSubStreamsRequest) TestN() {
	t.T().Skip()

	serve := func(conn Conn, stream Stream, n uint64, sentb []string) error {
		defer closeStream(stream, nil)

		ctx := conn.Context()

		uid, _, err := t.initialServeSubStream(n, stream)
		if err != nil {
			return err
		}

		rerr, err := readSubStreamError(stream)
		if err != nil {
			return err
		}
		if rerr != nil {
			return rerr
		}

		swait, err := t.serveOpenStream(ctx,
			t.newFuncOpenStream(conn.OpenStreamSync),
			n, uid,
			func(_ context.Context, i uint64, stream Stream) error {
				if i%(n/2) == 0 || i == n-1 {
					fmt.Fprintf(os.Stderr, "s%d ", i)
				}

				_, err := frame.WriteBytes(stream, []byte(sentb[i]))
				return err
			})
		if err != nil {
			return err
		}

		err = func() error {
			if err := swait(); err != nil {
				return err
			}

			rerr, err = readSubStreamError(stream)
			if err != nil {
				return err
			}
			if rerr != nil {
				return rerr
			}

			return nil
		}()

		gerr := writeSubStreamError(stream, err)

		if err := closeStream(stream, gerr); err != nil {
			return err
		}

		return gerr
	}

	runN := func(ctx context.Context, si uint64, sentb []string, n uint64, sconn Conn, rcstream, scstream Stream, request requestSubStreamsFunc) error {
		var opened uint64

		defer func() func() {
			t.Print("> request/serve: %d, n=%d", si, n)
			return func() {
				t.Print("< request/serve: %d, opened=%d", si, atomic.LoadUint64(&opened))
			}
		}()()

		servech := make(chan error, 1)
		go func() {
			servech <- serve(sconn, scstream, n, sentb)
		}()

		recvb := make([]string, 0, n)
		defer clear(recvb)

		err := request(ctx, rcstream, n, func(nn uint64) (subStreamHandlerkFunc, error) {
			if nn != n {
				return nil, errors.Errorf("nn != n; %d != %d", nn, n)
			}

			var recvl sync.Mutex

			return func(_ context.Context, i uint64, stream Stream) error {
					atomic.AddUint64(&opened, 1)
					if i%(n/2) == 0 || i == n-1 {
						fmt.Fprintf(os.Stderr, "r%d ", i)
					}

					_, b, err := frame.ReadBytes(stream)
					if err != nil {
						return err
					}

					recvl.Lock()
					recvb = append(recvb, string(b))
					recvl.Unlock()

					return nil
				},
				nil
		})
		if err != nil {
			return err
		}

		slices.Sort(recvb)
		if !slices.Equal(sentb, recvb) {
			return errors.New("sentb != recvb")
		}

		select {
		case err := <-servech:
			return err
		case <-ctx.Done():
			return contextCause(ctx)
		}
	}

	run := func(sn, n uint64) func() {
		return func() {
			defer func() {
				t.T().Logf("< run: sn=%d n=%d", sn, n)
			}()

			ctx, ctxCancel := t.Ctx()
			defer ctxCancel(nil)

			sentb := make([]string, 0, n)
			defer clear(sentb)
			for i := range n {
				sentb = append(sentb, string([]byte{byte(i)}))
			}
			slices.Sort(sentb)

			t.T().Logf("> prepare conns")
			_, _, rconn, sconn, closef := t.openConnections(ctx, context.Background(), nil, nil)
			defer closef()
			t.T().Logf("< prepare conns")

			_, request, _ := defaultRequestSubStreams(
				ctx,
				func() (context.Context, openStreamFunc) {
					return rconn.Context(), t.newFuncOpenStream(rconn.AcceptStream)
				},
				time.Second*2,
			)

			eg, ectx := errgroup.WithContext(ctx)

			for i := range sn {
				eg.Go(func() error {
					rcstream, scstream, cclosef := t.openControlStreams(ctx, rconn, sconn)
					defer cclosef()

					return runN(ectx, i, sentb, n, sconn, rcstream, scstream, request)
				})
			}

			t.NoError(eg.Wait())
		}
	}

	// t.Run("sn=3 n=63333", run(3, 63333))
	// t.Run("sn=1 n=3", run(1, 3))
	repeat := 1000000
	var sn, n uint64 = 1, 500

	for i := range repeat {
		t.Run(fmt.Sprintf("%d/%d: sn=%d n=%d", i+1, repeat, sn, n), run(sn, n))
		<-time.After(time.Second)
	}

	t.NoError(pprofMem("/tmp/m.out"))
}

func TestSubStreamsRequest(t *testing.T) {
	suite.Run(t, new(testSubStreamsRequest))
}
