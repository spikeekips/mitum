package quicstream

import (
	"context"
	"io"
	"net"
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
)

/* FIXME
- context canceled; request/serve
	- before request, serve
	- during request, serve
	- during handler; request/serve
- packet conn closed; request/serve
	- before request, serve
	- during request, serve
	- during handler; request/serve
- control stream closed; request/serve
	- before request, serve
	- during request, serve
	- during handler; request/serve
- sub streams
	- after handler, sub streams closed
- timeout; context.WithTimeout/without context.WithTimeout/quic config.MaxIdleTimeout
	- timeout handler; request/serve
	- request:
		- but serve
		- serve started, but timeout
	- serve
		- no request
		- request started, but timeout
- returned context; request/serv
*/

type testSubStreams struct {
	BaseTest
}

func (t *testSubStreams) openConnections(
	serveCtx, reqCtx context.Context,
	reqQuicConfig, serveQuicConfig *quic.Config,
) (net.PacketConn, net.PacketConn, QuicConn, QuicConn, func()) {
	if serveCtx == nil {
		serveCtx = context.Background()
	}
	if reqCtx == nil {
		reqCtx = context.Background()
	}

	if serveQuicConfig == nil {
		serveQuicConfig = DefaultQuicConfig()
	}
	if reqQuicConfig == nil {
		reqQuicConfig = DefaultQuicConfig()
	}

	servepconn, l := t.RandomListener(nil, serveQuicConfig)
	reqpconn := t.RandomPacketConn()

	servech := make(chan QuicConn, 1)
	go func() {
		conn, err := l.Accept(serveCtx)
		t.PrintIfErrPanic(err, "accept")

		servech <- conn
	}()

	reqConn, err := dialWithPacketConn(reqCtx, reqpconn, servepconn.LocalAddr(), t.NewTLSConfig(), reqQuicConfig)
	t.PrintIfErrPanic(err, "req dial")

	serveconn := <-servech

	return servepconn, reqpconn, serveconn, reqConn, sync.OnceFunc(func() {
		err := servepconn.Close()
		t.PrintIfErrCaller(err, 2, "serve: PacketConn closed")
		err = reqpconn.Close()
		t.PrintIfErrCaller(err, 2, "req: PacketConn closed")
	})
}

func (t *testSubStreams) openStreams(
	ctx context.Context,
	reqQuicConfig, serveQuicConfig *quic.Config,
) (
	net.PacketConn, net.PacketConn, QuicConn, QuicConn,
	*quic.Stream, *quic.Stream,
	func(),
) {
	if ctx == nil {
		ctx = context.Background()
	}
	servepconn, reqpconn, serveconn, reqconn, closef := t.openConnections(ctx, ctx, reqQuicConfig, serveQuicConfig)

	sctx, sctxCancel := context.WithTimeout(ctx, time.Second*3)
	defer sctxCancel()

	servestreamch := make(chan *quic.Stream, 1)
	go func() {
		stream, err := contextEnsureWaitT(sctx, func(ctx context.Context) (*quic.Stream, error) {
			stream, err := serveconn.AcceptStream(ctx)
			if err != nil {
				return nil, err
			}
			t.PrintIfErr(err, "serve conn: AcceptStream")

			_, r, err := frame.ReadBytes(stream)
			t.PrintIfErr(err, "serve stream: read")
			_, err = io.ReadAll(r)
			t.PrintIfErr(err, "serve stream: read all")

			return stream, err
		})
		t.IfErrPanicf(err, "serve")

		servestreamch <- stream
	}()

	reqstream, err := reqconn.OpenStream()
	t.PrintIfErrPanic(err, "req conn: OpenStream")
	_, err = frame.WriteBytes(reqstream, []byte{0})
	t.IfErrPanicf(err, "req stream: write")

	var servestream *quic.Stream

	select {
	case <-sctx.Done():
		t.PrintIfErrPanic(err, "wait serve stream")
	case servestream = <-servestreamch:
	}

	return servepconn, reqpconn, serveconn, reqconn, servestream, reqstream, sync.OnceFunc(func() {
		closeStream(servestream, nil)
		closeStream(reqstream, nil)
		closeConnection(serveconn, nil)
		closeConnection(reqconn, nil)
		closef()
	})
}

func (t *testSubStreams) defaultRequest(
	ctx context.Context,
	reqconn QuicConn,
	pool *dutil.SingleLockedMap[subStreamsRequestUID, *subStreamPoolItem],
) (
	RequestSubStreamsFunc,
	dutil.InErrorFunc[error],
) {
	if ctx == nil {
		ctx = context.Background()
	}

	if pool == nil {
		pool = dutil.NewSingleLockedMap[subStreamsRequestUID, *subStreamPoolItem]()
	}

	f, c := newRequestSubStreamsWithPool(ctx, reqconn.AcceptStream, time.Second*2, pool)

	return f, c
}

func (t *testSubStreams) requestServeHandlers(
	ctx context.Context,
	reqQuicConfig, serveQuicConfig *quic.Config,
	n, limit uint64,
	pool *dutil.SingleLockedMap[subStreamsRequestUID, *subStreamPoolItem],
) (
	net.PacketConn, net.PacketConn, QuicConn, QuicConn,
	*quic.Stream, *quic.Stream,
	RequestSubStreamsFunc,
	ServeSubStreamsFunc,
	func(SubStreamsHandlerFunc, SubStreamsHandlerFunc) error,
	func(),
) {
	servepconn, reqpconn, serveconn, reqconn, servestream, reqstream, closef := t.openStreams(ctx, reqQuicConfig, serveQuicConfig)

	request, closerequest := t.defaultRequest(ctx, reqconn, pool)
	serve := newServeSubStreams(serveconn.OpenStreamSync, limit)

	doHandler := func(reqhandler, servehandler SubStreamsHandlerFunc) error {
		serveDone := make(chan error, 1)

		var servehandlerCalled, reqhandlerCalled int64

		go func() {
			serveh, err := serve(ctx, servestream)
			if err != nil {
				serveDone <- err

				return
			}

			_, err = serveh(ctx, func(ctx context.Context, streams []*quic.Stream) (context.Context, error) {
				t.Print("serve handler: streams=%d", len(streams))

				if !atomic.CompareAndSwapInt64(&servehandlerCalled, 0, 1) {
					return ctx, errors.New("multiple serve handler called")
				}

				if servehandler != nil {
					return servehandler(ctx, streams)
				}

				return ctx, nil
			})

			serveDone <- err
		}()

		reqh, err := request(ctx, reqstream, n)
		if err != nil {
			return err
		}

		if _, err := reqh(ctx, func(ctx context.Context, streams []*quic.Stream) (context.Context, error) {
			t.Print("request handler: streams=%d", len(streams))

			if !atomic.CompareAndSwapInt64(&reqhandlerCalled, 0, 1) {
				return ctx, errors.New("multiple req handler called")
			}

			if servehandler != nil {
				return reqhandler(ctx, streams)
			}

			return ctx, nil
		}); err != nil {
			return err
		}

		select {
		case <-time.After(time.Second * 3):
			return errors.New("wait serve handler")
		case err := <-serveDone:
			return err
		}
	}

	return servepconn, reqpconn, serveconn, reqconn, servestream, reqstream,
		request, serve,
		doHandler,
		sync.OnceFunc(func() {
			closerequest(nil)

			closef()
		})
}

func (t *testSubStreams) TestRequestNumber() {
	run := func(n, limit uint64, servehandler, reqhandler SubStreamsHandlerFunc) {
		t.T().Logf("n=%d limit=%d", n, limit)

		ctx, ctxCancel := t.Ctx()
		defer ctxCancel(nil)

		var reqn, serven uint64

		_, _, _, _, _, _, _, _, doHandler, closef := t.requestServeHandlers(ctx, nil, nil, n, limit, nil)
		defer closef()

		err := doHandler(
			func(ctx context.Context, streams []*quic.Stream) (context.Context, error) {
				ok := atomic.CompareAndSwapUint64(&reqn, 0, uint64(len(streams)))
				t.IfTruePanic(!ok, "multiple request handler called")

				if servehandler != nil {
					return reqhandler(ctx, streams)
				}

				return ctx, nil
			},
			func(ctx context.Context, streams []*quic.Stream) (context.Context, error) {
				ok := atomic.CompareAndSwapUint64(&serven, 0, uint64(len(streams)))
				t.IfTruePanic(!ok, "multiple serve handler called")

				if servehandler != nil {
					return servehandler(ctx, streams)
				}

				return ctx, nil
			},
		)
		t.NoError(err)

		expected := min(n, limit)
		t.T().Logf("n=%d expected=%d", n, expected)

		t.Equal(expected, atomic.LoadUint64(&reqn))
		t.Equal(expected, atomic.LoadUint64(&serven))
	}

	t.Run("n < limit", func() { run(1, 2, nil, nil) })
	t.Run("n == limit", func() { run(2, 2, nil, nil) })
	t.Run("n > limit", func() { run(3, 2, nil, nil) })
	t.Run("n = 0", func() {
		var servehandlerCalled, reqhandlerCalled int64

		run(0, 2,
			func(ctx context.Context, streams []*quic.Stream) (context.Context, error) {
				_ = atomic.CompareAndSwapInt64(&servehandlerCalled, 0, 1)

				return ctx, nil
			},
			func(ctx context.Context, streams []*quic.Stream) (context.Context, error) {
				_ = atomic.CompareAndSwapInt64(&reqhandlerCalled, 0, 1)

				return ctx, nil
			},
		)

		t.EqualValues(1, atomic.LoadInt64(&servehandlerCalled))
		t.EqualValues(1, atomic.LoadInt64(&reqhandlerCalled))
	})
	t.Run("limit = 0", func() {
		var servehandlerCalled, reqhandlerCalled int64

		run(2, 0,
			func(ctx context.Context, streams []*quic.Stream) (context.Context, error) {
				_ = atomic.CompareAndSwapInt64(&servehandlerCalled, 0, 1)

				return ctx, nil
			},
			func(ctx context.Context, streams []*quic.Stream) (context.Context, error) {
				_ = atomic.CompareAndSwapInt64(&reqhandlerCalled, 0, 1)

				return ctx, nil
			},
		)

		t.EqualValues(1, atomic.LoadInt64(&servehandlerCalled))
		t.EqualValues(1, atomic.LoadInt64(&reqhandlerCalled))
	})
}

func (t *testSubStreams) TestRequest() {
	checkPacketConnClosedErr := func(err error) {
		if err == nil {
			t.Error(err)

			return
		}

		var opErr *net.OpError

		switch {
		case errors.Is(err, quic.ErrTransportClosed):
		case errors.As(err, &opErr):
		default:
			t.Fail("unexpected error", "%T %+v", err, err)
		}
	}

	t.Run("local packet conn closed", func() {
		t.Run("before request", func() {
			_, reqpconn, _, reqconn, _, reqstream, closef := t.openStreams(nil, nil, nil)
			defer closef()

			ctx, ctxCancel := t.Ctx()
			defer ctxCancel(nil)

			request, closerequest := newRequestSubStreams(context.Background(), reqconn.AcceptStream, time.Minute)
			defer closerequest(nil)

			t.T().Log("local packet conn closed")
			t.NoError(reqpconn.Close())

			_, err := request(ctx, reqstream, 3)
			checkPacketConnClosedErr(err)
		})

		t.Run("before request; n == 0", func() {
			_, reqpconn, _, reqconn, _, reqstream, closef := t.openStreams(nil, nil, nil)
			defer closef()

			ctx, ctxCancel := t.Ctx()
			defer ctxCancel(nil)

			request, closerequest := newRequestSubStreams(context.Background(), reqconn.AcceptStream, time.Minute)
			defer closerequest(nil)

			t.T().Log("local packet conn closed")
			t.NoError(reqpconn.Close())

			_, err := request(ctx, reqstream, 0)
			checkPacketConnClosedErr(err)
		})

		t.Run("during request", func() {
			ctx, ctxCancel := t.Ctx()
			defer ctxCancel(nil)

			_, reqpconn, _, reqconn, servestream, reqstream, closef := t.openStreams(ctx, nil, nil)
			defer closef()

			go func() {
				n, _, err := frame.ReadUint64(servestream)
				t.IfErrPanicf(err, "serve: read n")

				_, err = frame.WriteUint64(servestream, n)
				t.IfErrPanicf(err, "serve: write n")

				t.Print("local packet conn closed before reading uid")
				t.IfErrPanicf(reqpconn.Close(), "close local packat conn")
			}()

			request, closerequest := t.defaultRequest(ctx, reqconn, nil)
			defer closerequest(nil)

			_, err := request(ctx, reqstream, 3)
			checkPacketConnClosedErr(err)
		})

		t.Run("during accept stream", func() {
			ctx, ctxCancel := t.Ctx()
			defer ctxCancel(nil)

			_, reqpconn, _, reqconn, servestream, reqstream, closef := t.openStreams(ctx, nil, nil)
			defer closef()

			go func() {
				n, _, err := frame.ReadUint64(servestream)
				t.IfErrPanicf(err, "serve: read n")

				_, err = frame.WriteUint64(servestream, n)
				t.IfErrPanicf(err, "serve: write n")

				_, err = readSubStreamRequestUID(servestream)
				t.IfErrPanicf(err, "serve: read uid")

				t.Print("local packet conn closed before accept stream")
				t.IfErrPanicf(reqpconn.Close(), "close local packat conn")
			}()

			request, closerequest := t.defaultRequest(ctx, reqconn, nil)
			defer closerequest(nil)

			_, err := request(ctx, reqstream, 3)
			checkPacketConnClosedErr(err)
		})

		pconnClosedDuringHandler := func(isReq, reqh, serveh bool) {
			quicConfig := DefaultQuicConfig()
			quicConfig.MaxIdleTimeout = time.Millisecond * 333

			ctx, ctxCancel := t.Ctx()
			defer ctxCancel(nil)

			reqpconn, servepconn, _, _, _, _, _, _, doHandler, closef := t.requestServeHandlers(ctx, quicConfig, quicConfig, 2, 2, nil)
			defer closef()

			var pconn net.PacketConn

			switch {
			case isReq:
				pconn = reqpconn
			case serveh:
				pconn = servepconn
			}

			tctx, tctxCancel := context.WithTimeout(ctx, time.Second*2)
			defer tctxCancel()

			err := contextEnsureWait(tctx, func(context.Context) error {
				return doHandler(
					func(ctx context.Context, streams []*quic.Stream) (context.Context, error) {
						if reqh {
							return ctx, pconn.Close()
						}

						p := make([]byte, 1)

						if _, err := streams[0].Read(p); err != nil {
							return ctx, err
						}

						return ctx, nil
					},
					func(ctx context.Context, streams []*quic.Stream) (context.Context, error) {
						if serveh {
							return ctx, pconn.Close()
						}

						p := make([]byte, 1)

						if _, err := streams[0].Read(p); err != nil {
							return ctx, err
						}

						return ctx, nil
					},
				)
			})

			var ierr *quic.IdleTimeoutError
			var opErr *net.OpError

			switch {
			case errors.Is(err, context.DeadlineExceeded):
			case errors.Is(err, quic.ErrTransportClosed):
			case errors.As(err, &ierr):
			case errors.As(err, &opErr):
			default:
				t.Fail("unexpected error", "%T %+v", err, err)
			}
		}

		t.Run("during request sub streams handler", func() { pconnClosedDuringHandler(true, false, true) })
		t.Run("during serve sub streams handler", func() { pconnClosedDuringHandler(true, true, false) })
	})

	t.Run("remote packet conn closed", func() {
		t.Run("before request", func() {})
		t.Run("during request", func() {})
		t.Run("during sub streams handler", func() {})
	})

	t.Run("accept timeout", func() {
		_, _, _, reqconn, _, reqstream, closef := t.openStreams(nil, nil, nil)
		defer closef()

		request, closerequest := newRequestSubStreams(context.Background(), reqconn.AcceptStream, time.Minute)
		defer closerequest(nil)

		tctx, tctxCancel := context.WithTimeout(context.Background(), time.Millisecond*333)
		defer tctxCancel()

		h, err := request(tctx, reqstream, 3)
		t.ErrorIs(err, context.DeadlineExceeded)
		t.Nil(h)
	})
}

func (t *testSubStreams) TestRequestAcceptLoop() {
	t.Run("main context canceled", func() {
		t.Run("after request", func() {
			var n, limit uint64 = 2, 2

			ctx, ctxCancel := t.Ctx()
			defer ctxCancel(nil)

			pool := dutil.NewSingleLockedMap[subStreamsRequestUID, *subStreamPoolItem]()

			_, _, _, _, _, reqstream, request, _, doHandler, closef := t.requestServeHandlers(ctx, nil, nil, n, limit, pool)
			defer closef()

			err := doHandler(
				func(ctx context.Context, streams []*quic.Stream) (context.Context, error) {
					t.IfTruePanic(pool.Len() != 0, "pool.Len(), %d != 0", pool.Len())

					t.IfTruePanic(uint64(len(streams)) != n, "wrong streams number")
					return ctx, nil
				},
				func(ctx context.Context, streams []*quic.Stream) (context.Context, error) {
					t.IfTruePanic(uint64(len(streams)) != n, "wrong streams number")
					return ctx, nil
				},
			)
			t.NoError(err)

			t.True(pool.Len() == 0)

			t.T().Log("main context canceled")
			ctxCancel(nil)

			h, err := request(ctx, reqstream, n)
			t.ErrorIs(err, context.Canceled)
			t.Nil(h)
		})

		t.Run("before request", func() {
			_, _, _, reqconn, _, reqstream, closef := t.openStreams(nil, nil, nil)
			defer closef()

			ctx, ctxCancel := t.Ctx()
			defer ctxCancel(nil)

			pool := dutil.NewSingleLockedMap[subStreamsRequestUID, *subStreamPoolItem]()

			request, closerequest := t.defaultRequest(ctx, reqconn, pool)
			defer closerequest(nil)

			t.T().Log("main context canceled")
			ctxCancel(nil)

			h, err := request(ctx, reqstream, 3)
			t.ErrorIs(err, context.Canceled)
			t.Nil(h)

			t.Equal(0, pool.Len())
		})
	})
}

func (t *testSubStreams) TestRequestPool() {
	t.Run("after request, pool cleaned", func() {
		var n, limit uint64 = 2, 2

		ctx, ctxCancel := t.Ctx()
		defer ctxCancel(nil)

		pool := dutil.NewSingleLockedMap[subStreamsRequestUID, *subStreamPoolItem]()

		_, _, _, _, _, _, _, _, doHandler, closef := t.requestServeHandlers(ctx, nil, nil, n, limit, pool)
		defer closef()

		var reqstreams, servestreams []*quic.Stream
		err := doHandler(
			func(ctx context.Context, streams []*quic.Stream) (context.Context, error) {
				reqstreams = streams

				return ctx, nil
			},
			func(ctx context.Context, streams []*quic.Stream) (context.Context, error) {
				servestreams = streams

				return ctx, nil
			},
		)
		t.NoError(err)

		t.Equal(0, pool.Len())
		for stream := range slices.Values(reqstreams) {
			_, err := stream.Write(util.UUID().Bytes())
			t.ErrorContains(err, "closed stream")
		}

		for stream := range slices.Values(servestreams) {
			_, err := stream.Write(util.UUID().Bytes())
			t.ErrorContains(err, "closed stream")
		}
	})

	checkPoolLen := func(pool *dutil.SingleLockedMap[subStreamsRequestUID, *subStreamPoolItem]) error {
		ticker := time.NewTicker(time.Millisecond * 33)
		defer ticker.Stop()

		after := time.After(time.Second * 3)

		for {
			select {
			case <-after:
				return errors.New("wait pool.Len() == 0")
			case <-ticker.C:
				if pool.Len() == 0 {
					return nil
				}
			}
		}
	}

	t.Run("request failed, pool cleaned", func() {
		t.Run("timeout", func() {
			var n uint64 = 2

			_, _, _, reqconn, servestream, reqstream, closef := t.openStreams(nil, nil, nil)
			defer closef()

			ctx, ctxCancel := t.Ctx()
			defer ctxCancel(nil)

			pool := dutil.NewSingleLockedMap[subStreamsRequestUID, *subStreamPoolItem]()

			request, closerequest := t.defaultRequest(ctx, reqconn, pool)
			defer closerequest(nil)

			go func() {
				// read until uid
				n, _, err := frame.ReadUint64(servestream)
				t.IfErrPanicf(err, "serve: read n")

				_, err = frame.WriteUint64(servestream, n)
				t.IfErrPanicf(err, "serve: write n, %d", n)
				if n < 1 {
					return
				}

				_, err = readSubStreamRequestUID(servestream)
				t.IfErrPanicf(err, "serve: read uid")

				t.Print("serve: no OpenStream")
			}()

			tctx, tctxCancel := context.WithTimeout(ctx, time.Second*2)
			defer tctxCancel()

			_, err := request(tctx, reqstream, n)
			t.ErrorIs(err, context.DeadlineExceeded)

			t.NoError(checkPoolLen(pool))
		})

		t.Run("no end from serve", func() {
			var n uint64 = 1

			_, _, serveconn, reqconn, servestream, reqstream, closef := t.openStreams(nil, nil, nil)
			defer closef()

			ctx, ctxCancel := t.Ctx()
			defer ctxCancel(nil)

			pool := dutil.NewSingleLockedMap[subStreamsRequestUID, *subStreamPoolItem]()

			request, closerequest := t.defaultRequest(ctx, reqconn, pool)
			defer closerequest(nil)

			go func() {
				// read until uid
				n, _, err := frame.ReadUint64(servestream)
				t.IfErrPanicf(err, "serve: read n")

				_, err = frame.WriteUint64(servestream, n)
				t.IfErrPanicf(err, "serve: write n, %d", n)

				uid, err := readSubStreamRequestUID(servestream)
				t.IfErrPanicf(err, "serve: read uid")

				for range n {
					_, err = openSubStream(ctx, serveconn.OpenStreamSync, uid)
					t.IfErrPanicf(err, "serve: open stream")
				}

				t.Print("serve: no end")
			}()

			tctx, tctxCancel := context.WithTimeout(ctx, time.Second*1)
			defer tctxCancel()

			_, err := request(tctx, reqstream, n)
			t.ErrorIs(err, context.DeadlineExceeded)

			t.NoError(checkPoolLen(pool))
		})
	})
}

func (t *testSubStreams) TestServe() {
	t.Run("no n from request", func() {
		_, _, serveconn, _, servestream, _, closef := t.openStreams(nil, nil, nil)
		defer closef()

		serve := newServeSubStreams(serveconn.OpenStreamSync, 3)

		tctx, tctxCancel := context.WithTimeout(context.Background(), time.Second*2)
		defer tctxCancel()

		_, err := serve(tctx, servestream)
		t.ErrorIs(err, context.DeadlineExceeded)
	})

	t.Run("no uid from request", func() {
		_, _, serveconn, _, servestream, reqstream, closef := t.openStreams(nil, nil, nil)
		defer closef()

		serve := newServeSubStreams(serveconn.OpenStreamSync, 3)

		go func() {
			_, err := frame.WriteUint64(reqstream, 3)
			t.IfErrPanicf(err, "request: write n")

			n, _, err := frame.ReadUint64(reqstream)
			t.IfErrPanicf(err, "request: read n")
			t.IfTruePanic(n != 3, "request: n, %d != 3", n)
		}()

		tctx, tctxCancel := context.WithTimeout(context.Background(), time.Second*2)
		defer tctxCancel()

		_, err := serve(tctx, servestream)
		t.ErrorIs(err, context.DeadlineExceeded)
	})
}

func TestSubStreams(t *testing.T) {
	suite.Run(t, new(testSubStreams))
}
