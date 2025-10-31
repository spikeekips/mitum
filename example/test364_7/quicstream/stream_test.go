//go:build failed

package quicstream

import (
	"context"
	"fmt"
	"io"
	"net"
	"os"
	"slices"
	"strings"
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

type testSubStreams struct {
	BaseTest
}

func (t *testSubStreams) Run(a string, f func()) {
	t.BaseTest.RunWithGoLeak(a, f)
}

func (t *testSubStreams) openConnections(
	reqCtx, serveCtx context.Context,
	reqQuicConfig, serveQuicConfig *quic.Config,
) (net.PacketConn, net.PacketConn, QuicConn, QuicConn, func()) {
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

	servech := make(chan QuicConn, 1)
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
	reqpconn, servepconn, reqconn, serveconn, closef := t.openConnections(ctx, ctx, reqQuicConfig, serveQuicConfig)

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

	var servestream *quic.Stream

	select {
	case <-sctx.Done():
		t.PrintIfErrPanic(err, "wait serve stream")
	case servestream = <-servestreamch:
	}

	return reqpconn, servepconn, reqconn, serveconn, reqstream, servestream, sync.OnceFunc(func() {
		closeStream(reqstream, nil)
		closeStream(servestream, nil)
		closef()
	})
}

func (t *testSubStreams) defaultRequest(
	ctx context.Context,
	conn QuicConn,
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

	f, c := newRequestSubStreamsWithPool(ctx, conn, time.Second*2, pool)

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
	reqpconn, servepconn, reqconn, serveconn, reqstream, servestream, closef := t.openStreams(ctx, reqQuicConfig, serveQuicConfig)

	request, closerequest := t.defaultRequest(ctx, reqconn, pool)
	serve := newServeSubStreams(serveconn.OpenStreamSync, limit)

	doHandler := func(reqhandler, servehandler SubStreamsHandlerFunc) error {
		t.T().Helper()

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
	t.Run("accept timeout", func() {
		_, _, reqconn, _, reqstream, _, closef := t.openStreams(nil, nil, nil)
		defer closef()

		request, closerequest := newRequestSubStreams(context.Background(), reqconn, time.Minute)
		defer closerequest(nil)

		tctx, tctxCancel := context.WithTimeout(context.Background(), time.Millisecond*333)
		defer tctxCancel()

		h, err := request(tctx, reqstream, 3)
		t.ErrorIs(err, context.DeadlineExceeded)
		t.Nil(h)
	})
}

func (t *testSubStreams) TestPacketConnClosed() {
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
		default:
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

	t.Run("local packet conn closed", func() {
		t.Run("before request", func() {
			reqpconn, _, reqconn, _, reqstream, _, closef := t.openStreams(nil, nil, nil)
			defer closef()

			ctx, ctxCancel := t.Ctx()
			defer ctxCancel(nil)

			request, closerequest := newRequestSubStreams(context.Background(), reqconn, time.Minute)
			defer closerequest(nil)

			t.T().Log("local packet conn closed")
			t.NoError(reqpconn.Close())

			_, err := request(ctx, reqstream, 3)
			checkPacketConnClosedErr(err)
		})

		t.Run("before request; n == 0", func() {
			reqpconn, _, reqconn, _, reqstream, _, closef := t.openStreams(nil, nil, nil)
			defer closef()

			ctx, ctxCancel := t.Ctx()
			defer ctxCancel(nil)

			request, closerequest := newRequestSubStreams(context.Background(), reqconn, time.Minute)
			defer closerequest(nil)

			t.T().Log("local packet conn closed")
			t.NoError(reqpconn.Close())

			_, err := request(ctx, reqstream, 0)
			checkPacketConnClosedErr(err)
		})

		t.Run("during request", func() {
			ctx, ctxCancel := t.Ctx()
			defer ctxCancel(nil)

			reqpconn, _, reqconn, _, reqstream, servestream, closef := t.openStreams(ctx, nil, nil)
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

			reqpconn, _, reqconn, _, reqstream, servestream, closef := t.openStreams(ctx, nil, nil)
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

		t.Run("during request sub streams handler", func() { pconnClosedDuringHandler(true, true, false) })
		t.Run("during serve sub streams handler", func() { pconnClosedDuringHandler(true, false, true) })
	})

	t.Run("remote packet conn closed", func() {
		t.Run("before request", func() {
			reqQuicConfig := DefaultQuicConfig()
			reqQuicConfig.MaxIdleTimeout = time.Millisecond * 333

			_, servepconn, reqconn, _, reqstream, _, closef := t.openStreams(nil, reqQuicConfig, nil)
			defer closef()

			ctx, ctxCancel := t.Ctx()
			defer ctxCancel(nil)

			request, closerequest := newRequestSubStreams(context.Background(), reqconn, time.Minute)
			defer closerequest(nil)

			t.T().Log("remote packet conn closed")
			t.NoError(servepconn.Close())

			_, err := request(ctx, reqstream, 3)
			var ierr *quic.IdleTimeoutError
			t.ErrorAs(err, &ierr)
		})

		t.Run("before request; n == 0", func() {
			reqQuicConfig := DefaultQuicConfig()
			reqQuicConfig.MaxIdleTimeout = time.Millisecond * 333

			_, servepconn, reqconn, _, reqstream, _, closef := t.openStreams(nil, reqQuicConfig, nil)
			defer closef()

			ctx, ctxCancel := t.Ctx()
			defer ctxCancel(nil)

			request, closerequest := newRequestSubStreams(context.Background(), reqconn, time.Minute)
			defer closerequest(nil)

			t.T().Log("local packet conn closed")
			t.NoError(servepconn.Close())

			_, err := request(ctx, reqstream, 0)
			var ierr *quic.IdleTimeoutError
			t.ErrorAs(err, &ierr)
		})

		t.Run("during request", func() {
			reqQuicConfig := DefaultQuicConfig()
			reqQuicConfig.MaxIdleTimeout = time.Millisecond * 333

			ctx, ctxCancel := t.Ctx()
			defer ctxCancel(nil)

			_, servepconn, reqconn, _, reqstream, servestream, closef := t.openStreams(ctx, reqQuicConfig, nil)
			defer closef()

			go func() {
				n, _, err := frame.ReadUint64(servestream)
				t.IfErrPanicf(err, "serve: read n")

				_, err = frame.WriteUint64(servestream, n)
				t.IfErrPanicf(err, "serve: write n")

				t.Print("local packet conn closed before reading uid")
				t.IfErrPanicf(servepconn.Close(), "close local packat conn")
			}()

			request, closerequest := t.defaultRequest(ctx, reqconn, nil)
			defer closerequest(nil)

			tctx, tctxCancel := context.WithTimeout(ctx, time.Second*2)
			defer tctxCancel()

			var ierr *quic.IdleTimeoutError
			switch _, err := request(tctx, reqstream, 3); {
			case errors.Is(err, context.DeadlineExceeded):
			case errors.As(err, &ierr):
			default:
				t.Fail("unexpected error", "%T %+v", err, err)
			}
		})

		t.Run("during accept stream", func() {
			ctx, ctxCancel := t.Ctx()
			defer ctxCancel(nil)

			_, servepconn, reqconn, _, reqstream, servestream, closef := t.openStreams(ctx, nil, nil)
			defer closef()

			go func() {
				n, _, err := frame.ReadUint64(servestream)
				t.IfErrPanicf(err, "serve: read n")

				_, err = frame.WriteUint64(servestream, n)
				t.IfErrPanicf(err, "serve: write n")

				_, err = readSubStreamRequestUID(servestream)
				t.IfErrPanicf(err, "serve: read uid")

				t.Print("local packet conn closed before accept stream")
				t.IfErrPanicf(servepconn.Close(), "close local packat conn")
			}()

			request, closerequest := t.defaultRequest(ctx, reqconn, nil)
			defer closerequest(nil)

			tctx, tctxCancel := context.WithTimeout(ctx, time.Second*2)
			defer tctxCancel()

			_, err := request(tctx, reqstream, 3)
			t.ErrorIs(err, context.DeadlineExceeded)
		})

		t.Run("during request sub streams handler", func() { pconnClosedDuringHandler(false, false, true) })
		t.Run("during serve sub streams handler", func() { pconnClosedDuringHandler(false, true, false) })
	})
}

func (t *testSubStreams) TestControlStreamClosed() {
	controlStreamClosedDuringHandler := func(isReq, reqh, serveh bool) {
		t.T().Helper()

		quicConfig := DefaultQuicConfig()
		quicConfig.MaxIdleTimeout = time.Millisecond * 333

		ctx, ctxCancel := t.Ctx()
		defer ctxCancel(nil)

		_, _, _, _, reqstream, servestream, _, _, doHandler, closef := t.requestServeHandlers(ctx, quicConfig, quicConfig, 2, 2, nil)
		defer closef()

		var cstream *quic.Stream

		switch {
		case isReq:
			cstream = reqstream
		default:
			cstream = servestream
		}

		tctx, tctxCancel := context.WithTimeout(ctx, time.Second*2)
		defer tctxCancel()

		err := contextEnsureWait(tctx, func(context.Context) error {
			return doHandler(
				func(ctx context.Context, streams []*quic.Stream) (context.Context, error) {
					if reqh {
						return ctx, cstream.Close()
					}

					p := make([]byte, 1)

					if _, err := streams[0].Read(p); err != nil {
						if serveh {
							t.IfTruePanic(!errors.Is(err, io.EOF), "req eof, %T %+v", err, err)

							return ctx, nil
						}

						return ctx, err
					}

					return ctx, nil
				},
				func(ctx context.Context, streams []*quic.Stream) (context.Context, error) {
					if serveh {
						return ctx, cstream.Close()
					}

					p := make([]byte, 1)

					if _, err := streams[0].Read(p); err != nil {
						if reqh {
							t.IfTruePanic(!errors.Is(err, io.EOF), "serve eof")

							return ctx, nil
						}

						return ctx, err
					}

					return ctx, nil
				},
			)
		})
		switch {
		case err == nil:
		case errors.Is(err, context.DeadlineExceeded):
		default:
			t.Fail("unexpected error", "%T %+v", err, err)
		}
	}

	t.Run("request", func() {
		t.Run("before request", func() {
			_, _, reqconn, _, reqstream, _, closef := t.openStreams(nil, nil, nil)
			defer closef()

			ctx, ctxCancel := t.Ctx()
			defer ctxCancel(nil)

			request, closerequest := newRequestSubStreams(context.Background(), reqconn, time.Minute)
			defer closerequest(nil)

			t.T().Log("control stream closed")
			t.NoError(reqstream.Close())

			_, err := request(ctx, reqstream, 3)
			t.ErrorContains(err, "write on closed stream")
		})

		t.Run("during request; before writing uid", func() {
			ctx, ctxCancel := t.Ctx()
			defer ctxCancel(nil)

			_, _, reqconn, _, reqstream, servestream, closef := t.openStreams(ctx, nil, nil)
			defer closef()

			go func() {
				_, _, err := frame.ReadUint64(servestream)
				t.IfErrPanicf(err, "serve: read n")

				t.Print("closed before writing uid")
				t.IfErrPanicf(reqstream.Close(), "close request stream")
			}()

			request, closerequest := t.defaultRequest(ctx, reqconn, nil)
			defer closerequest(nil)

			tctx, tctxCancel := context.WithTimeout(ctx, time.Second*2)
			defer tctxCancel()

			_, err := request(tctx, reqstream, 3)
			t.ErrorIs(err, context.DeadlineExceeded)
		})

		t.Run("during request; after writing uid", func() {
			ctx, ctxCancel := t.Ctx()
			defer ctxCancel(nil)

			_, _, reqconn, _, reqstream, servestream, closef := t.openStreams(ctx, nil, nil)
			defer closef()

			go func() {
				n, _, err := frame.ReadUint64(servestream)
				t.IfErrPanicf(err, "serve: read n")

				_, err = frame.WriteUint64(servestream, n)
				t.IfErrPanicf(err, "serve: write n")

				t.Print("closed after writing uid")
				t.IfErrPanicf(reqstream.Close(), "close request stream")
			}()

			request, closerequest := t.defaultRequest(ctx, reqconn, nil)
			defer closerequest(nil)

			tctx, tctxCancel := context.WithTimeout(ctx, time.Second*2)
			defer tctxCancel()

			switch _, err := request(tctx, reqstream, 3); {
			case errors.Is(err, context.DeadlineExceeded):
			case strings.Contains(err.Error(), "write on closed stream"):
			default:
				t.Fail("unexpected error", "%T %+v", err, err)
			}
		})

		t.Run("during accept stream", func() {
			ctx, ctxCancel := t.Ctx()
			defer ctxCancel(nil)

			_, _, reqconn, _, reqstream, servestream, closef := t.openStreams(ctx, nil, nil)
			defer closef()

			go func() {
				n, _, err := frame.ReadUint64(servestream)
				t.IfErrPanicf(err, "serve: read n")

				_, err = frame.WriteUint64(servestream, n)
				t.IfErrPanicf(err, "serve: write n")

				_, err = readSubStreamRequestUID(servestream)
				t.IfErrPanicf(err, "serve: read uid")

				t.Print("before accept stream")
				t.IfErrPanicf(reqstream.Close(), "close request stream")
			}()

			request, closerequest := t.defaultRequest(ctx, reqconn, nil)
			defer closerequest(nil)

			tctx, tctxCancel := context.WithTimeout(ctx, time.Second*2)
			defer tctxCancel()

			_, err := request(tctx, reqstream, 3)
			t.ErrorIs(err, context.DeadlineExceeded)
		})

		t.Run("during request sub streams handler", func() { controlStreamClosedDuringHandler(true, true, false) })
		t.Run("during serve sub streams handler", func() { controlStreamClosedDuringHandler(true, false, true) })
	})
}

func (t *testSubStreams) TestRequestContextCanceled() {
	t.Run("before request", func() {
		_, _, reqconn, _, reqstream, _, closef := t.openStreams(nil, nil, nil)
		defer closef()

		pool := dutil.NewSingleLockedMap[subStreamsRequestUID, *subStreamPoolItem]()

		request, closerequest := t.defaultRequest(context.Background(), reqconn, pool)
		defer closerequest(nil)

		t.T().Log("context canceled")
		ctx, ctxCancel := t.Ctx()

		ctxCancel(nil)

		_, err := request(ctx, reqstream, 3)
		t.ErrorIs(err, context.Canceled)

		t.Equal(0, pool.Len())
	})

	t.Run("during request", func() {
		_, _, reqconn, _, reqstream, servestream, closef := t.openStreams(context.Background(), nil, nil)
		defer closef()

		ctx, ctxCancel := t.Ctx()

		go func() {
			n, _, err := frame.ReadUint64(servestream)
			t.IfErrPanicf(err, "serve: read n")

			_, err = frame.WriteUint64(servestream, n)
			t.IfErrPanicf(err, "serve: write n")

			t.Print("context canceled before reading uid")
			ctxCancel(nil)
		}()

		pool := dutil.NewSingleLockedMap[subStreamsRequestUID, *subStreamPoolItem]()

		request, closerequest := t.defaultRequest(ctx, reqconn, pool)
		defer closerequest(nil)

		_, err := request(ctx, reqstream, 3)
		t.ErrorIs(err, context.Canceled)

		t.checkPoolLen(pool, time.Second*3)
	})

	t.Run("during accept stream", func() {
		_, _, reqconn, _, reqstream, servestream, closef := t.openStreams(context.Background(), nil, nil)
		defer closef()

		ctx, ctxCancel := t.Ctx()

		go func() {
			n, _, err := frame.ReadUint64(servestream)
			t.IfErrPanicf(err, "serve: read n")

			_, err = frame.WriteUint64(servestream, n)
			t.IfErrPanicf(err, "serve: write n")

			_, err = readSubStreamRequestUID(servestream)
			t.IfErrPanicf(err, "serve: read uid")

			t.Print("context canceled before accept stream")
			ctxCancel(nil)
		}()

		pool := dutil.NewSingleLockedMap[subStreamsRequestUID, *subStreamPoolItem]()

		request, closerequest := t.defaultRequest(ctx, reqconn, pool)
		defer closerequest(nil)

		_, err := request(ctx, reqstream, 3)
		t.ErrorIs(err, context.Canceled)

		t.NoError(t.checkPoolLen(pool, time.Second*3))
	})

	t.Run("before handler", func() {
		var n, limit uint64 = 2, 2

		_, _, reqconn, serveconn, reqstream, servestream, closef := t.openStreams(context.Background(), nil, nil)
		defer closef()

		pool := dutil.NewSingleLockedMap[subStreamsRequestUID, *subStreamPoolItem]()

		request, closerequest := t.defaultRequest(context.Background(), reqconn, pool)
		defer closerequest(nil)

		serve := newServeSubStreams(serveconn.OpenStreamSync, limit)

		serveDone := make(chan error, 1)

		var reqhandlerCalled, servehandlerCalled int64

		go func() {
			serveh, err := serve(context.Background(), servestream)
			if err != nil {
				serveDone <- err

				return
			}

			_, err = serveh(context.Background(), func(ctx context.Context, streams []*quic.Stream) (context.Context, error) {
				atomic.AddInt64(&servehandlerCalled, 1)
				t.IfTruePanic(uint64(len(streams)) != n, "wrong serve streams")

				for stream := range slices.Values(streams) {
					_, err := stream.Write(util.UUID().Bytes())
					t.IfErrPanicf(err, "serve stream write")
				}

				return ctx, nil
			})

			serveDone <- err
		}()

		ctx, ctxCancel := t.Ctx()
		reqh, err := request(ctx, reqstream, n)
		t.NoError(err)

		t.T().Log("context canceled")
		ctxCancel(nil)

		_, err = reqh(ctx, func(ctx context.Context, streams []*quic.Stream) (context.Context, error) {
			atomic.AddInt64(&reqhandlerCalled, 1)
			t.IfTruePanic(uint64(len(streams)) != n, "wrong request streams")

			for stream := range slices.Values(streams) {
				_, err := stream.Write(util.UUID().Bytes())
				t.IfErrPanicf(err, "req stream write")
			}

			return ctx, nil
		})
		t.NoError(err)

		select {
		case <-time.After(time.Second * 3):
			t.Fail("wait serve handler")
		case err := <-serveDone:
			t.NoError(err)
		}

		t.EqualValues(1, atomic.LoadInt64(&reqhandlerCalled))
		t.EqualValues(1, atomic.LoadInt64(&servehandlerCalled))
	})

	t.Run("during handler", func() {
		var n, limit uint64 = 2, 2

		_, _, reqconn, serveconn, reqstream, servestream, closef := t.openStreams(context.Background(), nil, nil)
		defer closef()

		pool := dutil.NewSingleLockedMap[subStreamsRequestUID, *subStreamPoolItem]()

		request, closerequest := t.defaultRequest(context.Background(), reqconn, pool)
		defer closerequest(nil)

		serve := newServeSubStreams(serveconn.OpenStreamSync, limit)

		serveDone := make(chan error, 1)
		reqReady := make(chan error, 1)

		var reqhandlerCalled, servehandlerCalled int64

		ctx, ctxCancel := t.Ctx()

		go func() {
			serveh, err := serve(context.Background(), servestream)
			if err != nil {
				serveDone <- err

				return
			}

			t.IfErrPanicf(<-reqReady, "request ready")

			_, err = serveh(context.Background(), func(ctx context.Context, streams []*quic.Stream) (context.Context, error) {
				t.T().Log("context canceled before serve handler")
				ctxCancel(nil)

				atomic.AddInt64(&servehandlerCalled, 1)
				t.IfTruePanic(uint64(len(streams)) != n, "wrong serve streams")

				for stream := range slices.Values(streams) {
					_, err := stream.Write(util.UUID().Bytes())
					t.IfErrPanicf(err, "serve stream write")
				}

				return ctx, nil
			})

			serveDone <- err
		}()

		reqh, err := request(ctx, reqstream, n)
		reqReady <- err

		_, err = reqh(ctx, func(ctx context.Context, streams []*quic.Stream) (context.Context, error) {
			atomic.AddInt64(&reqhandlerCalled, 1)
			t.IfTruePanic(uint64(len(streams)) != n, "wrong request streams")

			for stream := range slices.Values(streams) {
				_, err := stream.Write(util.UUID().Bytes())
				t.IfErrPanicf(err, "req stream write")
			}

			return ctx, nil
		})
		t.NoError(err)

		select {
		case <-time.After(time.Second * 3):
			t.Fail("wait serve handler")
		case err := <-serveDone:
			t.NoError(err)
		}

		t.EqualValues(1, atomic.LoadInt64(&reqhandlerCalled))
		t.EqualValues(1, atomic.LoadInt64(&servehandlerCalled))
	})
}

func (t *testSubStreams) TestRequestMainContextCanceled() {
	t.Run("without request", func() {
		ctx, ctxCancel := t.Ctx()
		defer ctxCancel(nil)

		_, _, reqconn, _, _, _, closef := t.openStreams(ctx, nil, nil)
		defer closef()

		pool := dutil.NewSingleLockedMap[subStreamsRequestUID, *subStreamPoolItem]()

		_, closerequest := t.defaultRequest(ctx, reqconn, pool)
		defer closerequest(nil)

		ctxCancel(nil)
	})

	t.Run("before request", func() {
		ctx, ctxCancel := t.Ctx()
		defer ctxCancel(nil)

		pool := dutil.NewSingleLockedMap[subStreamsRequestUID, *subStreamPoolItem]()

		_, _, reqconn, _, reqstream, _, closef := t.openStreams(ctx, nil, nil)
		defer closef()

		request, requestclosef := newRequestSubStreamsWithPool(ctx, reqconn, time.Second, pool)
		defer requestclosef(nil)

		t.T().Log("main context canceled")
		ctxCancel(nil)

		_, err := request(context.Background(), reqstream, 2)
		t.ErrorIs(err, context.Canceled)

		t.NoError(t.checkPoolLen(pool, time.Second*3))
	})

	t.Run("during request", func() {
		ctx, ctxCancel := t.Ctx()
		defer ctxCancel(nil)

		_, _, reqconn, _, reqstream, servestream, closef := t.openStreams(ctx, nil, nil)
		defer closef()

		go func() {
			n, _, err := frame.ReadUint64(servestream)
			t.IfErrPanicf(err, "serve: read n")

			_, err = frame.WriteUint64(servestream, n)
			t.IfErrPanicf(err, "serve: write n")

			t.Print("context canceled before reading uid")
			ctxCancel(nil)
		}()

		pool := dutil.NewSingleLockedMap[subStreamsRequestUID, *subStreamPoolItem]()

		request, closerequest := t.defaultRequest(ctx, reqconn, pool)
		defer closerequest(nil)

		_, err := request(ctx, reqstream, 3)
		t.ErrorIs(err, context.Canceled)

		t.checkPoolLen(pool, time.Second*3)
	})

	t.Run("during accept stream", func() {
		ctx, ctxCancel := t.Ctx()
		defer ctxCancel(nil)

		_, _, reqconn, _, reqstream, servestream, closef := t.openStreams(ctx, nil, nil)
		defer closef()

		go func() {
			n, _, err := frame.ReadUint64(servestream)
			t.IfErrPanicf(err, "serve: read n")

			_, err = frame.WriteUint64(servestream, n)
			t.IfErrPanicf(err, "serve: write n")

			_, err = readSubStreamRequestUID(servestream)
			t.IfErrPanicf(err, "serve: read uid")

			t.Print("context canceled before accept stream")
			ctxCancel(nil)
		}()

		pool := dutil.NewSingleLockedMap[subStreamsRequestUID, *subStreamPoolItem]()

		request, closerequest := t.defaultRequest(ctx, reqconn, pool)
		defer closerequest(nil)

		_, err := request(ctx, reqstream, 3)
		t.ErrorIs(err, context.Canceled)

		t.NoError(t.checkPoolLen(pool, time.Second*3))
	})

	t.Run("during handlers", func() {
		var n, limit uint64 = 2, 2

		ctx, ctxCancel := t.Ctx()
		defer ctxCancel(nil)

		_, _, reqconn, serveconn, reqstream, servestream, closef := t.openStreams(ctx, nil, nil)
		defer closef()

		pool := dutil.NewSingleLockedMap[subStreamsRequestUID, *subStreamPoolItem]()

		request, closerequest := t.defaultRequest(ctx, reqconn, pool)
		defer closerequest(nil)

		serve := newServeSubStreams(serveconn.OpenStreamSync, limit)

		serveDone := make(chan error, 1)

		var reqhandlerCalled, servehandlerCalled int64

		go func() {
			serveh, err := serve(ctx, servestream)
			if err != nil {
				serveDone <- err

				return
			}

			_, err = serveh(ctx, func(ctx context.Context, streams []*quic.Stream) (context.Context, error) {
				atomic.AddInt64(&servehandlerCalled, 1)
				t.IfTruePanic(uint64(len(streams)) != n, "wrong serve streams")

				for stream := range slices.Values(streams) {
					_, err := stream.Write(util.UUID().Bytes())
					t.IfErrPanicf(err, "serve stream write")
				}

				return ctx, nil
			})

			serveDone <- err
		}()

		reqh, err := request(ctx, reqstream, n)
		t.NoError(err)

		t.T().Log("main context canceled")
		ctxCancel(nil)

		_, err = reqh(ctx, func(ctx context.Context, streams []*quic.Stream) (context.Context, error) {
			atomic.AddInt64(&reqhandlerCalled, 1)
			t.IfTruePanic(uint64(len(streams)) != n, "wrong request streams")

			for stream := range slices.Values(streams) {
				_, err := stream.Write(util.UUID().Bytes())
				t.IfErrPanicf(err, "req stream write")
			}

			return ctx, nil
		})
		t.NoError(err)

		select {
		case <-time.After(time.Second * 3):
			t.Fail("wait serve handler")
		case err := <-serveDone:
			t.NoError(err)
		}

		t.EqualValues(1, atomic.LoadInt64(&reqhandlerCalled))
		t.EqualValues(1, atomic.LoadInt64(&servehandlerCalled))
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
			_, _, reqconn, _, reqstream, _, closef := t.openStreams(nil, nil, nil)
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

func (t *testSubStreams) checkPoolLen(
	pool *dutil.SingleLockedMap[subStreamsRequestUID, *subStreamPoolItem],
	after time.Duration,
) error {
	ticker := time.NewTicker(time.Millisecond * 33)
	defer ticker.Stop()

	tafter := time.After(after)

	for {
		select {
		case <-tafter:
			return errors.New("wait pool.Len() == 0")
		case <-ticker.C:
			if pool.Len() == 0 {
				return nil
			}
		}
	}
}

func (t *testSubStreams) TestRequestPool() {
	t.Run("after request, pool cleaned", func() {
		var n, limit uint64 = 2, 2

		ctx, ctxCancel := t.Ctx()
		defer ctxCancel(nil)

		pool := dutil.NewSingleLockedMap[subStreamsRequestUID, *subStreamPoolItem]()

		_, _, _, _, _, _, _, _, doHandler, closef := t.requestServeHandlers(ctx, nil, nil, n, limit, pool)
		defer closef()

		var reqCalled, serveCalled int64
		var reqstreams, servestreams []*quic.Stream

		err := doHandler(
			func(ctx context.Context, streams []*quic.Stream) (context.Context, error) {
				atomic.AddInt64(&reqCalled, 1)

				reqstreams = streams

				return ctx, nil
			},
			func(ctx context.Context, streams []*quic.Stream) (context.Context, error) {
				atomic.AddInt64(&serveCalled, 1)

				servestreams = streams

				return ctx, nil
			},
		)
		t.NoError(err)

		t.EqualValues(1, atomic.LoadInt64(&reqCalled))
		t.EqualValues(1, atomic.LoadInt64(&serveCalled))

		for stream := range slices.Values(reqstreams) {
			_, err := stream.Write(util.UUID().Bytes())
			t.ErrorContains(err, "closed stream")
		}

		for stream := range slices.Values(servestreams) {
			_, err := stream.Write(util.UUID().Bytes())
			t.ErrorContains(err, "closed stream")
		}

		t.NoError(t.checkPoolLen(pool, time.Second*3))
	})

	t.Run("request failed, pool cleaned", func() {
		t.Run("timeout", func() {
			var n uint64 = 2

			_, _, reqconn, _, reqstream, servestream, closef := t.openStreams(nil, nil, nil)
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

			t.NoError(t.checkPoolLen(pool, time.Second*3))
		})

		t.Run("no end from serve", func() {
			var n uint64 = 1

			_, _, reqconn, serveconn, reqstream, servestream, closef := t.openStreams(nil, nil, nil)
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

			t.NoError(t.checkPoolLen(pool, time.Second*3))
		})
	})
}

func (t *testSubStreams) TestServe() {
	t.Run("no n from request", func() {
		_, _, _, serveconn, _, servestream, closef := t.openStreams(nil, nil, nil)
		defer closef()

		serve := newServeSubStreams(serveconn.OpenStreamSync, 3)

		tctx, tctxCancel := context.WithTimeout(context.Background(), time.Second*2)
		defer tctxCancel()

		_, err := serve(tctx, servestream)
		t.ErrorIs(err, context.DeadlineExceeded)
	})

	t.Run("no uid from request", func() {
		_, _, _, serveconn, reqstream, servestream, closef := t.openStreams(nil, nil, nil)
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

func (t *testSubStreams) TestMultipleRequestSubStreams() {
	t.Run("sub streams, write/read", func() {
		ctx, ctxCancel := t.Ctx()
		defer ctxCancel(nil)

		reqQuicConfig := DefaultQuicConfig()
		serveQuicConfig := DefaultQuicConfig()

		var n uint64 = 33333
		nreqs := 1

		sendb := make([]string, n)
		for i := range n {
			sendb[i] = util.UUID().String()
		}

		slices.Sort(sendb)

		t.T().Logf("n=%d requests=%d", n, nreqs)

		reqQuicConfig.MaxIncomingStreams = int64(n * uint64(nreqs+1))
		reqQuicConfig.MaxIncomingUniStreams = reqQuicConfig.MaxIncomingStreams
		serveQuicConfig.MaxIncomingStreams = reqQuicConfig.MaxIncomingStreams
		serveQuicConfig.MaxIncomingUniStreams = reqQuicConfig.MaxIncomingStreams

		t.True(n*uint64(nreqs) < uint64(reqQuicConfig.MaxIncomingStreams))

		_, _, reqconn, serveconn, closef := t.openConnections(ctx, ctx, reqQuicConfig, serveQuicConfig)
		defer closef()

		t.T().Log("reqconn/serveconn prepared")

		reqcstreams := make([]*quic.Stream, nreqs)
		servecstreams := make([]*quic.Stream, nreqs)

		peg, pectx := errgroup.WithContext(ctx)

		fmt.Fprintf(os.Stderr, "opening control streams in both sides, %d : ", nreqs)
		for i := range nreqs {
			peg.Go(func() error {
				stream, err := contextEnsureWaitT(pectx, func(ctx context.Context) (*quic.Stream, error) {
					stream, err := serveconn.AcceptStream(ctx)
					if err != nil {
						return nil, err
					}

					_, _, err = frame.ReadBytes(stream)

					return stream, err
				})

				servecstreams[i] = stream

				fmt.Fprintf(os.Stderr, "s%d ", i)

				return err
			})

			peg.Go(func() error {
				reqstream, err := reqconn.OpenStream()
				if err != nil {
					return errors.WithMessagef(err, "req: OpenStream")
				}

				if _, err = frame.WriteBytes(reqstream, []byte{0}); err != nil {
					return errors.WithMessagef(err, "req : write stream")
				}

				reqcstreams[i] = reqstream

				fmt.Fprintf(os.Stderr, "r%d ", i)

				return nil
			})
		}

		t.NoError(peg.Wait(), "prepare")

		fmt.Fprintln(os.Stderr)

		defer func() {
			t.IfErrPanicf(closeStreams(reqcstreams, nil), "req control sterams")
			t.IfErrPanicf(closeStreams(servecstreams, nil), "serve control sterams")
		}()

		t.T().Log("req/serve control stream prepared")

		pool := dutil.NewSingleLockedMap[subStreamsRequestUID, *subStreamPoolItem]()

		request, closerequest := newRequestSubStreamsWithPool(ctx, reqconn, time.Minute, pool)
		defer closerequest(nil)

		fmt.Fprintf(os.Stderr, "sub stream handlers in both sides, %d : ", nreqs)

		serveeg, serveectx := errgroup.WithContext(ctx)
		reqeg, reqectx := errgroup.WithContext(ctx)

		for serven, servecstream := range slices.All(servecstreams) {
			reqcstream := reqcstreams[serven]

			serveeg.Go(func() error {
				serve := newServeSubStreams(serveconn.OpenStreamSync, n)
				serveh, err := serve(serveectx, servecstream)
				if err != nil {
					return err
				}

				_, err = serveh(serveectx, func(ctx context.Context, streams []*quic.Stream) (context.Context, error) {
					if i := uint64(len(streams)); i != n {
						return ctx, errors.Errorf("serve streams != n; i != n")
					}

					eg, _ := errgroup.WithContext(ctx)

					for i, stream := range slices.All(streams) {
						eg.Go(func() error {
							_, err := stream.Write([]byte(sendb[i]))

							return err
						})
					}

					fmt.Fprintf(os.Stderr, "s%d ", serven)

					return ctx, eg.Wait()
				})

				return err
			})

			reqeg.Go(func() error {
				reqh, err := request(reqectx, reqcstream, n)
				if err != nil {
					return err
				}

				recvb := make([]string, n)

				_, err = reqh(serveectx, func(ctx context.Context, streams []*quic.Stream) (context.Context, error) {
					if i := uint64(len(streams)); i != n {
						return ctx, errors.Errorf("req streams != n; i != n")
					}

					eg, _ := errgroup.WithContext(ctx)

					for j, stream := range slices.All(streams) {
						eg.Go(func() error {
							b, err := io.ReadAll(stream)

							recvb[j] = string(b)

							return err
						})
					}

					fmt.Fprintf(os.Stderr, "r%d ", serven)

					return ctx, eg.Wait()
				})

				if a, b := len(recvb), len(sendb); a != b {
					return errors.Errorf("len(recvb) != len(sendb); %d != %d", a, b)
				}

				slices.Sort(recvb)
				for i, b := range slices.All(recvb) {
					t.IfTruePanic(b != sendb[i], "%q != %q", b, sendb[i])
				}

				return err
			})
		}

		t.NoError(serveeg.Wait(), "serve")
		t.NoError(reqeg.Wait(), "req")

		fmt.Fprintln(os.Stderr)

		t.Equal(0, pool.Len())
	})
}

func TestSubStreams(t *testing.T) {
	suite.Run(t, new(testSubStreams))
}
