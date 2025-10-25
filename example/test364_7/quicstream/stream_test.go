package quicstream

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/pkg/errors"
	"github.com/quic-go/quic-go"
	"github.com/spikeekips/mitum/example/test364_5/frame"
	dutil "github.com/spikeekips/mitum/example/test364_5/util"
	"github.com/spikeekips/mitum/util"
	"github.com/stretchr/testify/suite"
)

type testSubStreams struct {
	BaseTest
}

func (t *testSubStreams) OpenConnections(
	waitCtx, reqCtx context.Context,
	waitQuicConfig, reqQuicConfig *quic.Config,
) (net.PacketConn, net.PacketConn, QuicConn, QuicConn, func()) {
	if waitCtx == nil {
		waitCtx = context.Background()
	}
	if reqCtx == nil {
		reqCtx = context.Background()
	}

	if waitQuicConfig == nil {
		waitQuicConfig = DefaultQuicConfig()
	}
	if reqQuicConfig == nil {
		reqQuicConfig = DefaultQuicConfig()
	}

	waitpconn, l := t.RandomListener(nil, waitQuicConfig)
	reqpconn := t.RandomPacketConn()

	waitConnCh := make(chan QuicConn, 1)
	go func() {
		conn, err := l.Accept(waitCtx)
		t.PrintIfErrPanic(err, "accept")

		waitConnCh <- conn
	}()

	reqConn, err := dialWithPacketConn(reqCtx, reqpconn, waitpconn.LocalAddr(), t.NewTLSConfig(), reqQuicConfig)
	t.PrintIfErrPanic(err, "req dial")

	waitConn := <-waitConnCh

	return waitpconn, reqpconn, waitConn, reqConn, sync.OnceFunc(func() {
		err := waitpconn.Close()
		t.PrintIfErrCaller(err, 2, "wait: PacketConn closed")
		err = reqpconn.Close()
		t.PrintIfErrCaller(err, 2, "req: PacketConn closed")
	})
}

func (t *testSubStreams) OpenStreams(
	ctx context.Context,
	waitQuicConfig, reqQuicConfig *quic.Config,
) (
	net.PacketConn, net.PacketConn, QuicConn, QuicConn,
	*quic.Stream, *quic.Stream,
	func(),
) {
	if ctx == nil {
		ctx = context.Background()
	}
	waitpconn, reqpconn, waitConn, reqConn, closef := t.OpenConnections(ctx, ctx, waitQuicConfig, reqQuicConfig)

	waitWaitStream := dutil.AssertWaitChan(t.Assert(), time.Second, func() *quic.Stream {
		stream, err := waitConn.AcceptStream(ctx)
		t.PrintIfErrPanic(err, "wait conn: AcceptStream")

		_, _, err = frame.ReadFixed(stream)
		t.IfErrPanicf(err, "wait stream: read")

		return stream
	})

	reqStream, err := reqConn.OpenStream()
	t.PrintIfErrPanic(err, "req conn: OpenStream")
	t.IfErrPanicf(frame.WriteFixed(reqStream, []byte{0}), "req stream: write")

	return waitpconn, reqpconn, waitConn, reqConn, waitWaitStream(), reqStream, closef
}

func (t *testSubStreams) TestOpen() {
	run := func(n, limit uint64, re, wa SubStreamsHandlerFunc) {
		t.T().Helper()

		_, _, waitconn, reqconn, waitstream, reqstream, closef := t.OpenStreams(nil, nil, nil)
		defer closef()

		pool := dutil.NewSingleLockedMap[string, *subStreamPoolItem]()
		defer pool.Close()

		request, poolClose, err := newFuncRequestSubStreamsWithPool(
			context.Background(),
			reqconn,
			pool,
		)
		t.NoError(err)
		defer poolClose(nil)

		waitDone := make(chan error, 1)
		wait := newFuncWaitSubStreams(waitconn, limit)

		go func() {
			waitDone <- wait(context.Background(), waitstream, func(s []*quic.Stream, err error) error {
				t.Print("wait err: %T %v", err, err)

				return wa(s, err)
			})
		}()

		t.NoError(request(context.Background(), reqstream, n, func(s []*quic.Stream, err error) error {
			t.Print("request err: %T %v", err, err)

			return re(s, err)
		}))

		select {
		case <-time.After(time.Second * 3):
			t.Fail("wait")
		case err := <-waitDone:
			t.NoError(err)
		}

		t.Equal(0, pool.Len())
	}

	t.Run("n=0 limit=1", func() {
		var waitCalled, reqCalled int64
		run(0, 1,
			func([]*quic.Stream, error) error {
				atomic.AddInt64(&reqCalled, 1)

				return nil
			},
			func([]*quic.Stream, error) error {
				atomic.AddInt64(&waitCalled, 1)

				return nil
			},
		)

		t.EqualValues(1, reqCalled)
		t.EqualValues(1, waitCalled)
	})

	t.Run("n=9 limti=9", func() {
		var n uint64 = 9

		var bs [][]byte
		for range n {
			bs = append(bs, util.UUID().Bytes())
		}

		done := make(chan [][]byte, 1)

		run(n, n,
			func(s []*quic.Stream, err error) error {
				for i, stream := range slices.All(s) {
					t.IfErrPanic(err)

					if _, err := stream.Write(bs[i]); err != nil {
						t.IfErrPanic(err)
					}

					t.IfErrPanic(stream.Close())
				}

				return nil
			},
			func(s []*quic.Stream, _ error) error {
				var bb [][]byte
				for stream := range slices.Values(s) {
					b, err := io.ReadAll(stream)
					if err != nil {
						t.IfErrPanic(err)
					}
					bb = append(bb, b)

					_ = stream.Close()
				}

				done <- bb

				return nil
			},
		)

		select {
		case <-time.After(time.Second * 3):
			t.Fail("wait")
		case bb := <-done:
			t.EqualValues(n, len(bb))

			for b0 := range slices.Values(bs) {
				var found bool
				for b1 := range slices.Values(bb) {
					if bytes.Equal(b0, b1) {
						found = true

						break
					}
				}

				t.True(found)
			}
		}
	})

	t.Run("n=9 limti=3", func() {
		var waitCalled, reqCalled int64
		run(9, 3,
			func(s []*quic.Stream, err error) error {
				t.IfErrPanic(err)

				atomic.AddInt64(&reqCalled, int64(len(s)))

				return nil
			},
			func(s []*quic.Stream, err error) error {
				atomic.AddInt64(&waitCalled, int64(len(s)))

				return nil
			},
		)

		t.EqualValues(3, reqCalled)
		t.EqualValues(3, waitCalled)
	})
}

type lazyQuicConn struct {
	QuicConn
	acceptStream func(context.Context) error
	openStream   func(context.Context) error
}

func (c lazyQuicConn) AcceptStream(ctx context.Context) (*quic.Stream, error) {
	if c.acceptStream != nil {
		if err := c.acceptStream(ctx); err != nil {
			return nil, err
		}
	}

	return c.QuicConn.AcceptStream(ctx)
}

func (c lazyQuicConn) OpenStreamSync(ctx context.Context) (*quic.Stream, error) {
	if c.openStream != nil {
		if err := c.openStream(ctx); err != nil {
			return nil, err
		}
	}

	return c.QuicConn.OpenStreamSync(ctx)
}

func newLazyQuicConn(conn QuicConn, accept, open dutil.InErrorFunc[context.Context]) lazyQuicConn {
	return lazyQuicConn{
		QuicConn:     conn,
		acceptStream: accept,
		openStream:   open,
	}
}

func funcDelayConn(delay int64, next dutil.InErrorFunc[context.Context]) dutil.InErrorFunc[context.Context] {
	return func(ctx context.Context) error {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(time.Second * time.Duration(delay)):
		}

		if next != nil {
			return next(ctx)
		}

		return nil
	}
}

func funcCloseConn(conn QuicConn, i int64, next dutil.InErrorFunc[context.Context]) dutil.InErrorFunc[context.Context] {
	var opened int64

	return func(ctx context.Context) error {
		if atomic.AddInt64(&opened, 1) == i {
			err := conn.CloseWithError(3, "showme")
			fmt.Println("lazy quic conn closed", err)
			if err != nil {
				return err
			}
		}

		if next != nil {
			return next(ctx)
		}

		return nil
	}
}

func (t *testSubStreams) TestConnClosed() {
	t.Run("wait side conn closed", func() {
		_, _, waitconn, reqconn, waitstream, reqstream, closef := t.OpenStreams(nil, nil, nil)
		defer closef()

		pool := dutil.NewSingleLockedMap[string, *subStreamPoolItem]()
		defer pool.Close()

		request, poolClose, err := newFuncRequestSubStreamsWithPool(
			context.Background(),
			reqconn,
			pool,
		)
		t.NoError(err)
		defer poolClose(nil)

		waitDone := make(chan error, 1)

		lazywaitconn := newLazyQuicConn(waitconn, nil,
			funcCloseConn(waitconn, 3, funcDelayConn(1, nil)),
		)

		go func() {
			wait := newFuncWaitSubStreams(lazywaitconn, 9)

			waitDone <- wait(context.Background(), waitstream, func(_ []*quic.Stream, err error) error {
				t.Print("wait err: %T %v", err, err)

				var aerr *quic.ApplicationError
				t.IfTruePanic(!errors.As(err, &aerr), "not *quic.ApplicationError")
				t.IfTruePanic(aerr.Remote, "remote")
				t.IfTruePanic(aerr.ErrorCode != 3, "error code")
				t.IfTruePanic(aerr.ErrorMessage != "showme", "error message")

				return nil
			})
		}()

		err = request(context.Background(), reqstream, 9, func(s []*quic.Stream, err error) error {
			t.Print("request err: %T %v", err, err)

			var aerr *quic.ApplicationError
			t.IfTruePanic(!errors.As(err, &aerr), "not *quic.ApplicationError")
			t.IfTruePanic(!aerr.Remote, "remote")
			t.IfTruePanic(aerr.ErrorCode != 3, "error code")
			t.IfTruePanic(aerr.ErrorMessage != "showme", "error message")

			t.IfTruePanic(len(s) > 0, "not empty streams")

			return err
		})
		var aerr *quic.ApplicationError
		t.ErrorAs(err, &aerr)
		t.True(aerr.Remote)
		t.EqualValues(aerr.ErrorCode, 3)
		t.Equal(aerr.ErrorMessage, "showme")

		select {
		case <-time.After(time.Second * 3):
			t.Fail("wait")
		case err := <-waitDone:
			var aerr *quic.ApplicationError
			t.ErrorAs(err, &aerr)
			t.False(aerr.Remote)
			t.EqualValues(aerr.ErrorCode, 3)
			t.Equal(aerr.ErrorMessage, "showme")
		}
	})

	t.Run("wait side conn closed before wait", func() {
		_, _, waitconn, reqconn, waitstream, reqstream, closef := t.OpenStreams(nil, nil, nil)
		defer closef()

		pool := dutil.NewSingleLockedMap[string, *subStreamPoolItem]()
		defer pool.Close()

		request, poolClose, err := newFuncRequestSubStreamsWithPool(
			context.Background(),
			reqconn,
			pool,
		)
		t.NoError(err)
		defer poolClose(nil)

		waitDone := make(chan error, 1)

		t.NoError(waitconn.CloseWithError(3, "showme"))
		go func() {
			wait := newFuncWaitSubStreams(waitconn, 3)

			waitDone <- wait(context.Background(), waitstream, func(_ []*quic.Stream, err error) error {
				t.Print("wait err: %T %v", err, err)

				var aerr *quic.ApplicationError
				t.IfTruePanic(!errors.As(err, &aerr), "not *quic.ApplicationError")
				t.IfTruePanic(aerr.Remote, "remote")
				t.IfTruePanic(aerr.ErrorCode != 3, "error code")
				t.IfTruePanic(aerr.ErrorMessage != "showme", "error message")

				return nil
			})
		}()

		err = request(context.Background(), reqstream, 3, func(s []*quic.Stream, err error) error {
			t.Print("request err: %T %v", err, err)

			var aerr *quic.ApplicationError
			t.IfTruePanic(!errors.As(err, &aerr), "not *quic.ApplicationError")
			t.IfTruePanic(!aerr.Remote, "remote")
			t.IfTruePanic(aerr.ErrorCode != 3, "error code")
			t.IfTruePanic(aerr.ErrorMessage != "showme", "error message")

			t.IfTruePanic(len(s) > 0, "not empty streams")

			return err
		})

		var aerr *quic.ApplicationError
		switch {
		case errors.Is(err, context.Canceled):
		case errors.As(err, &aerr):
			t.True(aerr.Remote)
			t.EqualValues(aerr.ErrorCode, 3)
			t.Equal(aerr.ErrorMessage, "showme")
		default:
			t.Fail("unexpected error", "%T %v", err, err)
		}

		select {
		case <-time.After(time.Second * 3):
			t.Fail("wait")
		case err := <-waitDone:
			var aerr *quic.ApplicationError
			t.ErrorAs(err, &aerr)
			t.False(aerr.Remote)
			t.EqualValues(aerr.ErrorCode, 3)
			t.Equal(aerr.ErrorMessage, "showme")
		}
	})

	t.Run("request side conn closed", func() {
		_, _, waitconn, reqconn, waitstream, reqstream, closef := t.OpenStreams(nil, nil, nil)
		defer closef()

		pool := dutil.NewSingleLockedMap[string, *subStreamPoolItem]()
		defer pool.Close()

		var opened int64
		lazyreqconn := lazyQuicConn{
			QuicConn: reqconn,
			acceptStream: func(context.Context) error {
				if atomic.CompareAndSwapInt64(&opened, 0, 1) {
					reqconn.CloseWithError(3, "showme")
					t.Print("request conn closed")
				}

				return nil
			},
		}

		request, poolClose, err := newFuncRequestSubStreamsWithPool(
			context.Background(),
			lazyreqconn,
			pool,
		)
		t.NoError(err)
		defer poolClose(nil)

		waitDone := make(chan error, 1)

		go func() {
			wait := newFuncWaitSubStreams(waitconn, 9)

			waitDone <- wait(context.Background(), waitstream, func(s []*quic.Stream, err error) error {
				t.Print("wait stream: %d err=%T %v", len(s), err, err)

				var aerr *quic.ApplicationError
				t.IfTruePanic(!errors.As(err, &aerr), "not *quic.ApplicationError")
				t.IfTruePanic(!aerr.Remote, "remote")
				t.IfTruePanic(aerr.ErrorCode != 3, "error code")
				t.IfTruePanic(aerr.ErrorMessage != "showme", "error message")

				return nil
			})
		}()

		err = request(context.Background(), reqstream, 9, func(s []*quic.Stream, err error) error {
			t.Print("request err: %T %v", err, err)

			var aerr *quic.ApplicationError
			t.IfTruePanic(!errors.As(err, &aerr), "not *quic.ApplicationError")
			t.IfTruePanic(aerr.Remote, "remote")
			t.IfTruePanic(aerr.ErrorCode != 3, "error code")
			t.IfTruePanic(aerr.ErrorMessage != "showme", "error message")

			t.IfTruePanic(len(s) > 0, "not empty streams")

			return err
		})
		var aerr *quic.ApplicationError
		t.ErrorAs(err, &aerr)
		t.False(aerr.Remote)
		t.EqualValues(aerr.ErrorCode, 3)
		t.Equal(aerr.ErrorMessage, "showme")

		select {
		case <-time.After(time.Second * 3):
			t.Fail("wait")
		case err := <-waitDone:
			t.T().Logf("wait error: %T %v", err, err)

			var aerr *quic.ApplicationError
			t.ErrorAs(err, &aerr)
			t.True(aerr.Remote)
			t.EqualValues(aerr.ErrorCode, 3)
			t.Equal(aerr.ErrorMessage, "showme")
		}
	})

	t.Run("request side conn closed before wait", func() {
		_, _, waitconn, reqconn, waitstream, reqstream, closef := t.OpenStreams(nil, nil, nil)
		defer closef()

		pool := dutil.NewSingleLockedMap[string, *subStreamPoolItem]()
		defer pool.Close()

		t.NoError(reqconn.CloseWithError(3, "showme"))

		request, poolClose, err := newFuncRequestSubStreamsWithPool(
			context.Background(),
			reqconn,
			pool,
		)
		t.NoError(err)
		defer poolClose(nil)

		waitDone := make(chan error, 1)

		go func() {
			wait := newFuncWaitSubStreams(waitconn, 3)

			waitDone <- wait(context.Background(), waitstream, func(_ []*quic.Stream, err error) error {
				t.Print("wait err: %T %v", err, err)

				var aerr *quic.ApplicationError
				t.IfTruePanic(!errors.As(err, &aerr), "not *quic.ApplicationError")
				t.IfTruePanic(!aerr.Remote, "remote")
				t.IfTruePanic(aerr.ErrorCode != 3, "error code")
				t.IfTruePanic(aerr.ErrorMessage != "showme", "error message")

				return nil
			})
		}()

		err = request(context.Background(), reqstream, 3, func(s []*quic.Stream, err error) error {
			t.Print("request err: %T %v", err, err)

			var aerr *quic.ApplicationError
			t.IfTruePanic(!errors.As(err, &aerr), "not *quic.ApplicationError")
			t.IfTruePanic(aerr.Remote, "remote")
			t.IfTruePanic(aerr.ErrorCode != 3, "error code")
			t.IfTruePanic(aerr.ErrorMessage != "showme", "error message")

			t.IfTruePanic(len(s) > 0, "not empty streams")

			return err
		})
		var aerr *quic.ApplicationError
		t.ErrorAs(err, &aerr)
		t.False(aerr.Remote)
		t.EqualValues(aerr.ErrorCode, 3)
		t.Equal(aerr.ErrorMessage, "showme")

		select {
		case <-time.After(time.Second * 3):
			t.Fail("wait")
		case err := <-waitDone:
			t.T().Logf("wait error: %T %v", err, err)

			var aerr *quic.ApplicationError
			t.ErrorAs(err, &aerr)
			t.True(aerr.Remote)
			t.EqualValues(aerr.ErrorCode, 3)
			t.Equal(aerr.ErrorMessage, "showme")
		}
	})
}

func (t *testSubStreams) TestContextCanceled() {
	t.Run("wait side canceled before wait", func() {
		_, _, waitconn, _, waitstream, _, closef := t.OpenStreams(nil, nil, nil)
		defer closef()

		waitctx, waitctxCancel := dutil.ContextWithCancelCause(context.Background())
		waitctxCancel(nil)
		t.T().Log("wait context canceled")

		wait := newFuncWaitSubStreams(waitconn, 3)

		err := wait(waitctx, waitstream, func(_ []*quic.Stream, err error) error {
			t.Print("wait err: %T %v", err, err)

			return nil
		})
		t.ErrorIs(err, context.Canceled)
	})

	t.Run("wait side", func() {
		_, _, waitconn, reqconn, waitstream, reqstream, closef := t.OpenStreams(nil, nil, nil)
		defer closef()

		pool := dutil.NewSingleLockedMap[string, *subStreamPoolItem]()
		defer pool.Close()

		request, poolClose, err := newFuncRequestSubStreamsWithPool(
			context.Background(),
			reqconn,
			pool,
		)
		t.NoError(err)
		defer poolClose(nil)

		waitDone := make(chan error, 1)

		waitctx, waitctxCancel := dutil.ContextWithCancelCause(context.Background())
		defer waitctxCancel(nil)

		var opened int64
		lazywaitconn := lazyQuicConn{
			QuicConn: waitconn,
			openStream: func(ctx context.Context) error {
				if atomic.CompareAndSwapInt64(&opened, 0, 1) {
					waitctxCancel(errors.New("showme"))
					t.Print("wait canceled")
				}

				select {
				case <-ctx.Done():
					return ctx.Err()
				case <-time.After(time.Second * 1):
					return nil
				}
			},
		}

		go func() {
			wait := newFuncWaitSubStreams(lazywaitconn, 9)

			waitDone <- wait(waitctx, waitstream, func([]*quic.Stream, error) error {
				t.Print("wait err: %T %v", err, err)

				return nil
			})
		}()

		err = request(context.Background(), reqstream, 9, func(s []*quic.Stream, err error) error {
			t.Print("request err: %T %v", err, err)

			t.IfTruePanic(
				err.Error() != "context canceled" && !strings.Contains(err.Error(), "showme"),
				"request error: %T %v", err, err)
			t.IfTruePanic(len(s) > 0, "not empty streams")

			return err
		})
		t.ErrorContains(err, "context canceled")

		select {
		case <-time.After(time.Second * 3):
			t.Fail("wait")
		case err := <-waitDone:
			t.ErrorIs(err, context.Canceled)
		}
	})

	t.Run("request side", func() {
		_, _, waitconn, reqconn, waitstream, reqstream, closef := t.OpenStreams(nil, nil, nil)
		defer closef()

		pool := dutil.NewSingleLockedMap[string, *subStreamPoolItem]()
		defer pool.Close()

		reqctx, reqctxCancel := dutil.ContextWithCancelCause(context.Background())
		defer reqctxCancel(nil)

		var opened int64
		lazyreqconn := lazyQuicConn{
			QuicConn: reqconn,
			acceptStream: func(ctx context.Context) error {
				if atomic.CompareAndSwapInt64(&opened, 0, 1) {
					reqctxCancel(errors.New("showme"))
					t.Print("req canceled")
				}

				select {
				case <-ctx.Done():
					return ctx.Err()
				case <-time.After(time.Second * 1):
					return nil
				}
			},
		}

		request, poolClose, err := newFuncRequestSubStreamsWithPool(
			context.Background(),
			lazyreqconn,
			pool,
		)
		t.NoError(err)
		defer poolClose(nil)

		waitDone := make(chan error, 1)

		go func() {
			wait := newFuncWaitSubStreams(waitconn, 9)

			waitDone <- wait(context.Background(), waitstream, func([]*quic.Stream, error) error {
				t.Print("wait err: %T %v", err, err)

				return nil
			})
		}()

		rerr := request(reqctx, reqstream, 9, func(s []*quic.Stream, err error) error {
			t.Print("request streams: %d err: %T %v", len(s), err, err)

			t.IfTruePanic(!errors.Is(err, context.Canceled), "request error: %T %v", err, err)

			return err
		})
		t.ErrorIs(rerr, context.Canceled)

		select {
		case <-time.After(time.Second * 3):
			t.Fail("wait")
		case err := <-waitDone:
			t.NoError(err)
		}
	})

	t.Run("request side before request", func() {
		_, _, _, reqconn, _, reqstream, closef := t.OpenStreams(nil, nil, nil)
		defer closef()

		pool := dutil.NewSingleLockedMap[string, *subStreamPoolItem]()
		defer pool.Close()

		reqctx, reqctxCancel := dutil.ContextWithCancelCause(context.Background())
		reqctxCancel(nil)

		request, poolClose, err := newFuncRequestSubStreamsWithPool(
			context.Background(),
			reqconn,
			pool,
		)
		t.NoError(err)
		defer poolClose(nil)

		reqctxCancel(errors.New("showme"))
		t.T().Log("req canceled")

		err = request(reqctx, reqstream, 3, func(s []*quic.Stream, err error) error {
			t.Print("request err: %T %v", err, err)

			return nil
		})
		t.ErrorIs(err, context.Canceled)
	})
}

func (t *testSubStreams) TestPacketConnClosed() {
	t.Run("wait side closed before wait", func() {
		waitpconn, _, waitconn, _, waitstream, _, closef := t.OpenStreams(nil, nil, nil)
		defer closef()

		t.T().Log("wait context canceled")

		wait := newFuncWaitSubStreams(waitconn, 3)

		t.NoError(waitpconn.Close())

		err := wait(context.Background(), waitstream, func(_ []*quic.Stream, err error) error {
			t.Print("wait err: %T %v", err, err)

			return nil
		})
		t.Print("wait err: %T %v", err, err)
		var nerr *net.OpError
		switch {
		case errors.As(err, &nerr):
		case errors.Is(err, quic.ErrTransportClosed):
		default:
			t.Fail("unexpected error", "%T %v", err, err)
		}
	})

	t.Run("wait side", func() {
		reqQuicConfig := DefaultQuicConfig()
		reqQuicConfig.MaxIdleTimeout = time.Millisecond * 333

		waitpconn, _, waitconn, reqconn, waitstream, reqstream, closef := t.OpenStreams(nil, nil, reqQuicConfig)
		defer closef()

		pool := dutil.NewSingleLockedMap[string, *subStreamPoolItem]()
		defer pool.Close()

		request, poolClose, err := newFuncRequestSubStreamsWithPool(
			context.Background(),
			reqconn,
			pool,
		)
		t.NoError(err)
		defer poolClose(nil)

		waitDone := make(chan error, 1)

		var opened int64
		lazywaitconn := lazyQuicConn{
			QuicConn: waitconn,
			openStream: func(ctx context.Context) error {
				if atomic.AddInt64(&opened, 1) == 3 {
					t.IfErrPanic(waitpconn.Close())
					t.Print("wait packetconn closed")
				}

				select {
				case <-ctx.Done():
					return ctx.Err()
				case <-time.After(time.Second * 1):
					return nil
				}
			},
		}

		go func() {
			wait := newFuncWaitSubStreams(lazywaitconn, 9)

			waitDone <- wait(context.Background(), waitstream, func(_ []*quic.Stream, err error) error {
				t.Print("wait err: %T %v", err, err)

				return nil
			})
		}()

		err = request(context.Background(), reqstream, 9, func(s []*quic.Stream, err error) error {
			t.Print("request err: %T %v", err, err)

			return err
		})
		t.T().Logf("request err: %T %v", err, err)

		var ierr *quic.IdleTimeoutError
		t.ErrorAs(err, &ierr)

		select {
		case <-time.After(time.Second * 3):
			t.Fail("wait")
		case err := <-waitDone:
			t.T().Logf("wait err: %T %v", err, err)

			var nerr *net.OpError
			t.ErrorAs(err, &nerr)
		}
	})

	t.Run("request side before request", func() {
		_, reqpconn, _, reqconn, _, reqstream, closef := t.OpenStreams(nil, nil, nil)
		defer closef()

		pool := dutil.NewSingleLockedMap[string, *subStreamPoolItem]()
		defer pool.Close()

		request, poolClose, err := newFuncRequestSubStreamsWithPool(
			context.Background(),
			reqconn,
			pool,
		)
		t.NoError(err)
		defer poolClose(nil)

		t.NoError(reqpconn.Close())

		err = request(context.Background(), reqstream, 3, func(s []*quic.Stream, err error) error {
			t.Print("request err: %T %v", err, err)

			var aerr *quic.ApplicationError
			var nerr *net.OpError

			switch {
			case errors.Is(err, quic.ErrTransportClosed):
			case errors.As(err, &nerr):
			case errors.As(err, &aerr):
				t.IfTruePanic(aerr.Remote, "remote")
				t.IfTruePanic(aerr.ErrorCode != 3, "error code")
				t.IfTruePanic(aerr.ErrorMessage != "showme", "error message")
			default:
				t.IfErrPanic(err)
			}

			t.IfTruePanic(len(s) > 0, "not empty streams")

			return err
		})

		t.Print("request err: %T %v", err, err)
		var nerr *net.OpError
		switch {
		case errors.As(err, &nerr):
		case errors.Is(err, quic.ErrTransportClosed):
		default:
			t.Fail("unexpected error", "%T %v", err, err)
		}
	})

	t.Run("request side", func() {
		_, reqpconn, waitconn, reqconn, waitstream, reqstream, closef := t.OpenStreams(nil, nil, nil)
		defer closef()

		pool := dutil.NewSingleLockedMap[string, *subStreamPoolItem]()
		defer pool.Close()

		var opened int64
		lazyreqconn := lazyQuicConn{
			QuicConn: reqconn,
			acceptStream: func(ctx context.Context) error {
				if atomic.CompareAndSwapInt64(&opened, 0, 1) {
					t.IfErrPanic(reqpconn.Close())
					t.Print("request packetconn closed")
				}

				select {
				case <-ctx.Done():
					return ctx.Err()
				case <-time.After(time.Second * 1):
					return nil
				}
			},
		}

		request, poolClose, err := newFuncRequestSubStreamsWithPool(
			context.Background(),
			lazyreqconn,
			pool,
		)
		t.NoError(err)
		defer poolClose(nil)

		waitDone := make(chan error, 1)

		go func() {
			wctx, wctxCancel := context.WithTimeout(context.Background(), time.Second)
			defer wctxCancel()

			wait := newFuncWaitSubStreams(waitconn, 9)

			waitDone <- wait(wctx, waitstream, func(_ []*quic.Stream, err error) error {
				t.Print("wait err: %T %v", err, err)

				return nil
			})
		}()

		err = request(context.Background(), reqstream, 9, func(s []*quic.Stream, err error) error {
			t.Print("request err: %T %v", err, err)

			return err
		})

		t.Print("request err: %T %v", err, err)
		var nerr *net.OpError
		switch {
		case errors.As(err, &nerr):
		case errors.Is(err, quic.ErrTransportClosed):
		default:
			t.Fail("unexpected error", "%T %v", err, err)
		}

		select {
		case <-time.After(time.Second * 9):
			t.Fail("wait")
		case err := <-waitDone:
			t.T().Logf("wait err: %T %v", err, err)

			var ierr *quic.IdleTimeoutError
			var nerr *net.OpError
			switch {
			case errors.As(err, &ierr):
			case errors.As(err, &nerr):
			case errors.Is(err, context.DeadlineExceeded):
			default:
				t.Failf("unexpected error", "%T %v", err, err)
			}
		}
	})
}

// FIXME timeout
// FIXME sub streams opened and using, but conn closed

func TestSubStreams(t *testing.T) {
	suite.Run(t, new(testSubStreams))
}
