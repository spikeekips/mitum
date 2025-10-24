package quicstream

import (
	"context"
	"crypto/tls"
	"fmt"
	"net"
	"slices"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/pkg/errors"
	"github.com/quic-go/quic-go"
	"github.com/stretchr/testify/suite"
	"go.uber.org/goleak"
)

type testConnPool struct {
	BaseTest
}

func (t *testConnPool) TestGetConnection() {
	remotePconn, remoteListener := t.RandomListener(nil, nil)
	defer func() {
		remoteListener.Close()
		remotePconn.Close()
	}()

	quicConfig := DefaultQuicConfig()
	tlsConfig := t.NewTLSConfig()

	t.Run("new and get", func() {
		ctx, cancel := t.Ctx()
		defer cancel(nil)

		idleTimeout := time.Millisecond * 333

		pool := t.newConnPoolLockedMap()
		dial, poolClose, err := newConnPoolWithPool(context.Background(), pool, nil, idleTimeout, time.Millisecond*33)
		t.NoError(err)
		defer poolClose(nil)

		conn, err := dial(ctx, remoteListener.Addr(), tlsConfig, quicConfig)
		t.NoError(err)

		t.NotNil(conn)
		t.NoError(conn.Context().Err())

		prev := fmt.Sprintf("%p", conn)

		t.Run("get", func() {
			conn, err := dial(ctx, remoteListener.Addr(), tlsConfig, quicConfig)
			t.NoError(err)
			t.Equal(prev, fmt.Sprintf("%p", conn))
		})

		ticker := time.NewTicker(time.Millisecond * 33)
		defer ticker.Stop()

		wait := time.After(time.Second * 3)
	end0:
		for {
			select {
			case <-wait:
				t.Fail("wait connection closed")

				return
			case <-ticker.C:
				if conn.Context().Err() != nil {
					break end0
				}
			}
		}

		t.PrintErr(conn.Context().Err(), "conn.Context().Err()")
		t.ErrorIs(conn.Context().Err(), context.Canceled)
		t.PrintErr(context.Cause(conn.Context()), "context.Cause(conn.Context().Err())")

		wait = time.After(time.Second * 3)
	end1:
		for {
			select {
			case <-wait:
				t.Fail("wait pool to be cleaned")

				return
			case <-ticker.C:
				if pool.Len() < 1 {
					break end1
				}
			}
		}

		t.Equal(0, pool.Len())

		t.Run("get after close", func() {
			conn, err := dial(ctx, remoteListener.Addr(), tlsConfig, quicConfig)
			t.NoError(err)
			t.NotEqual(prev, fmt.Sprintf("%p", conn))

			t.Equal(1, pool.Len())
		})
	})

	t.Run("global context canceled before dial", func() {
		ctx, cancel := t.Ctx()

		dial, poolClose, err := NewConnPool(ctx, nil, 1<<9, time.Minute, time.Millisecond)
		t.NoError(err)
		defer poolClose(nil)

		cancel(nil)

		conn, err := dial(ctx, remoteListener.Addr(), tlsConfig, quicConfig)
		t.ErrorIs(err, context.Canceled)
		t.Nil(conn)
	})

	t.Run("global context canceled after dial", func() {
		ctx, cancel := t.Ctx()

		dial, poolClose, err := NewConnPool(ctx, nil, 1<<9, time.Minute, time.Millisecond)
		t.NoError(err)
		defer poolClose(nil)

		conn, err := dial(ctx, remoteListener.Addr(), tlsConfig, quicConfig)
		t.NoError(err)
		t.NotNil(conn)
		t.NoError(conn.Context().Err())

		cancel(nil)

		stream, err := conn.OpenStream()
		if err != nil {
			var aerr quic.ApplicationError
			switch {
			case errors.Is(err, context.Canceled),
				errors.As(err, &aerr):
			default:
				t.Failf("unknown error", "%T", err)
			}
		} else {
			t.NoError(err)
			t.NotNil(stream)
			defer stream.Close()
		}
	})

	t.Run("dial failed", func() {
		ctx, cancel := t.Ctx()
		defer cancel(nil)

		pool := t.newConnPoolLockedMap()
		dial, poolClose, err := newConnPoolWithPool(ctx, pool, nil, time.Minute, time.Millisecond)
		t.NoError(err)
		defer poolClose(nil)

		npconn, err := NewPacketConn(&net.UDPAddr{IP: net.IPv4zero, Port: 0})
		t.NoError(err)
		_ = npconn.Close()

		quicConfig := DefaultQuicConfig()
		quicConfig.HandshakeIdleTimeout = time.Millisecond * 33

		_, err = dial(ctx, npconn.LocalAddr(), tlsConfig, quicConfig)
		t.PrintErr(err, "dial")

		var herr *quic.HandshakeTimeoutError
		var ierr *quic.IdleTimeoutError
		switch {
		case errors.As(err, &herr):
		case errors.As(err, &ierr):
		default:
			t.Fail("unknown error: %T %v", err, err)
		}

		t.Equal(0, pool.Len())
	})
}

func (t *testConnPool) TestClean() {
	remotePconn, remoteListener := t.RandomListener(nil, nil)
	defer func() {
		remoteListener.Close()
		remotePconn.Close()
	}()

	tlsConfig := t.NewTLSConfig()
	quicConfig := DefaultQuicConfig()

	t.Run("connection closed", func() {
		ctx, cancel := t.Ctx()
		defer cancel(nil)

		interval := time.Millisecond * 33

		pool := t.newConnPoolLockedMap()
		dial, poolClose, err := newConnPoolWithPool(ctx, pool, nil, time.Minute, interval)
		t.NoError(err)
		defer poolClose(nil)

		conn, err := dial(ctx, remoteListener.Addr(), tlsConfig, quicConfig)
		t.NoError(err)

		t.NotNil(conn)
		t.NoError(conn.Context().Err())

		t.NoError(conn.CloseWithError(3, ""))

		wait := time.After(interval * 3)
	end:
		for {
			select {
			case <-wait:
				t.Fail("wait pool is empty")

				return
			default:
				if pool.Len() == 0 {
					break end
				}
			}
		}
	})

	t.Run("new after connection removed", func() {
		ctx, cancel := t.Ctx()
		defer cancel(nil)

		interval := time.Millisecond * 33

		pool := t.newConnPoolLockedMap()

		dial, poolClose, err := newConnPoolWithPool(ctx, pool, nil, time.Minute, interval)
		t.NoError(err)
		defer poolClose(nil)

		conn, err := dial(ctx, remoteListener.Addr(), t.NewTLSConfig(), quicConfig)
		t.NoError(err)

		t.NotNil(conn)
		t.NoError(conn.Context().Err())

		t.NoError(conn.CloseWithError(3, ""))

		wait := time.After(interval * 3)
	end:
		for {
			select {
			case <-wait:
				t.Fail("wait pool is empty")

				return
			default:
				if pool.Len() == 0 {
					break end
				}
			}
		}

		_, err = dial(ctx, remoteListener.Addr(), t.NewTLSConfig(), quicConfig)
		t.NoError(err)
	})
}

func (t *testConnPool) TestAsync() {
	remotes := make([]*quic.EarlyListener, 13)

	for i := range len(remotes) {
		rpconn, r := t.RandomListener(nil, nil)
		defer rpconn.Close()
		defer r.Close()

		remotes[i] = r
	}

	tlsConfig := t.NewTLSConfig()
	quicConfig := DefaultQuicConfig()

	pool := t.newConnPoolLockedMap()

	var dialed int64

	dial, poolClose, err := newConnPoolWithPool(context.Background(), pool, func(ctx context.Context, addr net.Addr, tlsConfig *tls.Config, quicConfig *quic.Config) (QuicConn, error) {
		atomic.AddInt64(&dialed, 1)

		return Dial(ctx, addr, tlsConfig, quicConfig)
	}, time.Minute, time.Millisecond)

	t.NoError(err)
	defer poolClose(nil)

	t.Run("dial", func() {
		var wg sync.WaitGroup

		for remote := range slices.Values(remotes) {
			wg.Go(func() {
				_, err := dial(context.Background(), remote.Addr(), tlsConfig, quicConfig)
				t.IfErrPanicf(err, "dial")
			})
		}

		wg.Wait()
	})

	t.EqualValues(len(remotes), atomic.LoadInt64(&dialed))

	t.Run("redial", func() {
		var wg sync.WaitGroup

		for remote := range slices.Values(remotes) {
			wg.Go(func() {
				_, err := dial(context.Background(), remote.Addr(), tlsConfig, quicConfig)
				t.IfErrPanicf(err, "dial")
			})
		}

		wg.Wait()
	})

	t.EqualValues(len(remotes), atomic.LoadInt64(&dialed))
}

func TestConnPool(t *testing.T) {
	defer goleak.VerifyNone(t)

	suite.Run(t, new(testConnPool))
}

type testRequest struct {
	BaseTest
}

func (t *testRequest) TestRequest() {
	remotePconn, remoteListener := t.RandomListener(nil, nil)
	defer func() {
		remoteListener.Close()
		remotePconn.Close()
	}()

	tlsConfig := t.NewTLSConfig()
	quicConfig := DefaultQuicConfig()

	t.Run("ok", func() {
		ctx, cancel := t.Ctx()
		defer cancel(nil)

		dial, poolClose, err := NewConnPool(context.Background(), nil, 1<<9, time.Minute, time.Millisecond)
		t.NoError(err)
		defer poolClose(nil)

		f := NewRequestFunc(dial)

		var called bool

		nctx, err := f(ctx, remoteListener.Addr(), tlsConfig, quicConfig, func(ctx context.Context, stream *quic.Stream) (context.Context, error) {
			called = true

			return ctx, nil
		})
		t.NoError(err)
		t.NotNil(nctx)
		t.True(called)
	})

	t.Run("handler net.OpError", func() {
		ctx, cancel := t.Ctx()
		defer cancel(nil)

		pool := t.newConnPoolLockedMap()
		dial, poolClose, err := newConnPoolWithPool(context.Background(), pool, nil, time.Minute, time.Millisecond)
		t.NoError(err)
		defer poolClose(nil)

		f := NewRequestFunc(dial)

		var called bool

		nctx, err := f(ctx, remoteListener.Addr(), tlsConfig, quicConfig, func(ctx context.Context, stream *quic.Stream) (context.Context, error) {
			called = true

			return ctx, &net.OpError{Err: errors.New("showme")}
		})

		var oerr *net.OpError
		t.ErrorAs(err, &oerr)
		t.ErrorContains(oerr, "showme")
		t.NotNil(nctx)
		t.True(called)
		t.Equal(1, pool.Len())
	})

	t.Run("handler inside error", func() {
		ctx, cancel := t.Ctx()
		defer cancel(nil)

		pool := t.newConnPoolLockedMap()
		dial, poolClose, err := newConnPoolWithPool(context.Background(), pool, nil, time.Minute, time.Millisecond)
		t.NoError(err)
		defer poolClose(nil)

		f := NewRequestFunc(dial)

		var called bool

		nctx, err := f(ctx, remoteListener.Addr(), tlsConfig, quicConfig, func(ctx context.Context, stream *quic.Stream) (context.Context, error) {
			called = true

			return ctx, errors.New("showme")
		})

		t.ErrorContains(err, "showme")
		t.NotNil(nctx)
		t.True(called)
		t.Equal(1, pool.Len())
	})
}

func (t *testRequest) TestRetry() {
	tlsConfig := t.NewTLSConfig()
	quicConfig := DefaultQuicConfig()

	t.Run("ok", func() {
		ctx, cancel := t.Ctx()
		defer cancel(nil)

		remotePconn, remoteListener := t.RandomListener(nil, nil)
		defer func() {
			remoteListener.Close()
			remotePconn.Close()
		}()

		f := NewRequestFunc(Dial)

		nctx, err := f(ctx, remoteListener.Addr(), tlsConfig, quicConfig, func(ctx context.Context, stream *quic.Stream) (context.Context, error) {
			return context.WithValue(ctx, "1", "1"), nil
		})
		t.NoError(err)
		t.Equal("1", nctx.Value("1"))

		r, err := NewRetryRequestFunc(1, 1)
		t.NoError(err)
		nctx, err = r(f)(ctx, remoteListener.Addr(), tlsConfig, quicConfig, func(ctx context.Context, stream *quic.Stream) (context.Context, error) {
			return context.WithValue(ctx, "2", "2"), nil
		})
		t.NoError(err)
		t.Equal("2", nctx.Value("2"))
	})

	t.Run("remote closed", func() {
		ctx, cancel := t.Ctx()
		defer cancel(nil)

		quicConfig.HandshakeIdleTimeout = time.Millisecond * 33

		remotePconn := t.RandomPacketConn()
		remotePconn.Close()

		r, err := NewRetryRequestFunc(1, 1)
		t.NoError(err)

		_, err = r(NewRequestFunc(Dial))(ctx, remotePconn.LocalAddr(), tlsConfig, quicConfig, func(ctx context.Context, stream *quic.Stream) (context.Context, error) {
			return ctx, nil
		})

		var herr *quic.HandshakeTimeoutError
		var ierr *quic.IdleTimeoutError
		switch {
		case errors.As(err, &herr):
		case errors.As(err, &ierr):
		default:
			t.Fail("unknown error: %T %v", err, err)
		}
	})

	t.Run("context canceled", func() {
		ctx, cancel := t.Ctx()

		quicConfig.HandshakeIdleTimeout = time.Millisecond * 33

		remotePconn := t.RandomPacketConn()
		remotePconn.Close()

		r, err := NewRetryRequestFunc(1, 1)
		t.NoError(err)

		cancel(nil)

		_, err = r(NewRequestFunc(Dial))(ctx, remotePconn.LocalAddr(), tlsConfig, quicConfig, func(ctx context.Context, stream *quic.Stream) (context.Context, error) {
			return ctx, nil
		})
		t.ErrorIs(err, context.Canceled)
	})
}

func TestRequest(t *testing.T) {
	defer goleak.VerifyNone(t)

	suite.Run(t, new(testRequest))
}

func dialWithPacketConn(
	ctx context.Context,
	pconn net.PacketConn,
	addr net.Addr, tlsConfig *tls.Config, quicConfig *quic.Config,
) (QuicConn, error) {
	c, err := quic.DialEarly(ctx, pconn, addr, tlsConfig, quicConfig)
	return c, errors.WithStack(err)
}
