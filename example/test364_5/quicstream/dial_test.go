package quicstream

import (
	"context"
	"crypto/tls"
	"fmt"
	"net"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/pkg/errors"
	"github.com/quic-go/quic-go"
	dutil "github.com/spikeekips/mitum/example/test364_5/util"
	"github.com/stretchr/testify/suite"
	"go.uber.org/goleak"
)

type testDialPool struct {
	BaseTest
}

func (t *testDialPool) TestGetConnection() {
	remotePconn, remoteListener := t.RandomListener(nil, nil)
	defer func() {
		remoteListener.Close()
		remotePconn.Close()
	}()

	_, tlsConfig := t.NewTLSConfig()
	quicConfig := DefaultQuicConfig()

	t.Run("new and get", func() {
		ctx, cancel := t.Ctx()
		defer cancel(nil)

		pool := t.NewDialPool()
		dial, poolClose, err := NewDialPool(pool, nil, time.Minute)
		t.NoError(err)
		defer poolClose()

		conn, closef, err := dial(ctx, remoteListener.Addr(), tlsConfig, quicConfig)
		t.NoError(err)

		t.NotNil(conn)
		t.NoError(conn.Context().Err())

		prev := fmt.Sprintf("%p", conn)

		t.Run("get", func() {
			conn, _, err := dial(ctx, remoteListener.Addr(), tlsConfig, quicConfig)
			t.NoError(err)
			t.Equal(prev, fmt.Sprintf("%p", conn))
		})

		t.NoError(closef(nil))

		t.PrintErr(conn.Context().Err(), "conn.Context().Err()")
		t.ErrorIs(conn.Context().Err(), context.Canceled)
		t.PrintErr(context.Cause(conn.Context()), "context.Cause(conn.Context().Err())")

		t.Equal(0, pool.Len())

		t.Run("get after close", func() {
			conn, closef, err := dial(ctx, remoteListener.Addr(), tlsConfig, quicConfig)
			t.NoError(err)
			t.NotEqual(prev, fmt.Sprintf("%p", conn))

			t.NoError(closef(nil))

			t.Equal(0, pool.Len())
		})
	})

	t.Run("global context canceled", func() {
		ctx, cancel := t.Ctx()

		pool := t.NewDialPool()
		dial, poolClose, err := NewDialPool(pool, nil, time.Minute)
		t.NoError(err)
		defer poolClose()

		conn, closef, err := dial(ctx, remoteListener.Addr(), tlsConfig, quicConfig)
		t.NoError(err)
		defer closef(nil)

		t.NotNil(conn)
		t.NoError(conn.Context().Err())

		cancel(nil)

		t.NoError(conn.Context().Err())

		stream, err := conn.OpenStream()
		t.NoError(err)
		t.NotNil(stream)
		defer stream.Close()
	})

	t.Run("dial failed", func() {
		ctx, cancel := t.Ctx()
		defer cancel(nil)

		pool := t.NewDialPool()
		dial, poolClose, err := NewDialPool(pool, nil, time.Minute)
		t.NoError(err)
		defer poolClose()

		npconn := t.NewPacketConn()
		_ = npconn.Close()

		quicConfig := DefaultQuicConfig()
		quicConfig.HandshakeIdleTimeout = time.Millisecond * 33

		_, _, err = dial(ctx, npconn.LocalAddr(), tlsConfig, quicConfig)
		t.PrintErr(err, "dial")

		var ierr *quic.IdleTimeoutError
		t.ErrorAs(err, &ierr)

		t.Equal(0, pool.Len())
	})
}

func (t *testDialPool) TestClean() {
	remotePconn, remoteListener := t.RandomListener(nil, nil)
	defer func() {
		remoteListener.Close()
		remotePconn.Close()
	}()

	_, tlsConfig := t.NewTLSConfig()
	quicConfig := DefaultQuicConfig()

	t.Run("connection closed", func() {
		ctx, cancel := t.Ctx()
		defer cancel(nil)

		interval := time.Millisecond * 33

		pool := t.NewDialPool()
		dial, poolClose, err := NewDialPool(pool, nil, interval)
		t.NoError(err)
		defer poolClose()

		conn, closef, err := dial(ctx, remoteListener.Addr(), tlsConfig, quicConfig)
		t.NoError(err)
		defer closef(nil)

		t.NotNil(conn)
		t.NoError(conn.Context().Err())

		t.NoError(conn.CloseWithError(3, ""))

		<-time.After(interval * 3)
		t.Equal(0, pool.Len())
	})

	t.Run("new after connection removed", func() {
		ctx, cancel := t.Ctx()
		defer cancel(nil)

		interval := time.Millisecond * 33

		pool := t.NewDialPool()

		dial, poolClose, err := NewDialPool(pool, nil, interval)
		t.NoError(err)
		defer poolClose()

		conn, closef, err := dial(ctx, remoteListener.Addr(), tlsConfig, quicConfig)
		t.NoError(err)
		defer closef(nil)

		t.NotNil(conn)
		t.NoError(conn.Context().Err())

		t.NoError(conn.CloseWithError(3, ""))

		<-time.After(interval * 3)
		t.Equal(0, pool.Len())

		_, _, err = dial(ctx, remoteListener.Addr(), tlsConfig, quicConfig)
		t.NoError(err)
	})
}

func (t *testDialPool) TestAsync() {
	pconn := t.NewPacketConn()
	defer pconn.Close()

	remotes := make([]*quic.EarlyListener, 13)

	for i := range len(remotes) {
		rpconn, r := t.RandomListener(nil, nil)
		defer rpconn.Close()
		defer r.Close()

		remotes[i] = r
	}

	_, tlsConfig := t.NewTLSConfig()
	quicConfig := DefaultQuicConfig()

	pool := t.NewDialPool()

	counts := dutil.NewSingleLockedMap[string, int]()

	dial, poolClose, err := NewDialPool(pool, func(ctx context.Context, addr net.Addr, tlsConfig *tls.Config, quicConfig *quic.Config) (quic.Connection, error) {
		counts.Set(addr.String(), func(i int, found bool) (int, error) {
			i++

			return i, nil
		})

		pconn, err := NewPacketConn(0)
		if err != nil {
			return nil, err
		}

		return DialWithPacketConn(ctx, pconn, addr, tlsConfig, quicConfig)
	}, time.Minute)

	t.NoError(err)
	defer poolClose()

	t.Run("dial", func() {
		var wg sync.WaitGroup

		for remote := range slices.Values(remotes) {
			wg.Add(1)

			go func() {
				defer wg.Done()

				_, _, err := dial(context.Background(), remote.Addr(), tlsConfig, quicConfig)
				t.IfErrPanicf(err, "dial")
			}()
		}

		wg.Wait()
	})

	t.Run("redial", func() {
		var wg sync.WaitGroup

		for remote := range slices.Values(remotes) {
			wg.Add(1)

			go func() {
				defer wg.Done()

				_, closef, err := dial(context.Background(), remote.Addr(), tlsConfig, quicConfig)
				t.IfErrPanicf(err, "dial")
				closef(nil)
			}()
		}

		wg.Wait()
	})

	for _, count := range counts.Seq2 {
		t.Equal(1, count)
	}
}

func TestDialPool(t *testing.T) {
	defer goleak.VerifyNone(t)

	suite.Run(t, new(testDialPool))
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

	_, tlsConfig := t.NewTLSConfig()
	quicConfig := DefaultQuicConfig()

	t.Run("ok", func() {
		ctx, cancel := t.Ctx()
		defer cancel(nil)

		pool := t.NewDialPool()
		dial, poolClose, err := NewDialPool(pool, nil, time.Minute)
		t.NoError(err)
		defer poolClose()

		f, err := NewFuncRequest(dial, tlsConfig, quicConfig)
		t.NoError(err)

		var called bool

		nctx, err := f(ctx, remoteListener.Addr(), nil, nil, func(ctx context.Context, stream quic.Stream) (context.Context, error) {
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

		pool := t.NewDialPool()
		dial, poolClose, err := NewDialPool(pool, nil, time.Minute)
		t.NoError(err)
		defer poolClose()

		f, err := NewFuncRequest(dial, tlsConfig, quicConfig)
		t.NoError(err)

		var called bool

		nctx, err := f(ctx, remoteListener.Addr(), nil, nil, func(ctx context.Context, stream quic.Stream) (context.Context, error) {
			called = true

			return ctx, &net.OpError{Err: errors.Errorf("showme")}
		})

		var oerr *net.OpError
		t.ErrorAs(err, &oerr)
		t.ErrorContains(oerr, "showme")
		t.NotNil(nctx)
		t.True(called)
		t.Equal(0, pool.Len())
	})

	t.Run("handler inside error", func() {
		ctx, cancel := t.Ctx()
		defer cancel(nil)

		pool := t.NewDialPool()
		dial, poolClose, err := NewDialPool(pool, nil, time.Minute)
		t.NoError(err)
		defer poolClose()

		f, err := NewFuncRequest(NewDialWithCloseFunc(dial, NewFuncConnClosingStreamError), tlsConfig, quicConfig)
		t.NoError(err)

		var called bool

		nctx, err := f(ctx, remoteListener.Addr(), nil, nil, func(ctx context.Context, stream quic.Stream) (context.Context, error) {
			called = true

			return ctx, errors.Errorf("showme")
		})

		t.ErrorContains(err, "showme")
		t.NotNil(nctx)
		t.True(called)
		t.Equal(1, pool.Len())
	})
}

func TestRequest(t *testing.T) {
	defer goleak.VerifyNone(t)

	suite.Run(t, new(testRequest))
}
