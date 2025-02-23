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

	"github.com/quic-go/quic-go"
	dutil "github.com/spikeekips/mitum/example/test364_5/util"
	"github.com/stretchr/testify/suite"
	"go.uber.org/goleak"
)

type testDialPool struct {
	BaseTest
}

func (t *testDialPool) newPool() dutil.LockedMap[string, quic.Connection] {
	p, err := dutil.NewShardedLockedMap[string, quic.Connection](nil, 1<<9, 1<<9)

	t.PrintIfErrPanic(err, "new pool")

	return p
}

func (t *testDialPool) newRemoteListener() *quic.EarlyListener {
	remotePconn, remoteListener := t.RandomListener(nil, nil)

	t.T().Cleanup(func() {
		remoteListener.Close()
		remotePconn.Close()
	})

	return remoteListener
}

func (t *testDialPool) TestGetConnection() {
	remoteListener := t.newRemoteListener()

	_, tlsConfig := t.NewTLSConfig()
	quicConfig := DefaultQuicConfig()

	t.Run("new and get", func() {
		ctx, cancel := t.Ctx()
		defer cancel(nil)

		pool := t.newPool()
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

		pool := t.newPool()
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

		pool := t.newPool()
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
	remoteListener := t.newRemoteListener()

	_, tlsConfig := t.NewTLSConfig()
	quicConfig := DefaultQuicConfig()

	t.Run("connection closed", func() {
		ctx, cancel := t.Ctx()
		defer cancel(nil)

		interval := time.Millisecond * 33

		pool := t.newPool()
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

		pool := t.newPool()

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

	pool := t.newPool()

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
