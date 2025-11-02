package quicstream

import (
	"context"
	"crypto/tls"
	"iter"
	"net"
	"slices"
	"sync"
	"sync/atomic"
	"time"

	"github.com/pkg/errors"
	"github.com/quic-go/quic-go"
	dutil "github.com/spikeekips/mitum/example/test364_5/util"
)

type (
	DialFunc    = func(context.Context, net.Addr, *tls.Config, *quic.Config) (Conn, error)
	RequestFunc = func(context.Context, net.Addr, *tls.Config, *quic.Config, StreamHandler) (context.Context, error)
)

func Dial(
	ctx context.Context,
	addr net.Addr, tlsConfig *tls.Config, quicConfig *quic.Config,
) (Conn, error) {
	c, err := quic.DialAddrEarly(ctx, addr.String(), tlsConfig, quicConfig)
	return c, errors.WithStack(err)
}

func NewConnPool(
	ctx context.Context,
	dialf DialFunc,
	poolSize uint64,
	idleTimeout, cleanInterval time.Duration,
) (
	DialFunc,
	dutil.InErrorFunc[error],
	error,
) {
	var pool dutil.LockedMap[string, Conn]

	switch p, err := dutil.NewLockedMap[string, Conn](nil, poolSize); {
	case err != nil:
		return nil, nil, err
	default:
		pool = p
	}

	return newConnPoolWithPool(ctx, pool, dialf, idleTimeout, cleanInterval)
}

func newConnPoolWithPool(
	ctx context.Context,
	pool dutil.LockedMap[string, Conn],
	dialf DialFunc,
	idleTimeout, cleanInterval time.Duration,
) (
	DialFunc,
	dutil.InErrorFunc[error],
	error,
) {
	if dialf == nil {
		dialf = Dial
	}

	cctx, cctxCancel := context.WithCancel(ctx)

	go func() {
		cleanConnPool(cctx, pool, cleanInterval)

		cctxCancel()
	}()

	checkConfig := checkPoolQuicConfig(idleTimeout)

	return func(
			ctx context.Context,
			addr net.Addr, tlsConfig *tls.Config, quicConfig *quic.Config,
		) (Conn, error) {
			if !quicConfig.Allow0RTT {
				return nil, errors.New("Allow0RTT is disabled")
			}

			if !quicConfig.EnableDatagrams {
				return nil, errors.New("EnableDatagrams is disabled")
			}

			if err := cctx.Err(); err != nil {
				return nil, err
			}

			var conn Conn

			if _, _, err := pool.SetOrRemove(addr.String(), func(prev Conn, _ bool) (Conn, bool, error) {
				if prev != nil && prev.Context().Err() == nil {
					if err := prev.SendDatagram([]byte{0x00}); err == nil {
						conn = prev

						return nil, false, dutil.ErrLockedSetIgnore
					}
				}

				switch c, err := dialf(ctx, addr, tlsConfig, checkConfig(quicConfig)); {
				case err != nil:
					return nil, true, err
				default:
					conn = newNoCloseConn(c)

					return conn, false, nil
				}
			}); err != nil {
				return nil, err
			}

			return conn, nil
		},
		func(err error) error {
			defer pool.Empty()
			defer cctxCancel()

			return closePoolConns(pool.Seq2, err)
		},
		nil
}

func checkPoolQuicConfig(idleTimeout time.Duration) func(*quic.Config) *quic.Config {
	var l sync.Mutex

	return func(c *quic.Config) *quic.Config {
		l.Lock()
		defer l.Unlock()

		if c.KeepAlivePeriod > 0 {
			c.KeepAlivePeriod = 0
		}

		if c.MaxIdleTimeout != idleTimeout {
			c.MaxIdleTimeout = idleTimeout
		}

		return c
	}
}

func closePoolConns(seq iter.Seq2[string, Conn], err error) (gerr error) {
	var wg sync.WaitGroup
	var e int64

	f := newFuncCloseConnection(err)

	for _, conn := range seq {
		wg.Go(func() {
			if err := f(conn.(*noCloseConn).Conn); err != nil {
				if atomic.CompareAndSwapInt64(&e, 0, 1) {
					gerr = err
				}
			}
		})
	}

	wg.Wait()

	return gerr
}

func cleanConnPool(
	ctx context.Context,
	pool dutil.LockedMap[string, Conn],
	interval time.Duration,
) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	var saddrs, removes []string

	for {
		select {
		case <-ctx.Done():
			_ = closePoolConns(pool.Seq2, ctx.Err())

			return
		case <-ticker.C:
			if pool.Len() < 1 {
				continue
			}

			saddrs = make([]string, 0, pool.Len())
			for saddr := range pool.Seq2 {
				saddrs = append(saddrs, saddr)
			}

			if len(saddrs) < 1 {
				continue
			}

			removes = make([]string, 0, len(saddrs))
			for saddr := range slices.Values(saddrs) {
				conn, _ := pool.Value(saddr)
				if conn != nil && conn.Context().Err() != nil {
					removes = append(removes, saddr)

					_ = closeConnection(conn.(*noCloseConn).Conn, nil)
				}
			}

			if len(removes) > 0 {
				_ = pool.RemoveValues(removes...)

				clear(removes)
			}

			clear(saddrs)
		}
	}
}

func NewRequestFunc(dialf DialFunc) RequestFunc {
	return func(ctx context.Context, addr net.Addr, tlsConfig *tls.Config, quicConfig *quic.Config, h StreamHandler) (
		context.Context, error,
	) {
		var conn Conn

		switch o, err := dialf(ctx, addr, tlsConfig, quicConfig); {
		case err != nil:
			return nil, err
		default:
			defer func() {
				o.CloseWithError(connErrorCodeOK, "")
			}()

			conn = o
		}

		switch s, err := conn.OpenStreamSync(ctx); {
		case err != nil:
			return nil, closeConnection(conn, err)
		default:
			bctx := context.WithValue(ctx, LocalAddrContextKey, conn.LocalAddr())
			bctx = context.WithValue(bctx, RemoteAddrContextKey, conn.RemoteAddr())
			bctx, err := h(bctx, s)

			_ = closeStream(s, err)

			return bctx, errors.WithStack(err)
		}
	}
}

func NewRetryRequestFunc(maxRetry uint64, interval time.Duration) (func(RequestFunc) RequestFunc, error) {
	if maxRetry < 1 {
		return nil, errors.New("zero maxRetry")
	}

	if interval < 1 {
		return nil, errors.New("zero interval")
	}

	return func(f RequestFunc) RequestFunc {
			return func(ctx context.Context, addr net.Addr, tlsConfig *tls.Config, quicConfig *quic.Config, h StreamHandler) (context.Context, error) {
				var gerr error

				for range maxRetry {
					switch nctx, err := f(ctx, addr, tlsConfig, quicConfig, h); {
					case err != nil:
						gerr = err

						<-time.After(interval)
					default:
						return nctx, nil
					}
				}

				return ctx, gerr
			}
		},
		nil
}
