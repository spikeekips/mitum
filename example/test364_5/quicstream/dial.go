package quicstream

import (
	"context"
	"crypto/tls"
	"net"
	"slices"
	"time"

	"github.com/pkg/errors"
	"github.com/quic-go/quic-go"
	dutil "github.com/spikeekips/mitum/example/test364_5/util"
)

type (
	DialFunc      = func(context.Context, net.Addr, *tls.Config, *quic.Config) (quic.Connection, error)
	DialCloseFunc = func(context.Context, net.Addr, *tls.Config, *quic.Config) (quic.Connection, dutil.InErrorFunc[error], error)
	RequestFunc   = func(context.Context, net.Addr, *tls.Config, *quic.Config, StreamHandler) (context.Context, error)
)

func DefaultDial(
	ctx context.Context,
	addr net.Addr,
	tlsConfig *tls.Config,
	quicConfig *quic.Config,
) (net.PacketConn, quic.Connection, error) {
	switch pconn, err := NewPacketConn(0); {
	case err != nil:
		return nil, nil, err
	default:
		c, err := quic.DialEarly(ctx, pconn, addr, tlsConfig, quicConfig)

		return pconn, c, errors.WithStack(err)
	}
}

func DialWithPacketConn(
	ctx context.Context,
	pconn net.PacketConn,
	addr net.Addr,
	tlsConfig *tls.Config,
	quicConfig *quic.Config,
) (quic.Connection, error) {
	c, err := quic.DialEarly(ctx, pconn, addr, tlsConfig, quicConfig)

	return c, errors.WithStack(err)
}

func NewDialPool(
	pool dutil.LockedMap[string, quic.Connection],
	dial DialFunc,
	cleanInterval time.Duration,
) (
	DialCloseFunc,
	dutil.ErrorFunc,
	error,
) {
	var closef []dutil.ErrorFunc

	if dial == nil {
		var tr *quic.Transport

		switch c, err := NewPacketConn(0); {
		case err != nil:
			return nil, nil, err
		default:
			tr = &quic.Transport{Conn: c}
		}

		dial = func(ctx context.Context, addr net.Addr, tlsConfig *tls.Config, quicConfig *quic.Config) (quic.Connection, error) {
			c, err := tr.DialEarly(ctx, addr, tlsConfig, quicConfig)

			return c, errors.WithStack(err)
		}

		closef = append(closef, tr.Close)
	}

	if cleanInterval < time.Millisecond {
		cleanInterval = time.Minute
	}

	gctx, gctxCancel := context.WithCancel(context.Background())

	closef = append(closef, func() error { gctxCancel(); return nil })

	go func() {
		ticker := time.NewTicker(cleanInterval)
		defer ticker.Stop()

		for {
			select {
			case <-gctx.Done():
				return
			case <-ticker.C:
				cleanDialPool(pool)
			}
		}
	}()

	newCloseConnf := func(saddr string) dutil.InErrorFunc[error] {
		return func(err error) (gerr error) {
			gerr = err

			_ = pool.GetOrRemove(saddr, func(conn quic.Connection, found bool) (bool, error) {
				if conn == nil {
					return false, nil
				}

				if cerr := closeConnection(conn, err); cerr != nil {
					gerr = cerr
				}

				return true, nil
			})

			return gerr
		}
	}

	return func(
			ctx context.Context,
			addr net.Addr,
			tlsConfig *tls.Config, quicConfig *quic.Config,
		) (conn quic.Connection, _ dutil.InErrorFunc[error], _ error) {
			saddr := addr.String()

			if _, _, err := pool.SetOrRemove(saddr, func(prev quic.Connection, found bool) (quic.Connection, bool, error) {
				if prev != nil && prev.Context().Err() == nil {
					conn = prev

					return nil, false, dutil.ErrLockedSetIgnore
				}

				switch c, err := dial(ctx, addr, tlsConfig, quicConfig); {
				case err != nil:
					return nil, true, err
				default:
					conn = c

					return c, false, nil
				}
			}); err != nil {
				return nil, nil, err
			}

			return conn, newCloseConnf(saddr), nil
		},
		func() (gerr error) {
			for f := range slices.Values(closef) {
				if err := f(); err != nil && gerr == nil {
					gerr = err
				}
			}

			return gerr
		},
		nil
}

func NewFuncRequest(dial DialCloseFunc, defaultTLSConfig *tls.Config, defaultQuicConfig *quic.Config) (RequestFunc, error) {
	if defaultTLSConfig == nil {
		return nil, errors.Errorf("nil tls config")
	}

	if defaultQuicConfig == nil {
		return nil, errors.Errorf("nil quic config")
	}

	return func(ctx context.Context, addr net.Addr, tlsConfig *tls.Config, quicConfig *quic.Config, h StreamHandler) (context.Context, error) {
			if tlsConfig == nil {
				tlsConfig = defaultTLSConfig
			}

			if quicConfig == nil {
				quicConfig = defaultQuicConfig.Clone()
			}

			var conn quic.Connection
			var closef dutil.InErrorFunc[error]

			switch o, c, err := dial(ctx, addr, tlsConfig, quicConfig); {
			case err != nil:
				return nil, err
			default:
				conn = o
				closef = c
			}

			switch s, err := conn.OpenStreamSync(ctx); {
			case err != nil:
				return nil, closef(err)
			default:
				nctx := context.WithValue(ctx, QuicConnectionContextKey, conn)
				nctx = context.WithValue(nctx, RemoteAddrContextKey, conn.RemoteAddr())
				nctx, err := h(nctx, s)

				return nctx, closef(closeStreamWithError(s, err))
			}
		},
		nil
}

func cleanDialPool(pool dutil.LockedMap[string, quic.Connection]) {
	l := pool.Len()

	if l < 1 {
		return
	}

	saddrs := make([]string, 0, pool.Len())
	defer dutil.ClearSlice(&saddrs)

	for saddr, conn := range pool.Seq2 {
		if conn.Context().Err() != nil {
			saddrs = append(saddrs, saddr)
		}
	}

	_ = pool.RemoveValues(saddrs...)
}

func NewFuncConnClosingStreamError(closef dutil.InErrorFunc[error]) dutil.InErrorFunc[error] {
	return func(err error) error {
		var operr *net.OpError

		switch {
		case errors.Is(err, quic.ErrTransportClosed),
			errors.As(err, &operr):
			return closef(err)
		default:
			return err
		}
	}
}

func NewDialWithCloseFunc(
	dial DialCloseFunc,
	closef dutil.InFunc[dutil.InErrorFunc[error], dutil.InErrorFunc[error]],
) DialCloseFunc {
	if closef == nil {
		return dial
	}

	return func(ctx context.Context, addr net.Addr, tlsConfig *tls.Config, quicConfig *quic.Config) (
		quic.Connection,
		dutil.InErrorFunc[error],
		error,
	) {
		switch c, f, err := dial(ctx, addr, tlsConfig, quicConfig); {
		case err != nil:
			return nil, nil, err
		default:
			return c, closef(f), nil
		}
	}
}
