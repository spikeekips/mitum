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
	DialFunc         = func(context.Context, net.Addr, *tls.Config, *quic.Config) (quic.Connection, error)
	DialFromPoolFunc = func(context.Context, net.Addr, *tls.Config, *quic.Config) (quic.Connection, dutil.InErrorFunc[error], error)
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
	DialFromPoolFunc,
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
			_ = pool.GetOrRemove(saddr, func(conn quic.Connection, found bool) (bool, error) {
				if conn == nil {
					return false, nil
				}

				gerr = closeConnection(conn, err)

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

			if _, err := pool.Set(saddr, func(prev quic.Connection, found bool) (quic.Connection, error) {
				if prev != nil && prev.Context().Err() == nil {
					conn = prev

					return nil, dutil.ErrLockedSetIgnore
				}

				switch c, err := dial(ctx, addr, tlsConfig, quicConfig); {
				case err != nil:
					return nil, err
				default:
					conn = c

					return c, nil
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

func cleanDialPool(pool dutil.LockedMap[string, quic.Connection]) {
	l := pool.Len()

	if l < 1 {
		return
	}

	saddrs := make([]string, 0, pool.Len())
	defer dutil.ClearSlice(saddrs)

	for saddr, conn := range pool.Seq2 {
		if conn.Context().Err() != nil {
			saddrs = append(saddrs, saddr)
		}
	}

	_ = pool.RemoveValues(saddrs...)
}
