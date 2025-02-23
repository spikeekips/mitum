package quicstream

import (
	"context"
	"crypto/tls"
	"net"
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
		if err != nil {
			_ = pconn.Close()

			return nil, c, nil
		}

		return pconn, c, nil
	}
}

func DialWithPacketConn(
	ctx context.Context,
	pconn net.PacketConn,
	addr net.Addr,
	tlsConfig *tls.Config,
	quicConfig *quic.Config,
) (quic.Connection, error) {
	return quic.DialEarly(ctx, pconn, addr, tlsConfig, quicConfig)
}

func NewDialPool(
	pool dutil.LockedMap[string, quic.Connection],
	dial DialFunc,
	cleanInterval time.Duration,
) (
	DialFromPoolFunc,
	func(),
	error,
) {
	var tr *quic.Transport

	if dial == nil {
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
	}

	if cleanInterval < time.Millisecond {
		cleanInterval = time.Minute
	}

	gctx, gctxCancel := context.WithCancel(context.Background())

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

				var errcode quic.ApplicationErrorCode
				var errmsg string

				switch {
				case err != nil:
					errcode = connErrorCodeUnknown
					errmsg = err.Error()
				default:
					errcode = connErrorCodeOK
				}

				gerr = conn.CloseWithError(errcode, errmsg)

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
		func() {
			if tr != nil {
				tr.Close()
			}

			gctxCancel()
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
	// FIXME keep alive?
}
