package quicstream

import (
	"context"
	"crypto/tls"
	"net"

	"github.com/quic-go/quic-go"
)

func Dial(
	ctx context.Context,
	conn net.PacketConn,
	addr net.Addr,
	tlsConfig *tls.Config,
	quicConfig *quic.Config,
) (quic.Connection, error) {
	if tlsConfig.ClientSessionCache == nil {
		tlsConfig.ClientSessionCache = tls.NewLRUClientSessionCache(0)
	}

	return quic.DialEarly(ctx, conn, addr, tlsConfig, quicConfig)
}

// FIXME connection pool
