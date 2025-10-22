package quicstream

import (
	"net"
	"time"

	"github.com/pkg/errors"
	"github.com/quic-go/quic-go"
)

func NewPacketConn(bind *net.UDPAddr) (net.PacketConn, error) {
	c, err := net.ListenUDP("udp", bind)

	return c, errors.WithStack(err)
}

func DefaultQuicConfig() *quic.Config {
	return &quic.Config{
		HandshakeIdleTimeout:  time.Second * 3,
		MaxIncomingStreams:    1 << 14,
		MaxIncomingUniStreams: 1 << 14,
		KeepAlivePeriod:       time.Second * 3,
		MaxIdleTimeout:        time.Second * 5,
		Allow0RTT:             true,
		EnableDatagrams:       true,
	}
}
