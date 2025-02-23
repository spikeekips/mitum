package quicstream

import (
	"net"
	"time"

	"github.com/quic-go/quic-go"
	"golang.org/x/crypto/sha3"
)

func NewStatelessResetKey(b []byte) *quic.StatelessResetKey {
	i := quic.StatelessResetKey(sha3.Sum256(b))

	return &i
}

func DefaultQuicConfig() *quic.Config {
	return &quic.Config{
		MaxIncomingStreams:    1 << 14,
		MaxIncomingUniStreams: 1 << 14,
		KeepAlivePeriod:       time.Second,
		Allow0RTT:             true,
	}
}

func NewPacketConn(port int) (net.PacketConn, error) {
	return net.ListenUDP("udp", &net.UDPAddr{IP: net.IPv4zero, Port: port})
}
