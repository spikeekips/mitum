package quicstream

import (
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
