package isaac

import (
	"context"
	"crypto/tls"
	"io"
	"net"

	"github.com/spikeekips/mitum/network/quicstream"
	"github.com/spikeekips/mitum/network/quictransport"
	"github.com/spikeekips/mitum/util/encoder"
)

type QuicstreamNodeNetworkClient struct {
	*baseNodeNetworkClient
	client *quicstream.PoolClient
	proto  string
}

func NewQuicstreamNodeNetworkClient(
	encs *encoder.Encoders,
	enc encoder.Encoder,
	proto string,
) *QuicstreamNodeNetworkClient {
	c := &QuicstreamNodeNetworkClient{
		baseNodeNetworkClient: newBaseNodeNetworkClient(encs, enc, nil),
		client:                quicstream.NewPoolClient(),
		proto:                 proto,
	}

	c.baseNodeNetworkClient.send = func(
		ctx context.Context,
		ci quictransport.ConnInfo,
		prefix string,
		b []byte,
	) (io.ReadCloser, error) {
		return c.client.Send(
			ctx,
			ci.Address(),
			quicstream.BodyWithPrefix(prefix, b),
			c.newClient(ci),
		)
	}

	return c
}

func (c *QuicstreamNodeNetworkClient) newClient(ci quictransport.ConnInfo) func(*net.UDPAddr) *quicstream.Client {
	return func(*net.UDPAddr) *quicstream.Client {
		return quicstream.NewClient(
			ci.Address(),
			&tls.Config{
				InsecureSkipVerify: ci.Insecure(), // nolint:gosec
				NextProtos:         []string{c.proto},
			},
			nil,
			nil,
			nil,
		)
	}
}
