package isaacnetwork

import (
	"context"
	"crypto/tls"
	"io"
	"net"

	"github.com/spikeekips/mitum/network/quicstream"
	"github.com/spikeekips/mitum/network/quictransport"
	"github.com/spikeekips/mitum/util/encoder"
)

type QuicstreamClient struct {
	*baseNetworkClient
	client *quicstream.PoolClient
	proto  string
}

func NewQuicstreamClient(
	encs *encoder.Encoders,
	enc encoder.Encoder,
	proto string,
) *QuicstreamClient {
	c := &QuicstreamClient{
		baseNetworkClient: newBaseNetworkClient(encs, enc, nil),
		client:            quicstream.NewPoolClient(),
		proto:             proto,
	}

	c.baseNetworkClient.writef = func(
		ctx context.Context,
		ci quictransport.ConnInfo,
		writef quicstream.ClientWriteFunc,
	) (io.ReadCloser, error) {
		return c.client.Write(ctx, ci.Address(), writef, c.newClient(ci))
	}

	return c
}

func (c *QuicstreamClient) newClient(ci quictransport.ConnInfo) func(*net.UDPAddr) *quicstream.Client {
	return func(*net.UDPAddr) *quicstream.Client {
		return quicstream.NewClient(
			ci.Address(),
			&tls.Config{
				InsecureSkipVerify: ci.Insecure(), //nolint:gosec //...
				NextProtos:         []string{c.proto},
			},
			nil,
			nil,
		)
	}
}