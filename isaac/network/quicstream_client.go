package isaacnetwork

import (
	"context"
	"crypto/tls"
	"io"
	"net"
	"time"

	"github.com/lucas-clemente/quic-go"
	"github.com/spikeekips/mitum/network/quicstream"
	"github.com/spikeekips/mitum/network/quictransport"
	"github.com/spikeekips/mitum/util/encoder"
)

type QuicstreamClient struct {
	*baseNetworkClient
	client     *quicstream.PoolClient
	quicconfig *quic.Config
	proto      string
}

func NewQuicstreamClient(
	encs *encoder.Encoders,
	enc encoder.Encoder,
	idleTimeout time.Duration,
	proto string,
	quicconfig *quic.Config,
) *QuicstreamClient {
	c := &QuicstreamClient{
		baseNetworkClient: newBaseNetworkClient(encs, enc, idleTimeout, nil),
		client:            quicstream.NewPoolClient(),
		proto:             proto,
		quicconfig:        quicconfig,
	}

	c.baseNetworkClient.writef = func(
		ctx context.Context,
		ci quictransport.ConnInfo,
		writef quicstream.ClientWriteFunc,
	) (io.ReadCloser, func() error, error) {
		r, err := c.client.Write(ctx, ci.UDPAddr(), writef, c.NewClient(ci))
		if err != nil {
			return nil, nil, err
		}

		return r, func() error {
			r.CancelRead(0)

			return nil
		}, nil
	}

	return c
}

func (c *QuicstreamClient) PoolClient() *quicstream.PoolClient {
	return c.client
}

func (c *QuicstreamClient) NewClient(
	ci quictransport.ConnInfo,
) func(*net.UDPAddr) *quicstream.Client {
	return func(*net.UDPAddr) *quicstream.Client {
		return quicstream.NewClient(
			ci.UDPAddr(),
			&tls.Config{
				InsecureSkipVerify: ci.TLSInsecure(), //nolint:gosec //...
				NextProtos:         []string{c.proto},
			},
			c.quicconfig,
			nil,
		)
	}
}
