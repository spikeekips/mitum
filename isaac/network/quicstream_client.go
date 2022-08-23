package isaacnetwork

import (
	"context"
	"crypto/tls"
	"io"
	"net"
	"time"

	"github.com/lucas-clemente/quic-go"
	"github.com/spikeekips/mitum/network/quicstream"
	"github.com/spikeekips/mitum/util/encoder"
)

type QuicstreamClient struct {
	*BaseNetworkClient
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
		BaseNetworkClient: NewBaseNetworkClient(encs, enc, idleTimeout, nil),
		client:            quicstream.NewPoolClient(),
		proto:             proto,
		quicconfig:        quicconfig,
	}

	c.BaseNetworkClient.writef = c.writeFunc(c.client)

	return c
}

func (c *QuicstreamClient) Close() error {
	return c.client.Close()
}

func (c *QuicstreamClient) Clone() *QuicstreamClient {
	n := &QuicstreamClient{
		BaseNetworkClient: c.BaseNetworkClient.NewClient(),
		client:            quicstream.NewPoolClient(),
		proto:             c.proto,
		quicconfig:        c.quicconfig.Clone(),
	}

	n.BaseNetworkClient.writef = n.writeFunc(n.client)

	return n
}

func (c *QuicstreamClient) NewQuicstreamClient(
	ci quicstream.UDPConnInfo,
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

func (c *QuicstreamClient) writeFunc(client *quicstream.PoolClient) BaseNetworkClientWriteFunc {
	return func(
		ctx context.Context,
		ci quicstream.UDPConnInfo,
		writef quicstream.ClientWriteFunc,
	) (io.ReadCloser, func() error, error) {
		r, err := client.Write(ctx, ci.UDPAddr(), writef, c.NewQuicstreamClient(ci))
		if err != nil {
			return nil, nil, err
		}

		return r, func() error {
			r.CancelRead(0)

			return nil
		}, nil
	}
}
