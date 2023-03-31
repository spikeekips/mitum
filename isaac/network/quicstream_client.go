package isaacnetwork

import (
	"context"
	"crypto/tls"
	"io"
	"net"

	"github.com/quic-go/quic-go"
	"github.com/spikeekips/mitum/network/quicstream"
	"github.com/spikeekips/mitum/util/encoder"
)

type QuicstreamClient struct {
	*BaseClient
	client     *quicstream.PoolClient
	quicconfig *quic.Config
	proto      string
}

func NewQuicstreamClient(
	encs *encoder.Encoders,
	enc encoder.Encoder,
	proto string,
	quicconfig *quic.Config,
) *QuicstreamClient {
	c := &QuicstreamClient{
		client:     quicstream.NewPoolClient(),
		proto:      proto,
		quicconfig: quicconfig,
	}

	c.BaseClient = NewBaseClient(encs, enc, c.openstreamFunc(c.client))

	return c
}

func (c *QuicstreamClient) Close() error {
	return c.client.Close()
}

func (c *QuicstreamClient) Clone() *QuicstreamClient {
	return &QuicstreamClient{
		BaseClient: c.BaseClient,
		client:     quicstream.NewPoolClient(),
		proto:      c.proto,
		quicconfig: c.quicconfig.Clone(),
	}
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

func (c *QuicstreamClient) openstreamFunc(client *quicstream.PoolClient) quicstream.OpenStreamFunc {
	return func(ctx context.Context, ci quicstream.UDPConnInfo) (io.Reader, io.WriteCloser, error) {
		return client.OpenStream(ctx, ci.UDPAddr(), c.NewQuicstreamClient(ci))
	}
}
