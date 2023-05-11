package quicstreamheader

import (
	"context"
	"io"

	"github.com/spikeekips/mitum/network/quicstream"
	"github.com/spikeekips/mitum/util/encoder"
)

type (
	ClientBrokerFunc func(context.Context, quicstream.UDPConnInfo) (*ClientBroker, error)
	OpenStreamFunc   func(context.Context, quicstream.UDPConnInfo) (io.Reader, io.WriteCloser, error)
)

type Client struct {
	Encoders    *encoder.Encoders
	Encoder     encoder.Encoder
	openStreamf OpenStreamFunc
}

func NewClient(encs *encoder.Encoders, enc encoder.Encoder, openStreamf OpenStreamFunc) *Client {
	return &Client{
		Encoders:    encs,
		Encoder:     enc,
		openStreamf: openStreamf,
	}
}

func (c *Client) OpenStream(ctx context.Context, ci quicstream.UDPConnInfo) (io.Reader, io.WriteCloser, error) {
	return c.openStreamf(ctx, ci)
}

func (c *Client) Broker(ctx context.Context, ci quicstream.UDPConnInfo) (*ClientBroker, error) {
	r, w, err := c.openStreamf(ctx, ci)
	if err != nil {
		return nil, err
	}

	return NewClientBroker(c.Encoders, c.Encoder, r, w), nil
}
