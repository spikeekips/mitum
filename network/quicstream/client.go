package quicstream

import (
	"context"
	"crypto/tls"
	"net"
	"sync"

	"github.com/pkg/errors"
	"github.com/quic-go/quic-go"
	"github.com/rs/zerolog"
	"github.com/spikeekips/mitum/util"
	"github.com/spikeekips/mitum/util/logging"
)

type (
	DialFunc func(
		ctx context.Context,
		addr string,
		tlsconfig *tls.Config,
		quicconfig *quic.Config,
	) (quic.EarlyConnection, error)
)

type Client struct {
	dialf DialFunc
	*logging.Logging
	session    *util.Locked[quic.EarlyConnection]
	addr       *net.UDPAddr
	tlsconfig  *tls.Config
	quicconfig *quic.Config
	id         string
	sync.Mutex
}

func NewClient(
	addr *net.UDPAddr,
	tlsconfig *tls.Config,
	quicconfig *quic.Config,
	dialf DialFunc,
) *Client {
	ldialf := dialf
	if dialf == nil {
		ldialf = dial
	}

	return &Client{
		Logging: logging.NewLogging(func(zctx zerolog.Context) zerolog.Context {
			return zctx.Str("module", "quicstream-client")
		}),
		id:         util.UUID().String(),
		addr:       addr,
		tlsconfig:  tlsconfig,
		quicconfig: quicconfig,
		dialf:      ldialf,
		session:    util.EmptyLocked[quic.EarlyConnection](),
	}
}

func (c *Client) Close() error {
	_, err := c.session.Set(func(i quic.EarlyConnection, _ bool) (quic.EarlyConnection, error) {
		if i == nil {
			return nil, nil
		}

		if err := i.CloseWithError(0x100, ""); err != nil { //nolint:gomnd // errorNoError
			return nil, errors.Wrap(err, "close client")
		}

		return nil, nil
	})

	return err
}

func (c *Client) Session() quic.EarlyConnection {
	i, _ := c.session.Value()

	return i
}

func (c *Client) Dial(ctx context.Context) (quic.EarlyConnection, error) {
	session, err := c.dial(ctx)
	if err != nil {
		return nil, err
	}

	return session, nil
}

// OpenStream opens new stream. Reader and Writer should be closed.
func (c *Client) OpenStream(ctx context.Context) (reader quic.Stream, writer quic.Stream, _ error) {
	r, w, err := c.openStream(ctx)
	if err != nil {
		if IsNetworkError(err) {
			_ = c.session.EmptyValue()
		}

		return nil, nil, err
	}

	return r, w, nil
}

func (c *Client) openStream(ctx context.Context) (quic.Stream, quic.Stream, error) {
	e := util.StringErrorFunc("request")

	session, err := c.dial(ctx)
	if err != nil {
		return nil, nil, e(err, "")
	}

	stream, err := session.OpenStreamSync(ctx)
	if err != nil {
		return nil, nil, e(err, "open stream")
	}

	return stream, stream, nil
}

func (c *Client) dial(ctx context.Context) (quic.EarlyConnection, error) {
	c.Lock()
	defer c.Unlock()

	e := util.StringErrorFunc("dial")

	i, err := c.session.GetOrCreate(func() (quic.EarlyConnection, error) {
		i, err := c.dialf(ctx, c.addr.String(), c.tlsconfig, c.quicconfig)
		if err != nil {
			return nil, err
		}

		return i, nil
	})

	switch {
	case err != nil:
		return nil, e(err, "")
	case i == nil:
		return nil, &net.OpError{
			Net: "udp", Op: "dial",
			Err: errors.Errorf("already closed"),
		}
	default:
		return i, nil
	}
}

func dial(
	ctx context.Context,
	addr string,
	tlsconfig *tls.Config,
	quicconfig *quic.Config,
) (quic.EarlyConnection, error) {
	c, err := quic.DialAddrEarlyContext(ctx, addr, tlsconfig, quicconfig)

	return c, errors.WithStack(err)
}
