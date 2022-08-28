package quicstream

import (
	"context"
	"crypto/tls"
	"io"
	"net"
	"sync"

	"github.com/lucas-clemente/quic-go"
	"github.com/pkg/errors"
	"github.com/rs/zerolog"
	"github.com/spikeekips/mitum/util"
	"github.com/spikeekips/mitum/util/logging"
)

type ClientWriteFunc func(io.Writer) error

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
	session    *util.Locked
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
		session:    util.EmptyLocked(),
	}
}

func (c *Client) Close() error {
	_, err := c.session.Set(func(_ bool, i interface{}) (interface{}, error) {
		if i == nil {
			return nil, nil
		}

		if err := i.(quic.EarlyConnection). //nolint:forcetypeassert //...
							CloseWithError(0x100, ""); err != nil { //nolint:gomnd // errorNoError
			return nil, errors.Wrap(err, "failed to close client")
		}

		return nil, nil
	})

	return err
}

func (c *Client) Session() quic.EarlyConnection {
	i, _ := c.session.Value()
	if i == nil {
		return nil
	}

	return i.(quic.EarlyConnection) //nolint:forcetypeassert // ...
}

func (c *Client) Dial(ctx context.Context) (quic.EarlyConnection, error) {
	session, err := c.dial(ctx)
	if err != nil {
		return nil, err
	}

	return session, nil
}

func (c *Client) Write(ctx context.Context, f ClientWriteFunc) (quic.Stream, error) {
	r, err := c.write(ctx, f)
	if err != nil {
		if IsNetworkError(err) {
			_ = c.session.Empty()
		}

		return nil, err
	}

	return r, nil
}

func (c *Client) write(ctx context.Context, f ClientWriteFunc) (quic.Stream, error) {
	e := util.StringErrorFunc("failed to write")

	cctx, cancel := context.WithCancel(ctx)
	defer cancel()

	session, err := c.dial(cctx)
	if err != nil {
		return nil, e(err, "")
	}

	stream, err := session.OpenStreamSync(cctx)
	if err != nil {
		return nil, e(err, "failed to open stream")
	}

	defer func() {
		_ = stream.Close()
	}()

	if err := f(stream); err != nil {
		stream.CancelRead(0)

		return nil, e(err, "")
	}

	_ = stream.Close()

	return stream, nil
}

func (c *Client) dial(ctx context.Context) (quic.EarlyConnection, error) {
	c.Lock()
	defer c.Unlock()

	e := util.StringErrorFunc("failed to dial")

	i, err := c.session.Get(func() (interface{}, error) {
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
		return i.(quic.EarlyConnection), nil //nolint:forcetypeassert // ...
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

func IsNetworkError(err error) bool {
	if err == nil {
		return false
	}

	if e := (&quic.StreamError{}); errors.As(err, &e) {
		return true
	}

	if e := (&quic.TransportError{}); errors.As(err, &e) {
		return true
	}

	if e := (&quic.ApplicationError{}); errors.As(err, &e) {
		return true
	}

	var nerr net.Error

	return errors.As(err, &nerr)
}

func DefaultClientWriteFunc(b []byte) ClientWriteFunc {
	return func(w io.Writer) error {
		_, err := w.Write(b)

		return errors.WithStack(err)
	}
}
