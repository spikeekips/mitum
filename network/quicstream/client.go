package quicstream

import (
	"context"
	"crypto/tls"
	"net"
	"sync"

	"github.com/lucas-clemente/quic-go"
	"github.com/pkg/errors"
	"github.com/rs/zerolog"
	"github.com/spikeekips/mitum/util"
	"github.com/spikeekips/mitum/util/logging"
)

type (
	StreamWriteFunc func(quic.Stream, []byte) (int, error)
	DialFunc        func(
		ctx context.Context,
		addr string,
		tlsconfig *tls.Config,
		quicconfig *quic.Config,
	) (quic.EarlyConnection, error)
)

type Client struct {
	sync.Mutex
	*logging.Logging
	id           string
	addr         *net.UDPAddr
	tlsconfig    *tls.Config
	quicconfig   *quic.Config
	dialf        DialFunc
	streamWritef StreamWriteFunc
	session      *util.Locked
}

func NewClient(
	addr *net.UDPAddr,
	tlsconfig *tls.Config,
	quicconfig *quic.Config,
	streamWritef StreamWriteFunc,
	dialf DialFunc,
) *Client {
	lstreamWritef := streamWritef
	if lstreamWritef == nil {
		lstreamWritef = func(stream quic.Stream, b []byte) (int, error) { return stream.Write(b) }
	}

	ldialf := dialf
	if dialf == nil {
		ldialf = dial
	}

	return &Client{
		Logging: logging.NewLogging(func(zctx zerolog.Context) zerolog.Context {
			return zctx.Str("module", "quicstream-client")
		}),
		id:           util.UUID().String(),
		addr:         addr,
		tlsconfig:    tlsconfig,
		quicconfig:   quicconfig,
		streamWritef: lstreamWritef,
		dialf:        ldialf,
		session:      util.EmptyLocked(),
	}
}

func (c *Client) Session() quic.EarlyConnection {
	i, _ := c.session.Value()
	if i == nil {
		return nil
	}

	return i.(quic.EarlyConnection)
}

func (c *Client) Dial(ctx context.Context) (quic.EarlyConnection, error) {
	session, err := c.dial(ctx)
	if err != nil {
		return nil, errors.Wrap(err, "")
	}

	return session, nil
}

func (c *Client) Send(ctx context.Context, b []byte) (quic.Stream, error) {
	e := util.StringErrorFunc("failed to send")

	session, err := c.dial(ctx)
	if err != nil {
		return nil, e(err, "")
	}

	r, err := c.send(ctx, session, b)
	if err == nil {
		return r, nil
	}

	if isNetworkError(err) {
		_ = c.session.Empty()
	}

	return nil, e(err, "")
}

func (c *Client) send(ctx context.Context, session quic.EarlyConnection, b []byte) (quic.Stream, error) {
	e := util.StringErrorFunc("failed to send")

	stream, err := session.OpenStreamSync(ctx)
	if err != nil {
		return nil, e(err, "failed to open stream")
	}
	defer func() {
		_ = stream.Close()
	}()

	if _, err = c.streamWritef(stream, b); err != nil {
		return nil, e(err, "failed to write to stream")
	}

	_ = stream.Close()

	return StreamResponse{stream}, nil
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
	if err != nil {
		return nil, e(err, "")
	}

	return i.(quic.EarlyConnection), nil
}

func dial(
	ctx context.Context,
	addr string,
	tlsconfig *tls.Config,
	quicconfig *quic.Config,
) (quic.EarlyConnection, error) {
	return quic.DialAddrEarlyContext(ctx, addr, tlsconfig, quicconfig)
}

func isNetworkError(err error) bool {
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
	if errors.As(err, &nerr) && nerr != nil {
		return true
	}

	return false
}
