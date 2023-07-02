package quicstream

import (
	"context"
	"crypto/tls"
	"io"
	"net"
	"time"

	"github.com/pkg/errors"
	"github.com/quic-go/quic-go"
	"github.com/spikeekips/mitum/util"
)

var ErrNetwork = util.NewIDError("network error")

type (
	ConnInfoDialFunc func(context.Context, ConnInfo) (Streamer, error)
	StreamFunc       func(context.Context, io.Reader, io.WriteCloser) error
	OpenStreamFunc   func(context.Context) (io.Reader, io.WriteCloser, func() error, error)
)

type Streamer interface {
	Stream(context.Context, StreamFunc) error
	OpenStream(context.Context) (io.Reader, io.WriteCloser, func() error, error)
	Close() error
	Context() context.Context
}

func NewConnInfoDialFunc(
	quicconfigf func() *quic.Config,
	tlsconfigf func() *tls.Config,
) ConnInfoDialFunc {
	return func(ctx context.Context, ci ConnInfo) (Streamer, error) {
		tlsconfig := tlsconfigf()
		if tlsconfig == nil {
			tlsconfig = &tls.Config{}
		}

		tlsconfig.InsecureSkipVerify = ci.TLSInsecure() //nolint:gosec //...

		return Dial(
			ctx,
			ci.UDPAddr(),
			tlsconfig,
			quicconfigf(),
		)
	}
}

type ConnectionPool struct {
	conns util.LockedMap[string, Streamer]
	dialf ConnInfoDialFunc
	Stop  func()
}

func NewConnectionPool(
	size uint64,
	dialf ConnInfoDialFunc,
) (*ConnectionPool, error) {
	conns, err := util.NewLockedMap[string, Streamer](int64(size))
	if err != nil {
		return nil, err
	}

	cctx, ccancel := context.WithCancel(context.Background())

	c := &ConnectionPool{
		conns: conns,
		dialf: dialf,
		Stop:  ccancel,
	}

	go func() {
		ticker := time.NewTicker(time.Second * 3)
		defer ticker.Stop()

		for {
			select {
			case <-cctx.Done():
				return
			case <-ticker.C:
				c.clean()
			}
		}
	}()

	return c, nil
}

func (c *ConnectionPool) Dial(ctx context.Context, ci ConnInfo) (Streamer, error) {
	var conn Streamer

	if _, err := c.conns.Set(ci.Addr().String(), func(old Streamer, _ bool) (Streamer, error) {
		if old != nil && old.Context().Err() == nil {
			conn = old

			return nil, util.ErrLockedSetIgnore.WithStack()
		}

		switch i, err := c.dialf(ctx, ci); {
		case err == nil:
			conn = i

			return i, nil
		default:
			return i, err
		}
	}); err != nil {
		return nil, err
	}

	return connectionPoolStreamer{
		Streamer: conn,
		onerror: func() {
			c.onerror(ci)
		},
	}, nil
}

func (c *ConnectionPool) Close(ci ConnInfo) bool {
	removed, _ := c.conns.Remove(ci.Addr().String(), func(conn Streamer, _ bool) error {
		if i, ok := conn.(io.Closer); ok {
			return errors.WithStack(i.Close())
		}

		return nil
	})

	return removed
}

func (c *ConnectionPool) CloseAll() error {
	var keys []string

	c.conns.Traverse(func(key string, _ Streamer) bool {
		keys = append(keys, key)

		return true
	})

	if len(keys) < 1 {
		return nil
	}

	for i := range keys {
		_, _ = c.conns.Remove(keys[i], func(conn Streamer, found bool) error {
			if !found {
				return nil
			}

			_ = conn.Close()

			return nil
		})
	}

	return nil
}

func (c *ConnectionPool) onerror(ci ConnInfo) {
	_, _ = c.conns.Remove(ci.Addr().String(), func(conn Streamer, _ bool) error {
		if i, ok := conn.(io.Closer); ok {
			return errors.WithStack(i.Close())
		}

		return nil
	})
}

func (c *ConnectionPool) clean() {
	var keys []string

	c.conns.Traverse(func(key string, _ Streamer) bool {
		keys = append(keys, key)

		return true
	})

	if len(keys) < 1 {
		return
	}

	for i := range keys {
		_, _ = c.conns.Remove(keys[i], func(conn Streamer, found bool) error {
			switch {
			case !found,
				conn != nil && conn.Context().Err() != nil:
				return nil
			default:
				return util.ErrLockedSetIgnore.WithStack()
			}
		})
	}
}

type connectionPoolStreamer struct {
	Streamer
	onerror func()
}

func (s connectionPoolStreamer) Stream(ctx context.Context, f StreamFunc) error {
	err := s.Streamer.Stream(ctx, f)
	if IsSeriousError(err) {
		s.onerror()
	}

	return err
}

func IsSeriousError(err error) bool {
	switch {
	case err == nil:
		return false
	case errors.Is(err, ErrNetwork):
		return true
	}

	var terr *quic.TransportError
	var aerr *quic.ApplicationError
	var verr *quic.VersionNegotiationError
	var serr *quic.StatelessResetError
	var ierr *quic.IdleTimeoutError
	var herr *quic.HandshakeTimeoutError
	var nerr net.Error

	switch {
	case
		errors.As(err, &terr),
		errors.As(err, &aerr),
		errors.As(err, &verr),
		errors.As(err, &serr),
		errors.As(err, &ierr),
		errors.As(err, &herr),
		errors.As(err, &nerr):
		return true
	default:
		return false
	}
}
