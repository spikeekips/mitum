package quicstream

import (
	"context"
	"crypto/tls"
	"io"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/pkg/errors"
	"github.com/quic-go/quic-go"
	"github.com/spikeekips/mitum/util"
	"github.com/stretchr/testify/suite"
	"go.uber.org/goleak"
)

type testConnectionPool struct {
	BaseTest
}

func (t *testConnectionPool) prepareServer(quicconfig *quic.Config, handler Handler) (*TestServer, ConnInfo, func()) {
	srv := t.NewDefaultServer(quicconfig, handler)
	t.NoError(srv.EnsureStart(context.Background()))
	t.T().Log("server prepared")

	return srv, UnsafeConnInfo(srv.Bind, true), func() {
		srv.StopWait()
		t.T().Log("server stopped")
	}
}

func (t *testConnectionPool) TestDial() {
	t.Run("ok", func() {
		p, err := NewConnectionPool(3, NewConnInfoDialFunc(
			func() *quic.Config { return nil },
			func() *tls.Config { return t.TLSConfig },
		))
		t.NoError(err)
		defer p.Stop()

		_, ci, deferred := t.prepareServer(nil, nil)
		defer deferred()

		_, err = p.Dial(context.Background(), ci)
		t.NoError(err)

		t.Equal(1, p.conns.Len())

		t.T().Log("close conn")
		t.True(p.Close(ci))
		t.Equal(0, p.conns.Len())
	})

	t.Run("dial unknown", func() {
		p, err := NewConnectionPool(3, NewConnInfoDialFunc(
			func() *quic.Config {
				return &quic.Config{HandshakeIdleTimeout: time.Millisecond * 33}
			},
			func() *tls.Config { return t.TLSConfig },
		))
		t.NoError(err)
		defer p.Stop()

		ci := RandomConnInfo()

		_, err = p.Dial(context.Background(), ci)
		t.Error(err)

		var nerr *net.OpError
		t.True(errors.As(err, &nerr))

		t.Equal(0, p.conns.Len())

		t.False(p.Close(ci))
		t.Equal(0, p.conns.Len())
	})

	t.Run("concurrent", func() {
		p, err := NewConnectionPool(3, NewConnInfoDialFunc(
			func() *quic.Config { return nil },
			func() *tls.Config { return t.TLSConfig },
		))
		t.NoError(err)
		defer p.Stop()

		var conns int64 = 9999
		quicconfig := &quic.Config{MaxIncomingStreams: conns}

		_, ci, deferred := t.prepareServer(quicconfig, nil)
		defer deferred()

		var llock sync.Mutex
		l := func(a string, aa ...interface{}) {
			llock.Lock()
			defer llock.Unlock()

			t.T().Logf(a, aa...)
		}

		worker := util.NewErrgroupWorker(context.Background(), 999)
		go func() {
			for i := range make([]byte, conns) {
				i := i

				err := worker.NewJob(func(ctx context.Context, _ uint64) error {
					if i%133 == 0 {
						l("conn #%d", i)
					}

					conn, err := p.Dial(ctx, ci)
					if err != nil {
						return err
					}

					if err := conn.Stream(ctx, func(_ context.Context, r io.Reader, w io.WriteCloser) error {
						if _, err := w.Write(util.UUID().Bytes()); err != nil {
							return err
						}

						w.Close()

						if _, err := io.ReadAll(r); err != nil {
							return err
						}

						return nil
					}); err != nil {
						return err
					}

					if p.conns.Len() != 1 {
						return errors.Errorf("conns, not 1")
					}

					return nil
				})
				if err != nil {
					l("error #%d: %+v", i, err)
				}
			}

			worker.Done()
		}()

		t.NoError(worker.Wait())

		t.Equal(1, p.conns.Len())
	})
}

func (t *testConnectionPool) TestCloseAll() {
	t.Run("clean", func() {
		p, err := NewConnectionPool(3, NewConnInfoDialFunc(
			func() *quic.Config { return &quic.Config{MaxIdleTimeout: time.Millisecond * 33} },
			func() *tls.Config { return t.TLSConfig },
		))
		t.NoError(err)
		defer p.Stop()

		_, ci, deferred := t.prepareServer(&quic.Config{MaxIdleTimeout: time.Millisecond * 33}, nil)
		defer deferred()

		_, err = p.Dial(context.Background(), ci)
		t.NoError(err)

		t.Equal(1, p.conns.Len())

		t.NoError(p.CloseAll())
		t.Equal(0, p.conns.Len())
	})
}

func (t *testConnectionPool) TestClean() {
	t.Run("clean", func() {
		p, err := NewConnectionPool(3, NewConnInfoDialFunc(
			func() *quic.Config { return &quic.Config{MaxIdleTimeout: time.Millisecond * 33} },
			func() *tls.Config { return t.TLSConfig },
		))
		t.NoError(err)
		defer p.Stop()

		_, ci, deferred := t.prepareServer(&quic.Config{MaxIdleTimeout: time.Millisecond * 33}, nil)
		defer deferred()

		_, err = p.Dial(context.Background(), ci)
		t.NoError(err)

		t.Equal(1, p.conns.Len())

		<-time.After(time.Second * 3)
		t.Equal(0, p.conns.Len())
	})

	t.Run("stream error", func() {
		p, err := NewConnectionPool(3, NewConnInfoDialFunc(
			func() *quic.Config { return nil },
			func() *tls.Config { return t.TLSConfig },
		))
		t.NoError(err)
		defer p.Stop()

		_, ci, deferred := t.prepareServer(nil, func(_ context.Context, _ net.Addr, r io.Reader, w io.WriteCloser) error {
			if _, err := io.ReadAll(r); err != nil {
				return err
			}

			return errors.Errorf("hihihi")
		})
		defer deferred()

		conn, err := p.Dial(context.Background(), ci)
		t.NoError(err)

		t.Equal(1, p.conns.Len())

		t.T().Log("open stream, but stream error from handler")
		conn.Stream(context.Background(), func(_ context.Context, r io.Reader, w io.WriteCloser) error {
			if _, err := w.Write(util.UUID().Bytes()); err != nil {
				return err
			}

			w.Close()

			if _, err := io.ReadAll(r); err != nil {
				return err
			}

			return nil
		})

		<-time.After(time.Second * 3)
		t.Equal(1, p.conns.Len())
	})

	t.Run("connection error", func() {
		p, err := NewConnectionPool(3, NewConnInfoDialFunc(
			func() *quic.Config { return nil },
			func() *tls.Config { return t.TLSConfig },
		))
		t.NoError(err)
		defer p.Stop()

		_, ci, deferred := t.prepareServer(nil, nil)
		defer deferred()

		conn, err := p.Dial(context.Background(), ci)
		t.NoError(err)

		t.Equal(1, p.conns.Len())

		t.T().Log("open stream, but stream error from handler")
		conn.Stream(context.Background(), func(_ context.Context, r io.Reader, w io.WriteCloser) error {
			return &quic.IdleTimeoutError{}
		})

		<-time.After(time.Second)
		t.Equal(0, p.conns.Len())
	})
}

func TestConnectionPool(t *testing.T) {
	defer goleak.VerifyNone(t)

	suite.Run(t, new(testConnectionPool))
}
