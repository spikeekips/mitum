package quicstream

import (
	"context"
	"crypto/tls"
	"io"
	"net"
	"testing"
	"time"

	"github.com/pkg/errors"
	"github.com/quic-go/quic-go"
	"github.com/spikeekips/mitum/util"
	"github.com/stretchr/testify/suite"
	"go.uber.org/goleak"
)

type testConnection struct {
	BaseTest
}

func (t *testConnection) TestDial() {
	t.Run("unknown", func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()

		conn, err := Dial(
			ctx,
			t.NewAddr(),
			&tls.Config{
				InsecureSkipVerify: true,
				NextProtos:         []string{t.Proto},
			},
			nil,
		)
		t.Error(err)
		t.Nil(conn)

		t.T().Logf("error: %T %+v", err, err)

		var operr *net.OpError
		t.True(errors.As(err, &operr))

		t.T().Logf("op error: %T %+v", operr.Err, operr.Err)
		t.True(errors.Is(err, context.DeadlineExceeded))
	})

	srv := t.NewDefaultServer(nil, nil)
	t.NoError(srv.EnsureStart(context.Background()))
	t.T().Log("server prepared")
	defer srv.StopWait()
	defer t.T().Log("server stopped")

	t.Run("ok", func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()

		conn, err := Dial(
			ctx,
			srv.Bind,
			&tls.Config{
				InsecureSkipVerify: true,
				NextProtos:         []string{t.Proto},
			},
			nil,
		)
		t.NoError(err)
		t.NotNil(conn)
	})

	t.Run("close", func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()

		conn, err := Dial(
			ctx,
			srv.Bind,
			&tls.Config{
				InsecureSkipVerify: true,
				NextProtos:         []string{t.Proto},
			},
			nil,
		)
		t.NoError(err)
		t.NotNil(conn)

		t.NoError(conn.Close())
		t.NoError(conn.Close())
	})
}

func (t *testConnection) TestOpenStream() {
	t.Run("max reached", func() {
		qconfig := &quic.Config{MaxIncomingStreams: 1}

		opench := make(chan struct{}, 3)

		srv := t.NewDefaultServer(qconfig, func(ctx context.Context, _ net.Addr, r io.Reader, w io.WriteCloser) error {
			select {
			case <-ctx.Done():
			case <-opench:
			case <-time.After(time.Second * 33):
			}

			return nil
		})
		t.NoError(srv.EnsureStart(context.Background()))
		t.T().Log("server prepared")
		defer srv.StopWait()
		defer t.T().Log("server stopped")

		conn := t.NewConnection(t.Bind, nil)
		t.T().Log("connected", conn.ID())

		{
			t.T().Log("open stream")

			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()

			t.NoError(conn.Stream(ctx, func(_ context.Context, r io.Reader, w io.WriteCloser) error {
				return nil
			}))
		}

		{
			t.T().Log("open stream #2; reached max")

			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()

			err := conn.Stream(ctx, func(_ context.Context, r io.Reader, w io.WriteCloser) error {
				return nil
			})
			t.Error(err)
			t.True(errors.Is(err, ErrOpenStream))
		}

		opench <- struct{}{}

		t.T().Log("open stream #3; wait until available")

		t.NoError(conn.Stream(context.Background(), func(_ context.Context, r io.Reader, w io.WriteCloser) error {
			return nil
		}))

		t.T().Log("close conn")
		t.NoError(conn.Close())
		t.NoError(conn.Close())

		t.Run("open stream after closing", func() {
			err := conn.Stream(context.Background(), func(_ context.Context, r io.Reader, w io.WriteCloser) error {
				return nil
			})
			t.Error(err)
			t.ErrorContains(err, "closed")
		})
	})
}

func (t *testConnection) TestEcho() {
	handlerechoedch := make(chan struct{}, 1)

	srv := t.NewDefaultServer(nil, func(_ context.Context, _ net.Addr, r io.Reader, w io.WriteCloser) error {
		defer func() {
			handlerechoedch <- struct{}{}
		}()

		t.T().Log("> echo handler")
		defer t.T().Log("< echo handler")

		defer w.Close()

		b, err := io.ReadAll(r)
		if err != nil {
			t.T().Log("> echo handler; read error:", err)
			return err
		}
		t.T().Log("> echo handler; read")

		_, err = w.Write(b)
		if err != nil {
			t.T().Log("> echo handler; write error:", err)
			return err
		}

		t.T().Log("> echo handler; write")

		return nil
	})

	t.NoError(srv.EnsureStart(context.Background()))
	t.T().Log("server prepared")
	defer srv.StopWait()
	defer t.T().Log("server stopped")

	conn := t.NewConnection(t.Bind, nil)
	t.T().Log("connected")

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	t.Run("client error", func() {
		t.T().Log("stream")

		sent := util.UUID().Bytes()

		err := conn.Stream(ctx, func(_ context.Context, r io.Reader, w io.WriteCloser) error {
			_, err := w.Write(sent)
			if err != nil {
				return err
			}

			return errors.Errorf("hehehe")
		})
		t.Error(err)
		t.ErrorContains(err, "hehehe")

		<-handlerechoedch
	})

	t.Run("ok", func() {
		t.T().Log("stream")
		sent := util.UUID().Bytes()

		var received []byte
		t.NoError(conn.Stream(ctx, func(_ context.Context, r io.Reader, w io.WriteCloser) error {
			_, err := w.Write(sent)
			if err != nil {
				return err
			}
			if err := w.Close(); err != nil {
				return err
			}

			received, err = io.ReadAll(r)
			if err != nil {
				return err
			}

			return nil
		}))
		t.T().Log("echoed")

		t.Equal(sent, received)

		<-handlerechoedch
	})
}

func (t *testConnection) TestClosed() {
	t.Run("dialed, but idle timeout in server", func() {
		qconfig := &quic.Config{MaxIdleTimeout: time.Millisecond * 333}

		srv := t.NewDefaultServer(qconfig, nil)
		t.NoError(srv.EnsureStart(context.Background()))
		t.T().Log("server prepared")
		defer srv.StopWait()
		defer t.T().Log("server stopped")

		conn, err := Dial(
			context.Background(),
			srv.Bind,
			&tls.Config{
				InsecureSkipVerify: true,
				NextProtos:         []string{t.Proto},
			},
			&quic.Config{MaxIdleTimeout: time.Minute},
		)
		t.NoError(err)

		t.T().Log("wait closed")
		<-time.After(time.Second * 9)
		t.NotNil(conn.Context().Err())
	})

	t.Run("dialed, but idle timeout in client", func() {
		qconfig := &quic.Config{MaxIdleTimeout: time.Minute}

		srv := t.NewDefaultServer(qconfig, nil)
		t.NoError(srv.EnsureStart(context.Background()))
		t.T().Log("server prepared")
		defer srv.StopWait()
		defer t.T().Log("server stopped")

		conn, err := Dial(
			context.Background(),
			srv.Bind,
			&tls.Config{
				InsecureSkipVerify: true,
				NextProtos:         []string{t.Proto},
			},
			&quic.Config{MaxIdleTimeout: time.Millisecond * 33},
		)
		t.NoError(err)

		t.T().Log("wait closed")
		<-time.After(time.Second * 3)
		t.NotNil(conn.Context().Err())
	})

	t.Run("stream opened, but idle timeout in server", func() {
		qconfig := &quic.Config{MaxIdleTimeout: time.Millisecond * 333}

		srv := t.NewDefaultServer(qconfig, nil)
		t.NoError(srv.EnsureStart(context.Background()))
		t.T().Log("server prepared")
		defer srv.StopWait()
		defer t.T().Log("server stopped")

		conn, err := Dial(
			context.Background(),
			srv.Bind,
			&tls.Config{
				InsecureSkipVerify: true,
				NextProtos:         []string{t.Proto},
			},
			&quic.Config{MaxIdleTimeout: time.Minute},
		)
		t.NoError(err)

		t.T().Log("open stream")
		t.NoError(conn.Stream(context.Background(), func(_ context.Context, r io.Reader, w io.WriteCloser) error {
			_, err := w.Write(util.UUID().Bytes())
			if err != nil {
				return err
			}
			if err := w.Close(); err != nil {
				return err
			}

			if _, err = io.ReadAll(r); err != nil {
				return err
			}

			return nil
		}))

		t.T().Log("wait closed")
		<-time.After(time.Second * 9)
		t.NotNil(conn.Context().Err())
	})

	t.Run("open stream, but idle timeout in client", func() {
		qconfig := &quic.Config{MaxIdleTimeout: time.Minute}

		srv := t.NewDefaultServer(qconfig, nil)
		t.NoError(srv.EnsureStart(context.Background()))
		t.T().Log("server prepared")
		defer srv.StopWait()
		defer t.T().Log("server stopped")

		conn, err := Dial(
			context.Background(),
			srv.Bind,
			&tls.Config{
				InsecureSkipVerify: true,
				NextProtos:         []string{t.Proto},
			},
			&quic.Config{MaxIdleTimeout: time.Millisecond * 33},
		)
		t.NoError(err)

		t.T().Log("open stream")
		t.NoError(conn.Stream(context.Background(), func(_ context.Context, r io.Reader, w io.WriteCloser) error {
			_, err := w.Write(util.UUID().Bytes())
			if err != nil {
				return err
			}
			if err := w.Close(); err != nil {
				return err
			}

			if _, err = io.ReadAll(r); err != nil {
				return err
			}

			return nil
		}))

		t.T().Log("wait closed")
		<-time.After(time.Second * 3)
		t.NotNil(conn.Context().Err())
	})
}

func TestConnection(t *testing.T) {
	defer goleak.VerifyNone(t)

	suite.Run(t, new(testConnection))
}
