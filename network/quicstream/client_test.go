package quicstream

import (
	"context"
	"net"
	"testing"
	"time"

	"github.com/pkg/errors"
	"github.com/quic-go/quic-go"
	"github.com/spikeekips/mitum/util"
	"github.com/stretchr/testify/suite"
	"go.uber.org/goleak"
)

type testClient struct {
	BaseTest
}

func (t *testClient) TestSessionClose() {
	srv := t.NewDefaultServer(nil, nil)

	t.NoError(srv.Start(context.Background()))
	defer srv.Stop()

	client := t.NewClient(t.Bind)
	client.quicconfig = &quic.Config{
		HandshakeIdleTimeout: time.Millisecond * 100,
	}

	<-time.After(time.Second * 3) // NOTE for slow machine like github actions

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	_, _, err := client.OpenStream(ctx)
	t.NoError(err)

	i, isnil := client.session.Value()
	t.False(isnil)
	t.NotNil(i)

	t.Run("ok", func() {
		<-time.After(time.Second * 3) // NOTE for slow machine like github actions

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		_, _, err := client.OpenStream(ctx)
		t.NoError(err)
	})

	t.Run("send after close", func() {
		t.NoError(client.Close())

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		_, _, err := client.OpenStream(ctx)
		t.Error(err)
		t.True(IsNetworkError(err))
	})
}

func (t *testClient) TestSessionRemove() {
	srv := t.NewDefaultServer(nil, t.EchoHandler())

	t.NoError(srv.Start(context.Background()))
	defer srv.Stop()

	client := t.NewClient(t.Bind)
	client.quicconfig = &quic.Config{
		HandshakeIdleTimeout: time.Millisecond * 100,
	}

	<-time.After(time.Second * 3) // NOTE for slow machine like github actions

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	_, _, err := client.OpenStream(ctx)
	t.NoError(err)

	i, isnil := client.session.Value()
	t.False(isnil)
	t.NotNil(i)

	t.NoError(srv.Stop())

	t.Run("send after stopped", func() {
		<-time.After(time.Second * 3) // NOTE for slow machine like github actions

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		_, _, err := client.OpenStream(ctx)
		t.Error(err)

		t.True(IsNetworkError(err))

		i, isnil := client.session.Value()
		t.True(isnil)
		t.Nil(i)
	})

	t.Run("send again after stopped", func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		_, _, err := client.OpenStream(ctx)
		t.Error(err)

		var nerr net.Error
		t.True(errors.As(err, &nerr))
		t.True(nerr.Timeout())
		t.ErrorContains(err, "no recent network activity")

		i, isnil := client.session.Value()
		t.True(isnil)
		t.Nil(i)
	})

	newsrv := t.NewDefaultServer(nil, t.EchoHandler())
	t.NoError(newsrv.Start(context.Background()))
	defer newsrv.Stop()

	t.Run("send again after restarting", func() {
		<-time.After(time.Second * 3) // NOTE for slow machine like github actions

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		_, w, err := client.OpenStream(ctx)
		t.NoError(err)

		_, err = w.Write(util.UUID().Bytes())
		t.NoError(err)
		t.NoError(w.Close())
	})
}

func (t *testClient) TestIsNetworkError() {
	t.Run("not", func() {
		err := errors.Errorf("showme")
		t.False(IsNetworkError(err))
	})

	t.Run("quic.ApplicationError", func() {
		err := &quic.ApplicationError{
			Remote:       true,
			ErrorCode:    0x33,
			ErrorMessage: "findme",
		}

		t.True(IsNetworkError(err))
	})

	t.Run("net.Error", func() {
		err := &net.ParseError{
			Type: "a",
			Text: "b",
		}

		t.True(IsNetworkError(err))
	})

	t.Run("net.OpError", func() {
		err := &net.OpError{
			Op:     "dial",
			Net:    "udp",
			Source: nil,
			Addr:   nil,
			Err:    errors.Errorf("eatme"),
		}

		t.True(IsNetworkError(err))
	})
}

func TestClient(t *testing.T) {
	defer goleak.VerifyNone(t)

	suite.Run(t, new(testClient))
}
