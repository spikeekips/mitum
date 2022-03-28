package quicstream

import (
	"context"
	"net"
	"testing"
	"time"

	"github.com/lucas-clemente/quic-go"
	"github.com/pkg/errors"
	"github.com/spikeekips/mitum/util"
	"github.com/stretchr/testify/suite"
	"go.uber.org/goleak"
)

type testClient struct {
	BaseTest
}

func (t *testClient) TestSessionRemove() {
	srv := t.NewDefaultServer()

	t.NoError(srv.Start())
	defer srv.Stop()

	client := t.NewClient(t.Bind)
	client.quicconfig = &quic.Config{
		HandshakeIdleTimeout: time.Millisecond * 100,
	}

	_, err := client.Send(context.Background(), util.UUID().Bytes())
	t.NoError(err)

	i, isnil := client.session.Value()
	t.False(isnil)
	t.NotNil(i)

	t.NoError(srv.Stop())

	t.Run("send after stopped", func() {
		_, err = client.Send(context.Background(), util.UUID().Bytes())
		t.Error(err)

		t.True(isNetworkError(err))

		i, isnil := client.session.Value()
		t.True(isnil)
		t.Nil(i)
	})

	t.Run("send again after stopped", func() {
		_, err = client.Send(context.Background(), util.UUID().Bytes())
		t.Error(err)

		var nerr net.Error
		t.True(errors.As(err, &nerr))
		t.True(nerr.Timeout())
		t.Contains(err.Error(), "no recent network activity")

		i, isnil := client.session.Value()
		t.True(isnil)
		t.Nil(i)
	})

	newsrv := t.NewDefaultServer()
	t.NoError(newsrv.Start())
	defer newsrv.Stop()

	t.Run("send again after restarting", func() {
		_, err = client.Send(context.Background(), util.UUID().Bytes())
		t.NoError(err)
	})
}

func (t *testClient) TestIsNetworkError() {
	t.Run("not", func() {
		err := errors.Errorf("showme")
		t.False(isNetworkError(err))
	})

	t.Run("quic.ApplicationError", func() {
		err := &quic.ApplicationError{
			Remote:       true,
			ErrorCode:    0x33,
			ErrorMessage: "findme",
		}

		t.True(isNetworkError(err))
	})

	t.Run("net.Error", func() {
		err := &net.ParseError{
			Type: "a",
			Text: "b",
		}

		t.True(isNetworkError(err))
	})

	t.Run("net.OpError", func() {
		err := &net.OpError{
			Op:     "dial",
			Net:    "udp",
			Source: nil,
			Addr:   nil,
			Err:    errors.Errorf("eatme"),
		}

		t.True(isNetworkError(err))
	})
}

func TestClient(t *testing.T) {
	defer goleak.VerifyNone(t)

	suite.Run(t, new(testClient))
}
