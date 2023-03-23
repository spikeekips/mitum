package quicstream

import (
	"context"
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

type testPool struct {
	BaseTest
}

func (t *testPool) TestStream() {
	srv := t.NewDefaultServer(nil, t.EchoHandler())

	t.NoError(srv.Start(context.Background()))
	defer srv.Stop()

	p := NewPoolClient()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	b := util.UUID().Bytes()
	r, w, err := p.OpenStream(
		ctx,
		t.Bind,
		func(addr *net.UDPAddr) *Client {
			client := t.NewClient(addr)
			client.quicconfig = &quic.Config{
				HandshakeIdleTimeout: time.Millisecond * 100,
			}

			return client
		},
	)
	t.NoError(err)
	defer r.Close()

	_, err = w.Write(b)
	t.NoError(err)
	t.NoError(w.Close())

	rb, err := io.ReadAll(r)
	t.NoError(err)
	t.Equal(b, rb)
	t.Equal(1, p.clients.Len())

	t.True(p.clients.Exists(t.Bind.String()))
}

func (t *testPool) TestClose() {
	srv := t.NewDefaultServer(nil, t.EchoHandler())

	t.NoError(srv.Start(context.Background()))
	defer srv.Stop()

	p := NewPoolClient()

	b := util.UUID().Bytes()

	t.Run("ok", func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		r, w, err := p.OpenStream(
			ctx,
			t.Bind,
			func(addr *net.UDPAddr) *Client {
				client := t.NewClient(addr)
				client.quicconfig = &quic.Config{
					HandshakeIdleTimeout: time.Millisecond * 100,
				}

				return client
			},
		)
		t.NoError(err)
		defer r.Close()

		_, err = w.Write(b)
		t.NoError(err)
		t.NoError(w.Close())

		rb, err := io.ReadAll(r)
		t.NoError(err)
		t.Equal(b, rb)
	})

	t.Run("write after close", func() {
		t.NoError(p.Close())

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		_, _, err := p.OpenStream(
			ctx,
			t.Bind,
			nil,
		)
		t.Error(err)
		t.True(IsNetworkError(err))
	})
}

func (t *testPool) TestSend() {
	srv := t.NewDefaultServer(nil, t.EchoHandler())

	t.NoError(srv.Start(context.Background()))
	defer srv.Stop()

	p := NewPoolClient()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	b := util.UUID().Bytes()
	r, w, err := p.OpenStream(
		ctx,
		t.Bind,
		func(addr *net.UDPAddr) *Client {
			client := t.NewClient(addr)
			client.quicconfig = &quic.Config{
				HandshakeIdleTimeout: time.Millisecond * 100,
			}

			return client
		},
	)
	t.NoError(err)
	defer r.Close()

	_, err = w.Write(b)
	t.NoError(err)
	t.NoError(w.Close())

	rb, err := io.ReadAll(r)
	t.NoError(err)
	t.Equal(b, rb)
	t.Equal(1, p.clients.Len())

	t.True(p.clients.Exists(t.Bind.String()))
}

func (t *testPool) TestFailedToDial() {
	p := NewPoolClient()

	removedch := make(chan string, 1)
	p.onerrorf = func(_ *net.UDPAddr, c *Client, _ error) {
		removedch <- c.id
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var clientid string
	_, _, err := p.OpenStream(
		ctx,
		t.Bind,
		func(addr *net.UDPAddr) *Client {
			client := t.NewClient(addr)
			client.quicconfig = &quic.Config{
				HandshakeIdleTimeout: time.Millisecond * 100,
			}
			clientid = client.id

			return client
		},
	)
	t.Error(err)

	var nerr net.Error
	t.True(errors.As(err, &nerr))
	t.True(nerr.Timeout())
	t.ErrorContains(err, "no recent network activity")

	removedid := <-removedch
	t.Equal(clientid, removedid)

	t.Equal(0, p.clients.Len())
	t.False(p.clients.Exists(t.Bind.String()))
}

func (t *testPool) TestRemoveAgain() {
	srv := t.NewDefaultServer(nil, t.EchoHandler())

	t.NoError(srv.Start(context.Background()))
	defer srv.Stop()

	p := NewPoolClient()

	var oldclient *Client

	t.Run("remove", func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		_, _, err := p.OpenStream(
			ctx,
			t.Bind,
			func(addr *net.UDPAddr) *Client {
				oldclient = t.NewClient(addr)
				oldclient.quicconfig = &quic.Config{
					HandshakeIdleTimeout: time.Millisecond * 100,
				}

				return oldclient
			},
		)
		t.NoError(err)

		p.onerror(t.Bind, oldclient, &quic.ApplicationError{})
		t.False(p.clients.Exists(t.Bind.String()))
	})

	t.Run("remove again", func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		_, _, err := p.OpenStream(
			ctx,
			t.Bind,
			func(addr *net.UDPAddr) *Client {
				client := t.NewClient(addr)
				client.quicconfig = &quic.Config{
					HandshakeIdleTimeout: time.Millisecond * 100,
				}

				return client
			},
		)
		t.NoError(err)

		// remove again, but new client will not be removed
		p.onerror(t.Bind, oldclient, &quic.ApplicationError{})
		t.True(p.clients.Exists(t.Bind.String()))
	})
}

func (t *testPool) TestClean() {
	srv := t.NewDefaultServer(nil, t.EchoHandler())

	t.NoError(srv.Start(context.Background()))
	defer srv.Stop()

	p := NewPoolClient()

	youngs := make([]string, 4)
	for i := range youngs[:3] {
		uid := util.UUID().String()
		_ = p.clients.SetValue(uid, &poolClientItem{client: t.NewClient(t.Bind), accessed: time.Now()})
		youngs[i] = uid
	}

	dur := time.Minute
	refreshed := t.Bind.String()
	_ = p.clients.SetValue(refreshed, &poolClientItem{client: t.NewClient(t.Bind), accessed: time.Now().Add(dur * -1)})
	youngs[3] = refreshed

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	_, _, err := p.OpenStream(
		ctx,
		t.Bind,
		func(*net.UDPAddr) *Client { return nil },
	)
	t.NoError(err)

	olds := make([]string, 3)
	for i := range olds {
		uid := util.UUID().String()
		_ = p.clients.SetValue(uid, &poolClientItem{client: t.NewClient(t.Bind), accessed: time.Now().Add(dur * -1)})
		olds[i] = uid
	}

	removed := p.Clean(dur)
	t.Equal(len(olds), removed)

	for i := range youngs {
		t.True(p.clients.Exists(youngs[i]))
	}

	for i := range olds {
		t.False(p.clients.Exists(olds[i]))
	}
}

func TestPool(t *testing.T) {
	defer goleak.VerifyNone(t)

	suite.Run(t, new(testPool))
}
