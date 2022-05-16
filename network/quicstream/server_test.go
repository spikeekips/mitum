package quicstream

import (
	"context"
	"io"
	"math"
	"net"
	"testing"
	"time"

	"github.com/lucas-clemente/quic-go"
	"github.com/pkg/errors"
	"github.com/spikeekips/mitum/util"
	"github.com/spikeekips/mitum/util/logging"
	"github.com/stretchr/testify/suite"
	"go.uber.org/goleak"
)

type testServer struct {
	BaseTest
}

func (t *testServer) TestNew() {
	srv := NewServer(t.Bind, t.TLSConfig, nil, func(net.Addr, io.Reader, io.Writer) error {
		return nil
	})
	srv.SetLogging(logging.TestNilLogging)

	t.NoError(srv.Start())
	t.NoError(srv.Stop())
}

func (t *testServer) TestEcho() {
	srv := t.NewDefaultServer()

	t.NoError(srv.Start())
	defer srv.Stop()

	client := t.NewClient(t.Bind)

	ctx, cancel := context.WithTimeout(context.Background(), time.Second*1)
	defer cancel()

	b := util.UUID().Bytes()
	r, err := client.Write(ctx, DefaultClientWriteFunc(b))
	t.NoError(err)
	defer r.Close()

	rb, err := ReadAll(context.Background(), r)
	t.NoError(err)
	t.Equal(b, rb)
}

func (t *testServer) TestEchos() {
	srv := t.NewDefaultServer()

	t.NoError(srv.Start())
	defer srv.Stop()

	client := t.NewClient(t.Bind)

	wk := util.NewErrgroupWorker(context.Background(), math.MaxInt16)
	defer wk.Close()

	go func() {
		for range make([]struct{}, 100) {
			_ = wk.NewJob(func(ctx context.Context, jobid uint64) error {
				b := util.UUID().Bytes()
				r, err := client.Write(ctx, DefaultClientWriteFunc(b))
				t.NoError(err)
				defer r.Close()

				rb, err := ReadAll(context.Background(), r)
				t.NoError(err)
				t.Equal(b, rb)

				return nil
			})
		}

		wk.Done()
	}()

	t.NoError(wk.Wait())
}

func (t *testServer) TestSendTimeout() {
	srv := t.NewDefaultServer()
	srv.quicconfig = &quic.Config{
		MaxIdleTimeout: time.Millisecond * 100,
	}

	t.NoError(srv.Start())
	defer srv.Stop()

	client := t.NewClient(t.Bind)
	client.quicconfig = &quic.Config{
		MaxIdleTimeout: time.Millisecond * 900,
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	r, err := client.Write(ctx, func(w io.Writer) error {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(time.Millisecond * 500):
			_, err := w.Write(util.UUID().Bytes())

			return errors.Wrap(err, "")
		}
	})
	t.NoError(err)
	defer r.Close()

	_, err = ReadAll(context.Background(), r)
	t.Error(err)

	var idleerr *quic.IdleTimeoutError
	t.True(errors.As(err, &idleerr))
}

func (t *testServer) TestResponseIdleTimeout() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	srv := t.NewDefaultServer()
	srv.handler = func(_ net.Addr, r io.Reader, w io.Writer) error {
		select {
		case <-ctx.Done():
			return nil
		case <-time.After(time.Second * 2):
		}

		b, _ := io.ReadAll(r)
		_, _ = w.Write(b)

		return nil
	}

	t.NoError(srv.Start())
	defer srv.Stop()

	client := t.NewClient(t.Bind)
	client.quicconfig = &quic.Config{
		MaxIdleTimeout: time.Millisecond * 100,
	}

	r, err := client.Write(context.Background(), DefaultClientWriteFunc(util.UUID().Bytes()))
	t.NoError(err)
	defer r.Close()

	_, err = ReadAll(context.Background(), r)
	t.Error(err)

	var idleerr *quic.IdleTimeoutError
	t.True(errors.As(err, &idleerr))
}

func (t *testServer) TestResponseContextTimeout() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	srv := t.NewDefaultServer()
	srv.handler = func(_ net.Addr, r io.Reader, w io.Writer) error {
		select {
		case <-ctx.Done():
			return nil
		case <-time.After(time.Second * 2):
		}

		b, _ := io.ReadAll(r)
		_, _ = w.Write(b)

		return nil
	}

	t.NoError(srv.Start())
	defer srv.Stop()

	client := t.NewClient(t.Bind)

	tctx, tcancel := context.WithTimeout(ctx, time.Millisecond*100)
	defer tcancel()

	r, err := client.Write(tctx, DefaultClientWriteFunc(util.UUID().Bytes()))
	t.NoError(err)
	defer r.Close()

	_, err = ReadAll(tctx, r)
	t.Error(err)
	t.True(errors.Is(err, context.DeadlineExceeded))
}

func (t *testServer) TestServerGone() {
	srv := t.NewDefaultServer()

	donectx, done := context.WithCancel(context.Background())
	sentch := make(chan struct{}, 1)
	srv.handler = func(_ net.Addr, r io.Reader, w io.Writer) error {
		sentch <- struct{}{}
		select {
		case <-time.After(time.Second * 2):
			b, _ := io.ReadAll(r)
			_, _ = w.Write(b)
		case <-donectx.Done():
		}

		return nil
	}

	t.NoError(srv.Start())
	defer srv.Stop()

	client := t.NewClient(t.Bind)
	client.quicconfig = &quic.Config{
		HandshakeIdleTimeout: time.Millisecond * 300,
		MaxIdleTimeout:       time.Millisecond * 300,
	}

	errch := make(chan error, 1)
	go func() {
		_, err := client.Write(context.Background(), DefaultClientWriteFunc(util.UUID().Bytes()))
		errch <- err
	}()

	<-sentch
	t.NoError(srv.Stop())

	err := <-errch

	var nerr *quic.ApplicationError
	var serr *quic.StreamError
	switch {
	case errors.As(err, &nerr):
		t.True(nerr.Remote)
		t.Equal(quic.ApplicationErrorCode(0x401), nerr.ErrorCode)
	case errors.As(err, &serr):
		t.Equal(quic.StreamErrorCode(0x401), serr.ErrorCode)
	}

	done()
}

func (t *testServer) TestPrefixHandler() {
	srv := t.NewDefaultServer()

	handler := NewPrefixHandler(func(_ net.Addr, r io.Reader, w io.Writer, err error) error {
		_, _ = w.Write([]byte("hehehe"))

		return nil
	})
	handler.Add("findme", func(_ net.Addr, r io.Reader, w io.Writer) error {
		b, _ := io.ReadAll(r)
		_, _ = w.Write(b)

		return nil
	})

	handler.Add("showme", func(_ net.Addr, r io.Reader, w io.Writer) error {
		b, _ := io.ReadAll(r)
		_, _ = w.Write(b)

		return nil
	})

	srv.handler = Handler(handler.Handler)

	t.NoError(srv.Start())
	defer srv.Stop()

	client := t.NewClient(t.Bind)

	t.Run("findme", func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second*1)
		defer cancel()

		b := util.UUID().Bytes()
		r, err := client.Write(ctx, DefaultClientWriteFunc(BodyWithPrefix("findme", b)))
		t.NoError(err)
		defer r.Close()

		rb, err := ReadAll(context.Background(), r)
		t.NoError(err)
		t.Equal(b, rb)
	})

	t.Run("showme", func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second*1)
		defer cancel()

		b := util.UUID().Bytes()
		r, err := client.Write(ctx, DefaultClientWriteFunc(BodyWithPrefix("showme", b)))
		t.NoError(err)
		defer r.Close()

		rb, err := ReadAll(context.Background(), r)
		t.NoError(err)
		t.Equal(b, rb)
	})

	t.Run("unknown handler", func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second*1)
		defer cancel()

		b := util.UUID().Bytes()
		r, err := client.Write(ctx, DefaultClientWriteFunc(BodyWithPrefix("unknown", b)))
		t.NoError(err)
		defer r.Close()

		rb, err := ReadAll(context.Background(), r)
		t.NoError(err)
		t.Equal([]byte("hehehe"), rb)
	})
}

func TestServer(t *testing.T) {
	defer goleak.VerifyNone(t)

	suite.Run(t, new(testServer))
}
