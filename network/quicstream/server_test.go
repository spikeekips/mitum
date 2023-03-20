package quicstream

import (
	"bufio"
	"bytes"
	"container/list"
	"context"
	"io"
	"math"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/pkg/errors"
	"github.com/quic-go/quic-go"
	"github.com/spikeekips/mitum/util"
	"github.com/spikeekips/mitum/util/logging"
	"github.com/stretchr/testify/suite"
	"go.uber.org/goleak"
)

type testServer struct {
	BaseTest
}

func (t *testServer) TestNew() {
	srv, err := NewServer(t.Bind, t.TLSConfig, nil, func(net.Addr, io.Reader, io.Writer) error {
		return nil
	})
	t.NoError(err)
	srv.SetLogging(logging.TestNilLogging)

	t.NoError(srv.Start(context.Background()))
	t.NoError(srv.Stop())
}

func (t *testServer) TestEcho() {
	srv := t.NewDefaultServer(nil, t.EchoHandler())

	t.NoError(srv.Start(context.Background()))
	defer srv.Stop()

	client := t.NewClient(t.Bind)

	ctx, cancel := context.WithTimeout(context.Background(), time.Second*1)
	defer cancel()

	b := util.UUID().Bytes()
	r, w, err := client.OpenStream(ctx)
	t.NoError(err)

	_, err = w.Write(b)
	t.NoError(err)

	t.NoError(w.Close())

	rb, err := io.ReadAll(r)
	t.NoError(err)
	t.Equal(b, rb)
}

func (t *testServer) TestEchos() {
	srv := t.NewDefaultServer(nil, t.EchoHandler())

	t.NoError(srv.Start(context.Background()))
	defer srv.Stop()

	client := t.NewClient(t.Bind)

	wk := util.NewErrgroupWorker(context.Background(), math.MaxInt16)
	defer wk.Close()

	go func() {
		for range make([]struct{}, 100) {
			_ = wk.NewJob(func(ctx context.Context, jobid uint64) error {
				b := util.UUID().Bytes()
				r, w, err := client.OpenStream(ctx)
				t.NoError(err)

				_, err = w.Write(b)
				t.NoError(err)

				t.NoError(w.Close())

				rb, err := io.ReadAll(r)
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
	srv := t.NewDefaultServer(&quic.Config{
		MaxIdleTimeout: time.Millisecond * 100,
	}, t.EchoHandler())

	t.NoError(srv.Start(context.Background()))
	defer srv.Stop()

	client := t.NewClient(t.Bind)
	client.quicconfig = &quic.Config{
		MaxIdleTimeout: time.Millisecond * 33,
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	r, w, err := client.OpenStream(ctx)
	go func() {
		select {
		case <-ctx.Done():
		case <-time.After(time.Minute):
			_, _ = w.Write(util.UUID().Bytes())
			_ = w.Close()
		}
	}()

	t.NoError(err)

	_, err = io.ReadAll(r)
	t.Error(err)

	var idleerr *quic.IdleTimeoutError
	t.True(errors.As(err, &idleerr), "%T %+v", err, err)
}

func (t *testServer) TestResponseIdleTimeout() {
	gctx, gcancel := context.WithCancel(context.Background())
	defer gcancel()

	srv := t.NewDefaultServer(nil, func(_ net.Addr, r io.Reader, w io.Writer) error {
		select {
		case <-gctx.Done():
			return nil
		case <-time.After(time.Second * 2):
		}

		b, _ := io.ReadAll(r)
		_, _ = w.Write(b)

		return nil
	})

	t.NoError(srv.Start(gctx))
	defer srv.Stop()

	client := t.NewClient(t.Bind)
	client.quicconfig = &quic.Config{
		MaxIdleTimeout: time.Millisecond * 100,
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	r, w, err := client.OpenStream(ctx)
	t.NoError(err)

	_, err = w.Write(util.UUID().Bytes())
	t.NoError(err)

	t.NoError(w.Close())

	_, err = io.ReadAll(r)
	t.Error(err)

	var idleerr *quic.IdleTimeoutError
	t.True(errors.As(err, &idleerr))
}

func (t *testServer) TestResponseTimeout() {
	gctx, gcancel := context.WithTimeout(context.Background(), time.Second*3)
	defer gcancel()

	srv := t.NewDefaultServer(nil, func(_ net.Addr, r io.Reader, w io.Writer) error {
		select {
		case <-gctx.Done():
			return gctx.Err()
		case <-time.After(time.Minute):
		}

		b, _ := io.ReadAll(r)
		_, _ = w.Write(b)

		return nil
	})

	t.NoError(srv.Start(context.Background()))
	defer srv.Stop()

	client := t.NewClient(t.Bind)
	client.quicconfig = &quic.Config{
		MaxIdleTimeout: time.Minute,
	}

	r, w, err := client.OpenStream(context.Background())
	t.NoError(err)

	_, err = w.Write(util.UUID().Bytes())
	t.NoError(err)

	t.NoError(w.Close())

	_, err = io.ReadAll(r)
	t.Error(err)

	var serr *quic.StreamError
	t.True(errors.As(err, &serr), "%+v %T", err, err)
	t.Equal(quic.StreamErrorCode(0), serr.ErrorCode)
}

func (t *testServer) TestServerGone() {
	srv := t.NewDefaultServer(nil, t.EchoHandler())

	donectx, done := context.WithCancel(context.Background())
	sentch := make(chan struct{}, 1)
	srv.handler = func(_ net.Addr, r io.Reader, w io.Writer) error {
		sentch <- struct{}{}
		select {
		case <-time.After(time.Second * 22):
			b, _ := io.ReadAll(r)
			_, _ = w.Write(b)
		case <-donectx.Done():
		}

		return nil
	}

	t.NoError(srv.Start(context.Background()))
	defer srv.Stop()

	client := t.NewClient(t.Bind)
	client.quicconfig = &quic.Config{
		HandshakeIdleTimeout: time.Millisecond * 300,
		MaxIdleTimeout:       time.Millisecond * 300,
	}

	errch := make(chan error, 1)
	go func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		_, w, err := client.OpenStream(ctx)
		_, _ = w.Write([]byte("showme"))
		_ = w.Close()

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

func (t *testServer) TestStreamReadWrite() {
	bodies := make([]string, 33)
	squeue := list.New()
	cqueue := list.New()

	var sbodies, cbodies []string

	for i := range bodies {
		b := []byte(util.UUID().String())
		bodies[i] = string(b)
		squeue.PushBack(b)
		cqueue.PushBack(b)
	}

	var selem *list.Element

	var sbodieslock sync.Mutex
	donech := make(chan error, 1)

	srv := t.NewDefaultServer(nil, func(_ net.Addr, r io.Reader, w io.Writer) error {
		var err error

	end:
		for {
			b := make([]byte, 36)

			if _, err = util.EnsureRead(r, b); err != nil {
				break end
			}

			if selem == nil {
				selem = squeue.Front()
			}

			switch c := selem.Value.([]byte); {
			case !bytes.Equal(b, c):
				err = errors.Errorf("data mismatched")

				break end
			default:
				func() {
					sbodieslock.Lock()
					defer sbodieslock.Unlock()

					sbodies = append(sbodies, string(b))
				}()
			}

			selem = selem.Next()
			if selem == nil {
				break end
			}

			_, _ = w.Write(selem.Value.([]byte))
		}

		donech <- err

		return err
	})

	t.NoError(srv.Start(context.Background()))
	defer srv.Stop()

	client := t.NewClient(t.Bind)

	var celem *list.Element

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	r, w, err := client.OpenStream(ctx)
	t.NoError(err)
	defer w.Close()

	var i int

end:
	for {
		if celem == nil {
			celem = cqueue.Front()
			c := celem.Value.([]byte)
			cbodies = append(cbodies, string(c))

			_, err := w.Write(c)
			t.NoError(err)
		}

		b := make([]byte, 36)
		switch _, err := util.EnsureRead(r, b); {
		case err == nil:
			cbodies = append(cbodies, string(b))
		case errors.Is(err, io.EOF):
			break end
		}

		switch celem = celem.Next(); {
		case celem == nil:
			break end
		default:
			switch c := celem.Value.([]byte); {
			case !bytes.Equal(b, c):
				t.NoError(errors.Errorf("data mismatched: %d: %q != %q", i, string(b), string(c)))

				break end
			default:
				_, err := w.Write(c)
				t.NoError(err)
			}
		}

		i++
	}

	select {
	case <-time.After(time.Second * 3):
		t.NoError(errors.Errorf("waits done"))
	case err := <-donech:
		t.NoError(err)
	}

	t.Equal(bodies, sbodies)
	t.Equal(bodies, cbodies)
}

func (t *testServer) TestStreamReadWrite() {
	bodies := make([][]byte, 6)
	for i := range bodies {
		bodies[i] = []byte(util.UUID().String())
	}

	var nextlock sync.Mutex
	next := func(b []byte) ([]byte, error) {
		nextlock.Lock()
		defer nextlock.Unlock()

		if b == nil {
			return bodies[0], nil
		}

		for i := range bodies {
			if bytes.Equal(b, bodies[i]) {
				if i == len(bodies)-1 {
					return nil, io.EOF
				}

				return bodies[i+1], nil
			}
		}

		return nil, io.EOF
	}

	var receiveds [][]byte
	var receivedlock sync.Mutex
	receivedf := func(b []byte) {
		receivedlock.Lock()
		defer receivedlock.Unlock()

		receiveds = append(receiveds, b)
	}

	srv, err := NewServer(t.Bind, t.TLSConfig, nil, func(_ net.Addr, r io.Reader, w io.Writer) error {
		buf := bufio.NewReader(r)

	end:
		for {
			b, err := buf.ReadBytes('\n')
			if len(b) > 0 {
				r := b[:len(b)-1]
				receivedf(r)

				n, nerr := next(r)
				if nerr == io.EOF {
					break end
				}

				_, _ = w.Write(append(n, '\n'))
			}

			switch {
			case err == nil:
			case errors.Is(err, io.EOF):
				break end
			default:
				return err
			}
		}

		return nil
	})
	t.NoError(err)

	t.NoError(srv.Start(context.Background()))
	defer srv.Stop()

	client := t.NewClient(t.Bind)

	ctx, ctxcancel := context.WithTimeout(context.Background(), time.Second)
	defer ctxcancel()

	stream, cancel, err := client.Stream(ctx)
	t.NoError(err)

	defer cancel()

	b, _ := next(nil)
	_, _ = stream.Write(append(b, '\n'))

	buf := bufio.NewReader(stream)

end:
	for {
		b, err := buf.ReadBytes('\n')
		if len(b) > 0 {
			r := b[:len(b)-1]
			receivedf(r)

			n, nerr := next(r)
			if nerr == io.EOF {
				break end
			}

			_, _ = stream.Write(append(n, '\n'))
		}

		switch {
		case err == nil:
		case errors.Is(err, io.EOF):
			break end
		default:
			t.NoError(err)
		}
	}

	t.Equal(bodies, receiveds)
}

func TestServer(t *testing.T) {
	defer goleak.VerifyNone(t)

	suite.Run(t, new(testServer))
}
