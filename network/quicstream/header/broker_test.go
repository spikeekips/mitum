package quicstreamheader

import (
	"bytes"
	"context"
	"crypto/tls"
	"io"
	"net"
	"testing"
	"time"

	"github.com/pkg/errors"
	"github.com/quic-go/quic-go"
	"github.com/spikeekips/mitum/network/quicstream"
	"github.com/spikeekips/mitum/util"
	"github.com/spikeekips/mitum/util/encoder"
	jsonenc "github.com/spikeekips/mitum/util/encoder/json"
	"github.com/spikeekips/mitum/util/hint"
	"github.com/stretchr/testify/suite"
	"go.uber.org/goleak"
)

type testBrokers struct {
	quicstream.BaseTest
	encs *encoder.Encoders
	enc  encoder.Encoder
}

func (t *testBrokers) SetupSuite() {
	t.BaseTest.SetupSuite()

	t.enc = jsonenc.NewEncoder()
	t.encs = encoder.NewEncoders(t.enc, t.enc)

	t.NoError(t.encs.AddDetail(encoder.DecodeDetail{Hint: DefaultResponseHeaderHint, Instance: DefaultResponseHeader{}}))
	t.NoError(t.encs.AddDetail(encoder.DecodeDetail{Hint: dummyRequestHeaderHint, Instance: dummyRequestHeader{}}))
}

func (t *testBrokers) clientBroker(ctx context.Context, ci quicstream.ConnInfo, tlsConfig *tls.Config) (*ClientBroker, func() error) {
	streamer, err := quicstream.NewConnInfoDialFunc(
		func() *quic.Config { return nil },
		func() *tls.Config { return tlsConfig },
	)(ctx, ci)
	t.NoError(err)

	r, w, closef, err := streamer.OpenStream(ctx)
	t.NoError(err)

	return NewClientBroker(t.encs, t.enc, r, w), func() error {
		closef()

		return streamer.Close()
	}
}

func (t *testBrokers) dialBroker(ctx context.Context, ci quicstream.ConnInfo, tlsConfig *tls.Config) (StreamFunc, func() error, error) {
	return NewDialFunc(
		quicstream.NewConnInfoDialFunc(
			func() *quic.Config { return nil },
			func() *tls.Config { return tlsConfig },
		),
		t.encs,
		t.enc,
	)(ctx, ci)
}

func (t *testBrokers) server(name quicstream.HandlerName) (
	*quicstream.TestServer,
	*quicstream.PrefixHandler,
	chan error,
	func(context.Context) (*ClientBroker, func() error),
) {
	errch := make(chan error, 1)

	ph := quicstream.NewPrefixHandler(func(ctx context.Context, _ net.Addr, _ io.Reader, _ io.WriteCloser, err error) (context.Context, error) {
		errch <- err

		return ctx, nil
	})

	ph.Add(name, func(ctx context.Context, _ net.Addr, _ io.Reader, _ io.WriteCloser) (context.Context, error) {
		return ctx, errors.Errorf("unknown request")
	})

	srv := t.NewDefaultServer(nil, quicstream.Handler(ph.Handler))

	t.NoError(srv.Start(context.Background()))

	return srv, ph, errch, func(ctx context.Context) (*ClientBroker, func() error) {
		broker, closef := t.clientBroker(ctx, quicstream.UnsafeConnInfo(srv.Bind, true), srv.TLSConfig)

		return broker, func() error {
			return closef()
		}
	}
}

func (t *testBrokers) TestRequestHeader() {
	t.Run("handler timeout; before receiving header", func() {
		name := quicstream.HandlerName(util.UUID().String())
		srv, ph, _, _ := t.server(name)
		defer srv.StopWait()

		ph.Add(name, quicstream.TimeoutHandler(NewHandler(t.encs,
			func(ctx context.Context, _ net.Addr, broker *HandlerBroker, header RequestHeader) (context.Context, error) {
				select {
				case <-ctx.Done():
				case <-time.After(time.Second * 33):
				}

				return ctx, nil
			}, func(
				ctx context.Context,
				_ net.Addr,
				broker *HandlerBroker,
				err error,
			) (context.Context, error) {
				return ctx, nil
			},
		),
			func() time.Duration {
				return time.Nanosecond
			},
		))

		ctx := context.Background()
		f, closef, err := t.dialBroker(ctx, quicstream.UnsafeConnInfo(srv.Bind, true), srv.TLSConfig)
		t.NoError(err)
		defer closef()

		f(ctx, func(ctx context.Context, broker *ClientBroker) error {
			reqh := newDummyRequestHeader(name, util.UUID().String())

			t.T().Log("write request head")
			t.NoError(broker.WriteRequestHead(ctx, reqh))

			_, _, err := broker.ReadResponseHead(ctx)
			t.Error(err)
			t.ErrorContains(err, "insufficient read", "%T %+v", err, err)

			return nil
		})
	})

	t.Run("handler timeout; after receiving header", func() {
		name := quicstream.HandlerName(util.UUID().String())
		srv, ph, _, _ := t.server(name)
		defer srv.StopWait()

		errch := make(chan error, 1)
		ph.Add(name, quicstream.TimeoutHandler(NewHandler(t.encs,
			func(ctx context.Context, _ net.Addr, broker *HandlerBroker, header RequestHeader) (context.Context, error) {
				select {
				case <-ctx.Done():
				case <-time.After(time.Second * 33):
				}

				return ctx, nil
			}, func(
				ctx context.Context,
				_ net.Addr,
				broker *HandlerBroker,
				err error,
			) (context.Context, error) {
				errch <- err

				return ctx, broker.WriteResponseHeadOK(ctx, false, err)
			},
		),
			func() time.Duration {
				return time.Second * 3
			},
		))

		ctx := context.Background()
		f, closef, err := t.dialBroker(ctx, quicstream.UnsafeConnInfo(srv.Bind, true), srv.TLSConfig)
		t.NoError(err)
		defer closef()

		reqh := newDummyRequestHeader(name, util.UUID().String())

		f(ctx, func(ctx context.Context, broker *ClientBroker) error {
			t.T().Log("write request head")
			t.NoError(broker.WriteRequestHead(ctx, reqh))

			_, _, err := broker.ReadResponseHead(ctx)
			t.Error(err)
			t.ErrorContains(err, "read response head")

			select {
			case <-time.After(time.Second * 2):
			case <-errch:
				t.Fail("err handler called")
			}

			return nil
		})
	})

	name := quicstream.HandlerName(util.UUID().String())
	srv, ph, errch, _ := t.server(name)
	defer srv.StopWait()

	t.Run("request, response", func() {
		var denc encoder.Encoder
		var dreqh RequestHeader

		ph.Add(name, NewHandler(t.encs, func(ctx context.Context, _ net.Addr, broker *HandlerBroker, header RequestHeader) (context.Context, error) {
			denc = broker.Encoder
			dreqh = header

			errch <- nil

			resh := NewDefaultResponseHeader(true, nil)

			return ctx, broker.WriteResponseHead(ctx, resh)
		}, nil))

		ctx, cancel := context.WithTimeout(context.Background(), time.Second*1)
		defer cancel()

		f, closef, err := t.dialBroker(ctx, quicstream.UnsafeConnInfo(srv.Bind, true), srv.TLSConfig)
		t.NoError(err)
		defer closef()

		reqh := newDummyRequestHeader(name, util.UUID().String())

		f(ctx, func(ctx context.Context, broker *ClientBroker) error {
			t.T().Log("write request head")
			t.NoError(broker.WriteRequestHead(ctx, reqh))

			select {
			case <-time.After(time.Second * 2):
				t.Fail("failed to wait response")
			case err := <-errch:
				t.NoError(err)

				t.True(t.enc.Hint().Equal(denc.Hint()))

				ddreqh, ok := dreqh.(dummyRequestHeader)
				t.True(ok)

				t.Equal(reqh.ID, ddreqh.ID)
			}

			t.T().Log("read response head")
			renc, rresh, err := broker.ReadResponseHead(ctx)
			t.NoError(err)

			t.True(t.enc.Hint().Equal(renc.Hint()))
			t.True(rresh.OK())
			t.Nil(rresh.Err())

			return nil
		})
	})

	t.Run("failed to write", func() {
		ph.Add(name, NewHandler(t.encs, func(ctx context.Context, _ net.Addr, broker *HandlerBroker, _ RequestHeader) (context.Context, error) {
			return ctx, nil
		}, nil))

		ctx, cancel := context.WithTimeout(context.Background(), time.Second*1)
		defer cancel()

		t.T().Log("close writer")
		f, closef, err := t.dialBroker(ctx, quicstream.UnsafeConnInfo(srv.Bind, true), srv.TLSConfig)
		t.NoError(err)
		defer closef()

		reqh := newDummyRequestHeader(name, util.UUID().String())

		f(ctx, func(ctx context.Context, broker *ClientBroker) error {
			broker.Close()

			t.T().Log("write request head")
			err := broker.WriteRequestHead(ctx, reqh)

			t.Error(err)
			t.ErrorIs(err, quicstream.ErrNetwork)

			return nil
		})
	})
}

func (t *testBrokers) TestRequestHeaderButHandlerError() {
	name := quicstream.HandlerName(util.UUID().String())
	srv, ph, errch, brokerf := t.server(name)
	defer srv.StopWait()

	t.Run("error handler", func() {
		errhandlerch := make(chan error, 1)

		ph.Add(name, NewHandler(t.encs, func(ctx context.Context, _ net.Addr, _ *HandlerBroker, _ RequestHeader) (context.Context, error) {
			return ctx, errors.Errorf("hehehe")
		}, func(ctx context.Context, _ net.Addr, broker *HandlerBroker, err error) (context.Context, error) {
			errhandlerch <- err

			return ctx, errors.Errorf("hohoho")
		}))

		ctx, cancel := context.WithTimeout(context.Background(), time.Second*1)
		defer cancel()

		broker, closef := brokerf(ctx)
		defer closef()

		reqh := newDummyRequestHeader(name, util.UUID().String())

		t.T().Log("write request head")
		t.NoError(broker.WriteRequestHead(ctx, reqh))

		select {
		case <-time.After(time.Second * 2):
			t.Fail("failed to wait handler error")
		case err := <-errch:
			t.Error(err)
			t.ErrorContains(err, "hohoho")
		}

		t.T().Log("error handler")
		select {
		case <-time.After(time.Second * 2):
			t.Fail("failed to wait error handler")
		case err := <-errhandlerch:
			t.Error(err)
			t.ErrorContains(err, "hehehe")
		}

		t.T().Log("read response head")
		_, _, err := broker.ReadResponseHead(ctx)
		t.Error(err)
		t.ErrorContains(err, "insufficient read", "%T %+v", err, err)
	})

	t.Run("error handler response", func() {
		errhandlerch := make(chan error, 1)

		ph.Add(name, NewHandler(t.encs, func(ctx context.Context, _ net.Addr, _ *HandlerBroker, _ RequestHeader) (context.Context, error) {
			return ctx, errors.Errorf("hehehe")
		}, func(ctx context.Context, _ net.Addr, broker *HandlerBroker, err error) (context.Context, error) {
			errhandlerch <- err

			return ctx, broker.WriteResponseHeadOK(ctx, false, err)
		}))

		ctx := context.Background()
		broker, closef := brokerf(ctx)
		defer closef()

		reqh := newDummyRequestHeader(name, util.UUID().String())

		t.T().Log("write request head")
		t.NoError(broker.WriteRequestHead(ctx, reqh))

		select {
		case <-time.After(time.Second * 2):
		case err := <-errch:
			t.NoError(errors.WithMessage(err, "unexpected handler error"))
		}

		t.T().Log("error handler")
		select {
		case <-time.After(time.Second * 2):
			t.Fail("failed to wait error handler")
		case err := <-errhandlerch:
			t.Error(err)
			t.ErrorContains(err, "hehehe")
		}

		t.T().Log("read response head")
		renc, rresh, err := broker.ReadResponseHead(ctx)
		t.NoError(err)

		t.True(t.enc.Hint().Equal(renc.Hint()))
		t.False(rresh.OK())
		t.Error(rresh.Err())
		t.ErrorContains(rresh.Err(), "hehehe")
	})

	t.Run("unexpected response", func() {
		name := quicstream.HandlerName(util.UUID().String())
		ph.Add(name, NewHandler(t.encs, func(ctx context.Context, _ net.Addr, broker *HandlerBroker, _ RequestHeader) (context.Context, error) {
			resh := NewDefaultResponseHeader(false, errors.Errorf("hehehe"))

			defer func() {
				errch <- nil
			}()

			return ctx, broker.WriteResponseHead(ctx, resh)
		}, nil))

		ctx, cancel := context.WithTimeout(context.Background(), time.Second*1)
		defer cancel()

		broker, closef := brokerf(ctx)
		defer closef()

		reqh := newDummyRequestHeader(name, util.UUID().String())

		t.T().Log("write request head")
		t.NoError(broker.WriteRequestHead(ctx, reqh))

		select {
		case <-time.After(time.Second * 2):
			t.Fail("failed to wait response")
		case err := <-errch:
			t.NoError(err)
		}

		t.T().Log("trying to read body, but ResponseHeader returned")
		rbodyType, rbodyLength, rbody, renc, rresh, err := broker.ReadBody(ctx)
		t.NoError(err)

		t.Equal(UnknownBodyType, rbodyType)
		t.Equal(uint64(0), rbodyLength)
		t.Nil(rbody)

		t.True(t.enc.Hint().Equal(renc.Hint()))
		t.False(rresh.OK())
		t.Error(rresh.Err())
		t.ErrorContains(rresh.Err(), "hehehe")
	})
}

func (t *testBrokers) TestServerStopped() {
	name := quicstream.HandlerName(util.UUID().String())

	t.Run("write request head", func() {
		srv, ph, _, brokerf := t.server(name)
		defer srv.StopWait()

		nname := quicstream.HandlerName(util.UUID().String())
		ph.Add(nname, NewHandler(t.encs, func(ctx context.Context, _ net.Addr, _ *HandlerBroker, _ RequestHeader) (context.Context, error) {
			return ctx, nil
		}, nil))

		ctx, cancel := context.WithTimeout(context.Background(), time.Second*1)
		defer cancel()

		broker, closef := brokerf(ctx)
		defer closef()

		t.T().Log("serve gone")
		t.NoError(srv.StopWait())

		reqh := newDummyRequestHeader(nname, util.UUID().String())

		t.T().Log("write request head")
		err := broker.WriteRequestHead(ctx, reqh)
		t.Error(err)
		t.True(errors.Is(err, quicstream.ErrNetwork), "%T %+v", err, err)
	})

	t.Run("write body", func() {
		srv, ph, _, brokerf := t.server(name)
		defer srv.StopWait()

		nname := quicstream.HandlerName(util.UUID().String())
		ph.Add(nname, NewHandler(t.encs, func(ctx context.Context, _ net.Addr, _ *HandlerBroker, _ RequestHeader) (context.Context, error) {
			return ctx, nil
		}, nil))

		ctx, cancel := context.WithTimeout(context.Background(), time.Second*3)
		defer cancel()

		broker, closef := brokerf(ctx)
		defer closef()

		reqh := newDummyRequestHeader(nname, util.UUID().String())

		t.T().Log("write request head")
		err := broker.WriteRequestHead(ctx, reqh)
		t.NoError(err)

		t.T().Log("serve gone")
		t.NoError(srv.StopWait())

		t.T().Log("try to write body")
		body := bytes.NewBuffer(util.UUID().Bytes())
		if err := broker.WriteBody(ctx, FixedLengthBodyType, uint64(body.Len()), body); err != nil {
			t.True(quicstream.IsSeriousError(quicstream.ErrNetwork), "%T %+v", err, err)
		}
	})
}

func (t *testBrokers) TestReadBody() {
	name := quicstream.HandlerName(util.UUID().String())
	srv, ph, errch, brokerf := t.server(name)
	defer srv.StopWait()

	t.Run("read empty body; eof error", func() {
		ph.Add(name, NewHandler(t.encs, func(ctx context.Context, _ net.Addr, broker *HandlerBroker, _ RequestHeader) (context.Context, error) {
			errch <- nil

			return ctx, broker.WriteBody(ctx, EmptyBodyType, 0, nil)
		}, nil))

		ctx, cancel := context.WithTimeout(context.Background(), time.Second*1)
		defer cancel()

		broker, closef := brokerf(ctx)
		defer closef()

		reqh := newDummyRequestHeader(name, util.UUID().String())

		t.T().Log("write request head")
		t.NoError(broker.WriteRequestHead(ctx, reqh))

		select {
		case <-time.After(time.Second * 2):
			t.Fail("failed to wait response")
		case err := <-errch:
			t.NoError(err)
		}

		t.T().Log("read body")
		rbodyType, rbodyLength, rbody, renc, rres, err := broker.ReadBody(ctx)
		t.NoError(err)
		t.Nil(renc)
		t.Nil(rres)

		t.Equal(uint64(0), rbodyLength)
		t.Equal(EmptyBodyType, rbodyType)
		t.Nil(rbody)
	})

	t.Run("stream body", func() {
		bodybytes := util.UUID().Bytes()

		ph.Add(name, NewHandler(t.encs, func(ctx context.Context, _ net.Addr, broker *HandlerBroker, _ RequestHeader) (context.Context, error) {
			errch <- nil

			return ctx, broker.WriteBody(ctx, StreamBodyType, 33, bytes.NewBuffer(bodybytes)) // NOTE intended body length
		}, nil))

		ctx, cancel := context.WithTimeout(context.Background(), time.Second*1)
		defer cancel()

		broker, closef := brokerf(ctx)
		defer closef()

		reqh := newDummyRequestHeader(name, util.UUID().String())

		t.T().Log("write request head")
		t.NoError(broker.WriteRequestHead(ctx, reqh))

		select {
		case <-time.After(time.Second * 2):
			t.Fail("failed to wait response")
		case err := <-errch:
			t.NoError(err)
		}

		t.T().Log("read body")
		rbodyType, rbodyLength, rbody, renc, rres, err := broker.ReadBody(ctx)
		t.NoError(err)
		t.Nil(renc)
		t.Nil(rres)

		t.Equal(uint64(0), rbodyLength)
		t.Equal(StreamBodyType, rbodyType)
		t.NotNil(rbody)

		rb, err := io.ReadAll(rbody)
		t.NoError(err)
		t.Equal(bodybytes, rb)

		_, _, _, _, rres, err = broker.ReadBody(ctx)
		t.Nil(rres)
		t.Error(err)
		t.ErrorContains(err, "insufficient read", "%T %+v", err, err)
	})
}

func TestBrokers(t *testing.T) {
	defer goleak.VerifyNone(t,
		goleak.IgnoreTopFunction("github.com/spikeekips/mitum/util.EnsureRead.func1"),
	)

	suite.Run(t, new(testBrokers))
}

var dummyRequestHeaderHint = hint.MustNewHint("dummy-request-header-v1.2.3")

type dummyRequestHeader struct {
	BaseRequestHeader
	ID string
}

func newDummyRequestHeader(name quicstream.HandlerName, id string) dummyRequestHeader {
	return dummyRequestHeader{
		BaseRequestHeader: NewBaseRequestHeader(dummyRequestHeaderHint, quicstream.HashPrefix(name)),
		ID:                id,
	}
}

func (dummyRequestHeader) QUICStreamHeader() {}

type dummyRequestHeaderJSONMarshaler struct {
	ID string `json:"id"`
}

func (h dummyRequestHeader) MarshalJSON() ([]byte, error) {
	return util.MarshalJSON(struct {
		BaseRequestHeader
		dummyRequestHeaderJSONMarshaler
	}{
		BaseRequestHeader: h.BaseRequestHeader,
		dummyRequestHeaderJSONMarshaler: dummyRequestHeaderJSONMarshaler{
			ID: h.ID,
		},
	})
}

func (h *dummyRequestHeader) UnmarshalJSON(b []byte) error {
	if err := util.UnmarshalJSON(b, &h.BaseRequestHeader); err != nil {
		return err
	}

	var u dummyRequestHeaderJSONMarshaler
	if err := util.UnmarshalJSON(b, &u); err != nil {
		return err
	}

	h.ID = u.ID

	return nil
}
