package quicstreamheader

import (
	"bytes"
	"context"
	"io"
	"net"
	"testing"
	"time"

	"github.com/pkg/errors"
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

	t.encs = encoder.NewEncoders()
	t.enc = jsonenc.NewEncoder()
	t.NoError(t.encs.AddHinter(t.enc))

	t.NoError(t.enc.Add(encoder.DecodeDetail{Hint: DefaultResponseHeaderHint, Instance: DefaultResponseHeader{}}))
	t.NoError(t.enc.Add(encoder.DecodeDetail{Hint: dummyRequestHeaderHint, Instance: dummyRequestHeader{}}))
}

func (t *testBrokers) clientBroker(ctx context.Context, client *quicstream.Client) (*ClientBroker, func() error) {
	r, w, err := client.OpenStream(ctx)
	t.NoError(err)

	broker := NewClientBroker(t.encs, t.enc, r, w)

	return broker, broker.Close
}

func (t *testBrokers) server(prefix [32]byte) (
	*quicstream.Server,
	*quicstream.Client,
	*quicstream.PrefixHandler,
	chan error,
) {
	errch := make(chan error, 1)

	ph := quicstream.NewPrefixHandler(func(_ net.Addr, _ io.Reader, _ io.Writer, err error) error {
		errch <- err

		return nil
	})

	ph.Add(quicstream.HashPrefix(util.UUID().String()), func(addr net.Addr, r io.Reader, w io.Writer) error {
		return errors.Errorf("unknown request")
	})

	srv := t.NewDefaultServer(nil, quicstream.Handler(ph.Handler))

	t.NoError(srv.Start(context.Background()))

	client := t.NewClient(t.Bind)

	return srv, client, ph, errch
}

func (t *testBrokers) TestRequestHeader() {
	prefix := quicstream.HashPrefix(util.UUID().String())
	srv, client, ph, errch := t.server(prefix)
	defer srv.Stop()

	t.Run("request, response", func() {
		var denc encoder.Encoder
		var dreqh RequestHeader

		ph.Add(prefix, NewHandler(t.encs, 0, func(ctx context.Context, _ net.Addr, broker *HandlerBroker, header RequestHeader) error {
			denc = broker.Encoder
			dreqh = header

			errch <- nil

			resh := NewDefaultResponseHeader(true, nil)

			return broker.WriteResponseHead(ctx, resh)
		}, nil))

		ctx, cancel := context.WithTimeout(context.Background(), time.Second*1)
		defer cancel()

		broker, closef := t.clientBroker(ctx, client)
		defer closef()

		reqh := newDummyRequestHeader(prefix, util.UUID().String())

		t.T().Log("write request head")
		t.NoError(broker.WriteRequestHead(ctx, reqh))

		select {
		case <-time.After(time.Second * 2):
			t.NoError(errors.Errorf("failed to wait response"))
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
	})

	t.Run("failed to write", func() {
		ph.Add(prefix, NewHandler(t.encs, 0, func(ctx context.Context, _ net.Addr, broker *HandlerBroker, _ RequestHeader) error {
			return nil
		}, nil))

		ctx, cancel := context.WithTimeout(context.Background(), time.Second*1)
		defer cancel()

		broker, closef := t.clientBroker(ctx, client)

		t.T().Log("close writer")
		closef()

		reqh := newDummyRequestHeader(prefix, util.UUID().String())

		t.T().Log("write request head")
		err := broker.WriteRequestHead(ctx, reqh)

		t.Error(err)
		t.True(errors.Is(err, quicstream.ErrNetwork))
	})
}

func (t *testBrokers) TestRequestHeaderButHandlerError() {
	prefix := quicstream.HashPrefix(util.UUID().String())
	srv, client, ph, errch := t.server(prefix)
	defer srv.Stop()

	t.Run("error handler response", func() {
		errhandlerch := make(chan error, 1)

		ph.Add(prefix, NewHandler(t.encs, 0, func(context.Context, net.Addr, *HandlerBroker, RequestHeader) error {
			return errors.Errorf("hehehe")
		}, func(ctx context.Context, _ net.Addr, _ *HandlerBroker, err error) error {
			errhandlerch <- err

			return nil
		}))

		ctx, cancel := context.WithTimeout(context.Background(), time.Second*1)
		defer cancel()

		broker, closef := t.clientBroker(ctx, client)
		defer closef()

		reqh := newDummyRequestHeader(prefix, util.UUID().String())

		t.T().Log("write request head")
		t.NoError(broker.WriteRequestHead(ctx, reqh))

		select {
		case <-time.After(time.Second * 2):
			t.NoError(errors.Errorf("failed to wait response"))
		case err := <-errch:
			t.Error(err)
			t.ErrorContains(err, "hehehe")
		}

		t.T().Log("error handler")
		select {
		case <-time.After(time.Second * 2):
			t.NoError(errors.Errorf("failed to wait error handler"))
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
		prefix := quicstream.HashPrefix(util.UUID().String())
		ph.Add(prefix, NewHandler(t.encs, 0, func(ctx context.Context, _ net.Addr, broker *HandlerBroker, _ RequestHeader) error {
			resh := NewDefaultResponseHeader(false, errors.Errorf("hehehe"))

			defer func() {
				errch <- nil
			}()

			return broker.WriteResponseHead(ctx, resh)
		}, nil))

		ctx, cancel := context.WithTimeout(context.Background(), time.Second*1)
		defer cancel()

		broker, closef := t.clientBroker(ctx, client)
		defer closef()

		reqh := newDummyRequestHeader(prefix, util.UUID().String())

		t.T().Log("write request head")
		t.NoError(broker.WriteRequestHead(ctx, reqh))

		select {
		case <-time.After(time.Second * 2):
			t.NoError(errors.Errorf("failed to wait response"))
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
	prefix := quicstream.HashPrefix(util.UUID().String())

	t.Run("write request head", func() {
		srv, client, ph, _ := t.server(prefix)
		defer srv.Stop()

		prefix := quicstream.HashPrefix(util.UUID().String())
		ph.Add(prefix, NewHandler(t.encs, 0, func(context.Context, net.Addr, *HandlerBroker, RequestHeader) error {
			return nil
		}, nil))

		ctx, cancel := context.WithTimeout(context.Background(), time.Second*1)
		defer cancel()

		broker, closef := t.clientBroker(ctx, client)
		defer closef()

		t.T().Log("serve gone")
		t.NoError(srv.StopWait())

		reqh := newDummyRequestHeader(prefix, util.UUID().String())

		t.T().Log("write request head")
		err := broker.WriteRequestHead(ctx, reqh)
		t.Error(err)
		t.True(errors.Is(err, quicstream.ErrNetwork), "%T %+v", err, err)
	})

	t.Run("write body", func() {
		srv, client, ph, _ := t.server(prefix)
		defer srv.Stop()

		prefix := quicstream.HashPrefix(util.UUID().String())
		ph.Add(prefix, NewHandler(t.encs, 0, func(context.Context, net.Addr, *HandlerBroker, RequestHeader) error {
			return nil
		}, nil))

		ctx, cancel := context.WithTimeout(context.Background(), time.Second*1)
		defer cancel()

		broker, closef := t.clientBroker(ctx, client)
		defer closef()

		reqh := newDummyRequestHeader(prefix, util.UUID().String())

		t.T().Log("write request head")
		err := broker.WriteRequestHead(ctx, reqh)
		t.NoError(err)

		t.T().Log("serve gone")
		t.NoError(srv.StopWait())

		t.T().Log("try to write body")
		body := bytes.NewBuffer(util.UUID().Bytes())
		err = broker.WriteBody(ctx, FixedLengthBodyType, uint64(body.Len()), body)
		t.Error(err)
		t.True(quicstream.IsNetworkError(quicstream.ErrNetwork), "%T %+v", err, err)
	})
}

func (t *testBrokers) TestReadBody() {
	prefix := quicstream.HashPrefix(util.UUID().String())
	srv, client, ph, errch := t.server(prefix)
	defer srv.Stop()

	t.Run("read empty body; eof error", func() {
		ph.Add(prefix, NewHandler(t.encs, 0, func(ctx context.Context, _ net.Addr, broker *HandlerBroker, _ RequestHeader) error {
			errch <- nil

			return broker.WriteBody(ctx, EmptyBodyType, 0, nil)
		}, nil))

		ctx, cancel := context.WithTimeout(context.Background(), time.Second*1)
		defer cancel()

		broker, closef := t.clientBroker(ctx, client)
		defer closef()

		reqh := newDummyRequestHeader(prefix, util.UUID().String())

		t.T().Log("write request head")
		t.NoError(broker.WriteRequestHead(ctx, reqh))

		select {
		case <-time.After(time.Second * 2):
			t.NoError(errors.Errorf("failed to wait response"))
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

		ph.Add(prefix, NewHandler(t.encs, 0, func(ctx context.Context, _ net.Addr, broker *HandlerBroker, _ RequestHeader) error {
			errch <- nil

			return broker.WriteBody(ctx, StreamBodyType, 33, bytes.NewBuffer(bodybytes)) // NOTE intended body length
		}, nil))

		ctx, cancel := context.WithTimeout(context.Background(), time.Second*1)
		defer cancel()

		broker, closef := t.clientBroker(ctx, client)
		defer closef()

		reqh := newDummyRequestHeader(prefix, util.UUID().String())

		t.T().Log("write request head")
		t.NoError(broker.WriteRequestHead(ctx, reqh))

		select {
		case <-time.After(time.Second * 2):
			t.NoError(errors.Errorf("failed to wait response"))
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
		t.True(errors.Is(err, io.EOF), "%T %+v", err, err)
		t.True(errors.Is(err, quicstream.ErrNetwork), "%T %+v", err, err)
	})
}

var dummyRequestHeaderHint = hint.MustNewHint("dummy-request-header-v1.2.3")

type dummyRequestHeader struct {
	BaseRequestHeader
	ID string
}

func newDummyRequestHeader(prefix [32]byte, id string) dummyRequestHeader {
	return dummyRequestHeader{
		BaseRequestHeader: NewBaseRequestHeader(dummyRequestHeaderHint, prefix),
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

func TestBrokers(t *testing.T) {
	defer goleak.VerifyNone(t)

	suite.Run(t, new(testBrokers))
}
