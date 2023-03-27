package quicstream

import (
	"bytes"
	"context"
	"io"
	"net"
	"testing"
	"time"

	"github.com/pkg/errors"
	"github.com/quic-go/quic-go"
	"github.com/spikeekips/mitum/base"
	"github.com/spikeekips/mitum/util"
	"github.com/spikeekips/mitum/util/encoder"
	jsonenc "github.com/spikeekips/mitum/util/encoder/json"
	"github.com/spikeekips/mitum/util/hint"
	"github.com/stretchr/testify/suite"
	"go.uber.org/goleak"
)

var (
	dummyRequestHeaderHint  = hint.MustNewHint("dummy-request-header-v1.2.3")
	dummyResponseHeaderHint = hint.MustNewHint("dummy-response-header-v1.2.3")
)

type dummyRequestHeader struct {
	BaseRequestHeader
	ID string
}

func newDummyRequestHeader(prefix []byte, id string) dummyRequestHeader {
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

type dummyResponseHeader struct {
	BaseResponseHeader
	ID string
}

func newDummyResponseHeader(ok bool, err error, id string) dummyResponseHeader {
	return dummyResponseHeader{
		BaseResponseHeader: NewBaseResponseHeader(dummyResponseHeaderHint, ok, err),
		ID:                 id,
	}
}

func (dummyResponseHeader) QUICStreamHeader() {}

type dummyResponseHeaderJSONMarshaler struct {
	ID string `json:"id"`
}

func (h dummyResponseHeader) MarshalJSON() ([]byte, error) {
	return util.MarshalJSON(struct {
		BaseResponseHeaderJSONMarshaler
		dummyResponseHeaderJSONMarshaler
	}{
		BaseResponseHeaderJSONMarshaler: h.BaseResponseHeader.JSONMarshaler(),
		dummyResponseHeaderJSONMarshaler: dummyResponseHeaderJSONMarshaler{
			ID: h.ID,
		},
	})
}

func (h *dummyResponseHeader) UnmarshalJSON(b []byte) error {
	if err := util.UnmarshalJSON(b, &h.BaseResponseHeader); err != nil {
		return err
	}

	var u dummyResponseHeaderJSONMarshaler
	if err := util.UnmarshalJSON(b, &u); err != nil {
		return err
	}

	h.ID = u.ID

	return nil
}

type testHeaderClient struct {
	BaseTest
	encs *encoder.Encoders
	enc  encoder.Encoder
}

func (t *testHeaderClient) SetupSuite() {
	t.BaseTest.SetupSuite()

	t.encs = encoder.NewEncoders()
	t.enc = jsonenc.NewEncoder()
	t.NoError(t.encs.AddHinter(t.enc))

	t.NoError(t.enc.Add(encoder.DecodeDetail{Hint: base.StringAddressHint, Instance: base.StringAddress{}}))
	t.NoError(t.enc.Add(encoder.DecodeDetail{Hint: dummyRequestHeaderHint, Instance: dummyRequestHeader{}}))
	t.NoError(t.enc.Add(encoder.DecodeDetail{Hint: dummyResponseHeaderHint, Instance: dummyResponseHeader{}}))
}

func (t *testHeaderClient) newServer(prefix []byte, handler Handler) *Server {
	ph := NewPrefixHandler(nil)
	ph.Add(prefix, handler)

	srv := t.NewDefaultServer(nil, ph.Handler)

	t.NoError(srv.Start(context.Background()))

	return srv
}

func (t *testHeaderClient) openstreamf() OpenStreamFunc {
	return func(ctx context.Context, ci UDPConnInfo) (io.ReadCloser, io.WriteCloser, error) {
		client := t.NewClient(ci.UDPAddr())

		return client.OpenStream(ctx)
	}
}

func (t *testHeaderClient) skipEOF(err error) error {
	if err == nil {
		return nil
	}

	if errors.Is(err, io.EOF) {
		return nil
	}

	return err
}

func (t *testHeaderClient) logRequestHeadDetail(detail RequestHeadDetail) {
	t.logHead("request detail",
		detail.Header,
		detail.Encoder.Hint(),
	)
}

func (t *testHeaderClient) logHead(
	prefix string,
	h Header,
	enchint hint.Hint,
) {
	t.T().Logf("%s header: %v", prefix, util.MustMarshalJSONIndentString(h))
	t.T().Logf("%s header type: %T", prefix, h)
	t.T().Logf("%s encoder: %v", prefix, enchint)
}

func (t *testHeaderClient) logBody(
	prefix string,
	dataFormat HeaderDataFormat,
	bodyLength uint64,
	body io.Reader,
) {
	t.T().Logf("%s data format: %v", prefix, dataFormat)
	t.T().Logf("%s body length: %v", prefix, bodyLength)
	t.T().Logf("%s body: %v", prefix, body != nil)
}

func (t *testHeaderClient) TestRequest() {
	localci := NewUDPConnInfo(t.Bind, true)

	prefix := HashPrefix(util.UUID().String())
	id := util.UUID().String()

	var delay *time.Duration

	gctx, gcancel := context.WithCancel(context.Background())
	defer gcancel()

	handler := NewHeaderHandler(
		t.encs,
		time.Minute,
		func(_ context.Context, _ net.Addr, r io.Reader, w io.Writer, detail RequestHeadDetail) error {
			if delay != nil {
				t.T().Log("delaying:", delay)

				select {
				case <-gctx.Done():
					return nil
				case <-time.After(*delay):
				}
			}

			t.logRequestHeadDetail(detail)

			dh, ok := detail.Header.(dummyRequestHeader)
			t.True(ok)

			dataFormat, bodyLength, body, err := HeaderReadBody(r)
			if err != nil {
				return err
			}

			t.logBody("request", dataFormat, bodyLength, body)

			b, err := io.ReadAll(body)
			t.NoError(err)
			t.T().Log("request body string:", string(b))

			header := newDummyResponseHeader(true, nil, id)
			buf := bytes.NewBuffer([]byte(dh.ID))

			return WriteResponse(w, detail.Encoder, header, LengthedDataFormat, uint64(buf.Len()), buf)
		},
	)

	t.Run("ok", func() {
		srv := t.newServer(prefix, handler)
		defer srv.Stop()

		c := NewHeaderClient(t.encs, t.enc, t.openstreamf())

		broker, err := c.Broker(context.Background(), localci)
		t.NoError(err)
		defer broker.Close()

		header := newDummyRequestHeader(prefix, id)
		t.NoError(broker.WriteRequestHead(header))

		t.NoError(broker.WriteBody(EmptyDataFormat, 0, nil))

		renc, rh, err := broker.ReadResponseHead()
		t.NoError(err)

		t.logHead("response", rh, renc.Hint())

		t.NoError(rh.Err())
		t.True(rh.OK())

		rdh, ok := rh.(dummyResponseHeader)
		t.True(ok)
		t.Equal(id, rdh.ID)

		dataFormat, bodyLength, rbody, err := broker.ReadBody()
		t.NoError(err)

		t.logBody("response", dataFormat, bodyLength, rbody)

		b, err := io.ReadAll(rbody)
		t.NoError(t.skipEOF(err))
		t.T().Log("response body:", string(b))

		t.Equal(id, string(b))
	})

	t.Run("empty request header", func() {
		srv := t.newServer(prefix, handler)
		defer srv.Stop()

		c := NewHeaderClient(t.encs, t.enc, t.openstreamf())

		broker, err := c.Broker(context.Background(), localci)
		t.NoError(err)
		defer broker.Close()

		err = broker.WriteRequestHead(nil)
		t.ErrorContains(err, "empty header")
	})

	t.Run("empty response header", func() {
		handler := NewHeaderHandler(
			t.encs,
			time.Second,
			func(_ context.Context, _ net.Addr, r io.Reader, w io.Writer, detail RequestHeadDetail) error {
				t.logRequestHeadDetail(detail)

				dh, ok := detail.Header.(dummyRequestHeader)
				t.True(ok)

				dataFormat, bodyLength, body, err := HeaderReadBody(r)
				t.NoError(err)
				t.logBody("request", dataFormat, bodyLength, body)

				b, err := io.ReadAll(body)
				t.NoError(err)
				t.T().Log("request body:", string(b))

				if err := HeaderWriteHead(w, detail.Encoder, nil); err != nil {
					return err
				}

				buf := bytes.NewBuffer([]byte(dh.ID))

				return HeaderWriteBody(w, LengthedDataFormat, uint64(buf.Len()), buf)
			},
		)

		srv := t.newServer(prefix, handler)
		defer srv.Stop()

		c := NewHeaderClient(t.encs, t.enc, t.openstreamf())

		broker, err := c.Broker(context.Background(), localci)
		t.NoError(err)

		defer broker.Close()

		header := newDummyRequestHeader(prefix, id)
		t.NoError(broker.WriteRequestHead(header))

		t.NoError(broker.WriteBody(EmptyDataFormat, 0, nil))

		renc, rh, err := broker.ReadResponseHead()
		t.NoError(err)

		t.logHead("response", rh, renc.Hint())
		t.Nil(rh)

		dataFormat, bodyLength, rbody, err := broker.ReadBody()
		t.NoError(err)

		t.logBody("response", dataFormat, bodyLength, rbody)

		b, err := io.ReadAll(rbody)
		t.NoError(t.skipEOF(err))
		t.T().Log("response body:", string(b))

		t.Equal(id, string(b))
	})

	t.Run("empty body", func() {
		delay = nil

		srv := t.newServer(prefix, handler)
		defer srv.Stop()

		c := NewHeaderClient(t.encs, t.enc, t.openstreamf())

		broker, err := c.Broker(context.Background(), localci)
		t.NoError(err)
		defer broker.Close()

		header := newDummyRequestHeader(prefix, id)
		t.NoError(broker.WriteRequestHead(header))

		t.NoError(broker.WriteBody(EmptyDataFormat, 0, nil))

		renc, rh, err := broker.ReadResponseHead()
		t.NoError(err)
		t.NotNil(renc)

		t.logHead("response", rh, hint.Hint{})

		t.NoError(rh.Err())
		t.True(rh.OK())

		rdh, ok := rh.(dummyResponseHeader)
		t.True(ok)
		t.Equal(id, rdh.ID)

		dataFormat, bodyLength, rbody, err := broker.ReadBody()
		t.NoError(err)

		t.logBody("response", dataFormat, bodyLength, rbody)

		b, err := io.ReadAll(rbody)
		t.NoError(t.skipEOF(err))
		t.T().Log("body:", string(b))

		t.Equal(id, string(b))
	})

	t.Run("timeout", func() {
		d := time.Second * 33

		delay = &d

		srv := t.newServer(prefix, handler)
		defer srv.Stop()

		c := NewHeaderClient(t.encs, t.enc, t.openstreamf())

		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()

		broker, err := c.Broker(ctx, localci)
		t.NoError(err)

		header := newDummyRequestHeader(prefix, id)
		t.NoError(broker.WriteRequestHead(header))

		{
			buf := bytes.NewBuffer(util.UUID().Bytes())
			t.NoError(broker.WriteBody(LengthedDataFormat, uint64(buf.Len()), buf))
		}

		renc, rh, err := broker.ReadResponseHead()
		t.Error(err)

		t.logHead("response", rh, hint.Hint{})
		t.Nil(rh)
		t.Nil(renc)

		t.True(errors.Is(err, &quic.StreamError{}), "%+v %T", err, err)
	})
}

func TestHeaderClient(t *testing.T) {
	defer goleak.VerifyNone(t)

	suite.Run(t, new(testHeaderClient))
}
