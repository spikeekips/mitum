package quicstream

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net"
	"testing"
	"time"

	"github.com/pkg/errors"
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

func newDummyRequestHeader(prefix, id string) dummyRequestHeader {
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

func (t *testHeaderClient) newServer(prefix string, handler Handler) *Server {
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

func (t *testHeaderClient) printHeaderHandlerDetail(detail HeaderHandlerDetail) {
	t.T().Log("request header:", detail.Header)
	t.T().Logf("request header type: %T", detail.Header)
	t.T().Log("request encoder:", detail.Encoder.Hint())
	t.T().Log("request body:", detail.Body)
}

func (t *testHeaderClient) printResponseHeader(
	h ResponseHeader,
	enchint hint.Hint,
	body io.Reader,
) {
	t.T().Log("response header:", util.MustMarshalJSONIndentString(h))
	t.T().Logf("response header type: %T", h)
	t.T().Log("response encoder:", enchint)
	t.T().Log("response body:", body)
}

func (t *testHeaderClient) TestRequest() {
	localci := NewUDPConnInfo(t.Bind, true)

	prefix := util.UUID().String()
	id := util.UUID().String()

	var delay *time.Duration

	gctx, gcancel := context.WithCancel(context.Background())
	defer gcancel()

	handler := NewHeaderHandler(
		t.encs,
		time.Minute,
		func(_ net.Addr, r io.Reader, w io.Writer, detail HeaderHandlerDetail) error {
			if delay != nil {
				t.T().Log("delay:", delay)

				select {
				case <-gctx.Done():
					return nil
				case <-time.After(*delay):
				}
			}

			t.printHeaderHandlerDetail(detail)

			dh, ok := detail.Header.(dummyRequestHeader)
			t.True(ok)

			b, err := io.ReadAll(detail.Body)
			t.NoError(err)
			t.T().Log("request body:", string(b))

			header := newDummyResponseHeader(true, nil, id)

			if err := WriteHead(w, detail.Encoder, header); err != nil {
				return err
			}

			buf := bytes.NewBuffer([]byte(dh.ID))

			return WriteBody(w, LengthedDataFormat, uint64(buf.Len()), buf)
		},
	)

	t.Run("ok", func() {
		srv := t.newServer(prefix, handler)
		defer srv.Stop()

		c := NewHeaderClient(t.encs, t.enc, t.openstreamf())

		header := newDummyRequestHeader(prefix, id)

		rh, renc, rbody, r, w, err := c.Request(context.Background(), localci, header,
			func() (DataFormat, io.Reader, uint64, error) {
				return LengthedDataFormat, nil, 0, nil
			},
		)
		t.NoError(err)
		defer r.Close()
		defer w.Close()

		t.printResponseHeader(rh, renc.Hint(), rbody)

		t.NoError(rh.Err())
		t.True(rh.OK())

		rdh, ok := rh.(dummyResponseHeader)
		t.True(ok)
		t.Equal(id, rdh.ID)

		b, err := io.ReadAll(rbody)
		t.NoError(t.skipEOF(err))
		t.T().Log("response body:", string(b))

		t.Equal(id, string(b))
	})

	t.Run("empty request header", func() {
		srv := t.newServer(prefix, handler)
		defer srv.Stop()

		c := NewHeaderClient(t.encs, t.enc, t.openstreamf())

		_, _, _, _, _, err := c.Request(context.Background(), localci, nil, nil)
		t.Error(err)
		t.ErrorContains(err, "empty header")
	})

	t.Run("empty response header", func() {
		handler := NewHeaderHandler(
			t.encs,
			time.Second,
			func(_ net.Addr, r io.Reader, w io.Writer, detail HeaderHandlerDetail) error {
				t.printHeaderHandlerDetail(detail)

				dh, ok := detail.Header.(dummyRequestHeader)
				t.True(ok)

				b, err := io.ReadAll(detail.Body)
				t.NoError(err)
				t.T().Log("request body:", string(b))

				if err := WriteHead(w, detail.Encoder, nil); err != nil {
					return err
				}

				buf := bytes.NewBuffer([]byte(dh.ID))

				return WriteBody(w, LengthedDataFormat, uint64(buf.Len()), buf)
			},
		)

		srv := t.newServer(prefix, handler)
		defer srv.Stop()

		c := NewHeaderClient(t.encs, t.enc, t.openstreamf())

		header := newDummyRequestHeader(prefix, id)
		rh, renc, rbody, r, w, err := c.Request(context.Background(), localci, header,
			func() (DataFormat, io.Reader, uint64, error) {
				return LengthedDataFormat, nil, 0, nil
			},
		)
		t.NoError(err)

		defer r.Close()
		t.NoError(w.Close())

		t.printResponseHeader(rh, renc.Hint(), rbody)

		t.Nil(rh)

		b, err := io.ReadAll(r)
		t.NoError(t.skipEOF(err))
		t.T().Log("response body:", string(b))

		t.Equal(id, string(b))
	})

	t.Run("empty body", func() {
		delay = nil

		srv := t.newServer(prefix, handler)
		defer srv.Stop()

		c := NewHeaderClient(t.encs, t.enc, t.openstreamf())

		header := newDummyRequestHeader(prefix, id)

		rh, renc, rbody, r, w, err := c.Request(context.Background(), localci, header,
			func() (DataFormat, io.Reader, uint64, error) {
				return LengthedDataFormat, nil, 0, nil
			},
		)
		t.NoError(err)

		defer r.Close()
		defer w.Close()

		t.NotNil(renc)

		t.printResponseHeader(rh, hint.Hint{}, rbody)

		t.NoError(rh.Err())
		t.True(rh.OK())

		rdh, ok := rh.(dummyResponseHeader)
		t.True(ok)
		t.Equal(id, rdh.ID)

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

		header := newDummyRequestHeader(prefix, id)

		ctx, cancel := context.WithTimeout(context.Background(), time.Millisecond*333)
		defer cancel()

		rh, renc, rbody, _, _, err := c.Request(ctx, localci, header,
			func() (DataFormat, io.Reader, uint64, error) {
				buf := bytes.NewBuffer(util.UUID().Bytes())

				return LengthedDataFormat, buf, uint64(buf.Len()), nil
			},
		)
		t.Error(err)
		t.Nil(rh)
		t.Nil(renc)
		t.Nil(rbody)

		t.True(errors.Is(err, context.DeadlineExceeded), "%+v %T", err, err)

		t.printResponseHeader(rh, hint.Hint{}, rbody)
	})
}

func (t *testHeaderClient) TestRequestAndDecode() {
	localci := NewUDPConnInfo(t.Bind, true)

	prefix := util.UUID().String()
	id := util.UUID().String()

	address := base.RandomAddress("")

	handler := NewHeaderHandler(
		t.encs,
		time.Second,
		func(_ net.Addr, r io.Reader, w io.Writer, detail HeaderHandlerDetail) error {
			t.printHeaderHandlerDetail(detail)

			_, ok := detail.Header.(dummyRequestHeader)
			t.True(ok)

			{
				b, err := io.ReadAll(detail.Body)
				t.NoError(err)
				t.T().Log("request body:", string(b))
			}

			b, err := detail.Encoder.Marshal(address)
			t.NoError(err)

			header := newDummyResponseHeader(true, nil, id)

			if err := WriteHead(w, detail.Encoder, header); err != nil {
				return err
			}

			buf := bytes.NewBuffer(b)

			return WriteBody(w, LengthedDataFormat, uint64(buf.Len()), buf)
		},
	)

	t.Run("ok", func() {
		srv := t.newServer(prefix, handler)
		defer srv.Stop()

		c := NewHeaderClient(t.encs, t.enc, t.openstreamf())

		header := newDummyRequestHeader(prefix, id)

		rh, renc, rbody, r, w, err := HeaderRequestEncode(context.Background(), c, localci, header, address)
		t.NoError(err)
		defer r.Close()
		defer w.Close()

		t.printResponseHeader(rh, renc.Hint(), rbody)

		t.NoError(rh.Err())
		t.True(rh.OK())

		rdh, ok := rh.(dummyResponseHeader)
		t.True(ok)
		t.Equal(id, rdh.ID)

		b, err := io.ReadAll(r)
		t.NoError(t.skipEOF(err))
		t.T().Log("body:", string(b))

		var s string
		t.NoError(renc.Unmarshal(b, &s))
		raddress, err := base.DecodeAddress(s, t.enc)
		t.NoError(err)
		t.T().Log("response address:", raddress)

		t.True(address.Equal(raddress))
	})
}

func (t *testHeaderClient) TestRequestDecode() {
	localci := NewUDPConnInfo(t.Bind, true)

	prefix := util.UUID().String()
	id := util.UUID().String()

	address := base.RandomAddress("showme")

	handler := NewHeaderHandler(
		t.encs,
		time.Second,
		func(_ net.Addr, r io.Reader, w io.Writer, detail HeaderHandlerDetail) error {
			t.printHeaderHandlerDetail(detail)

			_, ok := detail.Header.(dummyRequestHeader)
			t.True(ok)

			var body json.RawMessage
			{
				b, err := io.ReadAll(detail.Body)
				t.NoError(err)
				t.T().Logf("request body: %q", string(b))

				t.NoError(detail.Encoder.Unmarshal(b, &body))
			}

			header := newDummyResponseHeader(true, nil, id)

			if err := WriteHead(w, detail.Encoder, header); err != nil {
				return err
			}

			buf := bytes.NewBuffer(body)

			return WriteBody(w, LengthedDataFormat, uint64(buf.Len()), buf)
		},
	)

	srv := t.newServer(prefix, handler)
	defer srv.Stop()

	t.Run("ok", func() {
		c := NewHeaderClient(t.encs, t.enc, t.openstreamf())

		header := newDummyRequestHeader(prefix, id)

		var u base.Address

		rh, renc, r, w, err := HeaderRequestDecode(
			context.Background(), c, localci, header, address, &u,
			func(enc encoder.Encoder, r io.Reader, u interface{}) error {
				b, err := io.ReadAll(r)
				if err != nil {
					return err
				}

				var s string
				if err := enc.Unmarshal(b, &s); err != nil {
					return err
				}

				i, err := base.DecodeAddress(s, enc)
				if err != nil {
					return err
				}

				return util.InterfaceSetValue(i, u)
			},
		)
		t.NoError(err)
		defer r.Close()
		defer w.Close()

		t.printResponseHeader(rh, renc.Hint(), nil)

		t.NoError(rh.Err())
		t.True(rh.OK())
		t.NotNil(u)

		rdh, ok := rh.(dummyResponseHeader)
		t.True(ok)
		t.Equal(id, rdh.ID)

		t.T().Log("response address:", u)

		t.True(address.Equal(u))
	})

	t.Run("nil", func() {
		c := NewHeaderClient(t.encs, t.enc, t.openstreamf())

		header := newDummyRequestHeader(prefix, id)

		var u base.Address

		rh, renc, r, w, err := HeaderRequestDecode(context.Background(), c, localci, header, nil, &u,
			func(enc encoder.Encoder, r io.Reader, u interface{}) error {
				b, err := io.ReadAll(r)
				if err != nil {
					return err
				}

				if len(b) < 1 {
					return nil
				}

				var s string
				if err := enc.Unmarshal(b, &s); err != nil {
					return err
				}

				i, err := base.DecodeAddress(s, enc)
				if err != nil {
					return err
				}

				return util.InterfaceSetValue(i, u)
			},
		)
		t.NoError(err)
		defer r.Close()
		defer w.Close()

		t.printResponseHeader(rh, renc.Hint(), nil)

		t.NoError(rh.Err())
		t.True(rh.OK())
		t.Nil(u)

		rdh, ok := rh.(dummyResponseHeader)
		t.True(ok)
		t.Equal(id, rdh.ID)

		t.Nil(u)
		t.T().Log("response address:", u)
	})
}

func TestHeaderClient(t *testing.T) {
	defer goleak.VerifyNone(t)

	suite.Run(t, new(testHeaderClient))
}
