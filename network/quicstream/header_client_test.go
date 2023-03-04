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
	dummyHeaderHint         = hint.MustNewHint("dummy-header-v1.2.3")
	dummyResponseHeaderHint = hint.MustNewHint("dummy-response-header-v1.2.3")
	dummyNodeHint           = hint.MustNewHint("dummy-node-v1.2.3")
)

type dummyHeader struct {
	BaseHeader
	ID string
}

func newDummyHeader(prefix, id string) dummyHeader {
	return dummyHeader{
		BaseHeader: NewBaseHeader(dummyHeaderHint, prefix),
		ID:         id,
	}
}

type dummyHeaderJSONMarshaler struct {
	ID string `json:"id"`
}

func (h dummyHeader) MarshalJSON() ([]byte, error) {
	return util.MarshalJSON(struct {
		BaseHeaderJSONMarshaler
		dummyHeaderJSONMarshaler
	}{
		BaseHeaderJSONMarshaler: h.BaseHeader.JSONMarshaler(),
		dummyHeaderJSONMarshaler: dummyHeaderJSONMarshaler{
			ID: h.ID,
		},
	})
}

func (h *dummyHeader) UnmarshalJSON(b []byte) error {
	if err := util.UnmarshalJSON(b, &h.BaseHeader); err != nil {
		return err
	}

	var u dummyHeaderJSONMarshaler
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

func newDummyResponseHeader(ok bool, err error, contentType ContentType, id string) dummyResponseHeader {
	return dummyResponseHeader{
		BaseResponseHeader: NewBaseResponseHeader(dummyResponseHeaderHint, ok, err, contentType),
		ID:                 id,
	}
}

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
	suite.Suite
	encs *encoder.Encoders
	enc  encoder.Encoder
}

func (t *testHeaderClient) SetupSuite() {
	t.encs = encoder.NewEncoders()
	t.enc = jsonenc.NewEncoder()
	t.NoError(t.encs.AddHinter(t.enc))

	t.NoError(t.enc.Add(encoder.DecodeDetail{Hint: base.StringAddressHint, Instance: base.StringAddress{}}))
	t.NoError(t.enc.Add(encoder.DecodeDetail{Hint: base.MPublickeyHint, Instance: base.MPublickey{}}))
	t.NoError(t.enc.Add(encoder.DecodeDetail{Hint: dummyHeaderHint, Instance: dummyHeader{}}))
	t.NoError(t.enc.Add(encoder.DecodeDetail{Hint: dummyResponseHeaderHint, Instance: dummyResponseHeader{}}))
	t.NoError(t.enc.Add(encoder.DecodeDetail{Hint: dummyNodeHint, Instance: base.BaseNode{}}))
}

func (t *testHeaderClient) writef(prefix string, handler Handler) HeaderClientWriteFunc {
	ph := NewPrefixHandler(nil)
	ph.Add(prefix, handler)

	return func(ctx context.Context, ci UDPConnInfo, f ClientWriteFunc) (io.ReadCloser, func() error, error) {
		r := bytes.NewBuffer(nil)
		if err := f(r); err != nil {
			return nil, nil, errors.WithStack(err)
		}

		w := bytes.NewBuffer(nil)

		return io.NopCloser(w), func() error { return nil }, ph.Handler(nil, r, w)
	}
}

func (t *testHeaderClient) TestRequest() {
	localci := NewUDPConnInfo(nil, true)

	prefix := util.UUID().String()
	id := util.UUID().String()

	var delay *time.Duration

	handler := NewHeaderHandler(
		t.encs,
		time.Second,
		func(_ net.Addr, r io.Reader, w io.Writer, h Header, encs *encoder.Encoders, enc encoder.Encoder) error {
			if delay != nil {
				t.T().Log("delay:", delay)

				<-time.After(*delay)
			}

			t.T().Log("request header:", util.MustMarshalJSONIndentString(h))
			t.T().Logf("request header type: %T", h)
			t.T().Log("request encoder:", enc.Hint())

			t.IsType(dummyHeader{}, h)

			dh, ok := h.(dummyHeader)
			t.True(ok)

			b, err := io.ReadAll(r)
			t.NoError(err)
			t.T().Log("body:", string(b))

			buf := bytes.NewBuffer([]byte(dh.ID))
			defer buf.Reset()

			return WriteResponseBody(w, newDummyResponseHeader(true, nil, RawContentType, id), t.enc, buf)
		},
	)

	t.Run("ok", func() {
		ph := NewPrefixHandler(nil)
		ph.Add(prefix, handler)

		c := NewHeaderClient(t.encs, t.enc, t.writef(prefix, handler))

		header := newDummyHeader(prefix, id)
		rh, r, _, renc, err := c.RequestEncode(context.Background(), localci, header, nil)
		t.NoError(err)

		t.T().Log("response header:", util.MustMarshalJSONIndentString(rh))
		t.T().Logf("response header type: %T", rh)
		t.T().Log("response encoder:", renc.Hint())

		t.NoError(rh.Err())
		t.True(rh.OK())

		rdh, ok := rh.(dummyResponseHeader)
		t.True(ok)
		t.Equal(id, rdh.ID)

		b, err := io.ReadAll(r)
		t.NoError(err)
		t.T().Log("body:", string(b))

		t.Equal(id, string(b))
	})

	t.Run("empty request header", func() {
		c := NewHeaderClient(t.encs, t.enc, t.writef(prefix, handler))

		_, _, _, _, err := c.RequestEncode(context.Background(), localci, nil, nil)
		t.Error(err)
		t.ErrorContains(err, "empty header")
	})

	t.Run("empty response header", func() {
		handler := NewHeaderHandler(
			t.encs,
			time.Second,
			func(_ net.Addr, r io.Reader, w io.Writer, h Header, encs *encoder.Encoders, enc encoder.Encoder) error {
				t.T().Log("request header:", util.MustMarshalJSONIndentString(h))
				t.T().Logf("request header type: %T", h)
				t.T().Log("request encoder:", enc.Hint())

				t.IsType(dummyHeader{}, h)

				dh, ok := h.(dummyHeader)
				t.True(ok)

				b, err := io.ReadAll(r)
				t.NoError(err)
				t.T().Log("body:", string(b))

				buf := bytes.NewBuffer([]byte(dh.ID))
				defer buf.Reset()

				return WriteResponseBody(w, nil, t.enc, buf) // empty response header
			},
		)

		c := NewHeaderClient(t.encs, t.enc, t.writef(prefix, handler))

		header := newDummyHeader(prefix, id)
		rh, r, _, renc, err := c.RequestEncode(context.Background(), localci, header, nil)
		t.NoError(err)

		t.T().Log("response header:", util.MustMarshalJSONIndentString(rh))
		t.T().Logf("response header type: %T", rh)
		t.Nil(renc)
		t.Nil(rh)

		b, err := io.ReadAll(r)
		t.NoError(err)
		t.T().Log("body:", string(b))

		t.Equal(id, string(b))
	})

	t.Run("timeout", func() {
		d := time.Second * 2

		delay = &d

		c := NewHeaderClient(t.encs, t.enc, t.writef(prefix, handler))

		header := newDummyHeader(prefix, id)

		ctx, cancel := context.WithTimeout(context.Background(), time.Millisecond*100)
		defer cancel()

		rh, r, _, renc, err := c.RequestEncode(ctx, localci, header, nil)
		t.Error(err)
		t.True(errors.Is(err, context.DeadlineExceeded))
		t.Nil(rh)
		t.Nil(r)
		t.Nil(renc)

		t.T().Log("response header:", util.MustMarshalJSONIndentString(rh))
		t.T().Logf("response header type: %T", rh)

		<-time.After(d * 2)
	})
}

func (t *testHeaderClient) TestRequestBody() {
	localci := NewUDPConnInfo(nil, true)

	prefix := util.UUID().String()
	id := util.UUID().String()

	handler := NewHeaderHandler(
		t.encs,
		time.Second,
		func(_ net.Addr, r io.Reader, w io.Writer, h Header, encs *encoder.Encoders, enc encoder.Encoder) error {
			t.T().Log("request header:", util.MustMarshalJSONIndentString(h))
			t.T().Logf("request header type: %T", h)
			t.T().Log("request encoder:", enc.Hint())

			t.IsType(dummyHeader{}, h)

			_, ok := h.(dummyHeader)
			t.True(ok)

			b, err := io.ReadAll(r)
			t.NoError(err)
			t.T().Log("body:", string(b))

			buf := bytes.NewBuffer(b)
			defer buf.Reset()

			return WriteResponseBody(w, newDummyResponseHeader(true, nil, HinterContentType, id), t.enc, buf)
		},
	)

	t.Run("ok", func() {
		c := NewHeaderClient(t.encs, t.enc, t.writef(prefix, handler))

		header := newDummyHeader(prefix, id)

		buf := bytes.NewBuffer([]byte(id))
		defer buf.Reset()

		rh, r, _, renc, err := c.RequestBody(context.Background(), localci, header, buf)
		t.NoError(err)

		t.T().Log("response header:", util.MustMarshalJSONIndentString(rh))
		t.T().Logf("response header type: %T", rh)
		t.T().Log("response encoder:", renc.Hint())

		t.NoError(rh.Err())
		t.True(rh.OK())

		rdh, ok := rh.(dummyResponseHeader)
		t.True(ok)
		t.Equal(id, rdh.ID)

		b, err := io.ReadAll(r)
		t.NoError(err)
		t.T().Log("body:", string(b))

		t.Equal(id, string(b))
	})

	t.Run("empty body", func() {
		c := NewHeaderClient(t.encs, t.enc, t.writef(prefix, handler))

		header := newDummyHeader(prefix, id)

		rh, r, _, renc, err := c.RequestBody(context.Background(), localci, header, nil)
		t.NoError(err)

		t.T().Log("response header:", util.MustMarshalJSONIndentString(rh))
		t.T().Logf("response header type: %T", rh)
		t.T().Log("response encoder:", renc.Hint())

		t.NoError(rh.Err())
		t.True(rh.OK())

		rdh, ok := rh.(dummyResponseHeader)
		t.True(ok)
		t.Equal(id, rdh.ID)

		b, err := io.ReadAll(r)
		t.NoError(err)
		t.T().Log("body:", string(b))

		t.Empty(b)
	})
}

func (t *testHeaderClient) TestRequestBodyAddress() {
	localci := NewUDPConnInfo(nil, true)

	prefix := util.UUID().String()
	id := util.UUID().String()

	address := base.RandomAddress("")

	handler := NewHeaderHandler(
		t.encs,
		time.Second,
		func(_ net.Addr, r io.Reader, w io.Writer, h Header, encs *encoder.Encoders, enc encoder.Encoder) error {
			t.T().Log("request header:", util.MustMarshalJSONIndentString(h))
			t.T().Logf("request header type: %T", h)
			t.T().Log("request encoder:", enc.Hint())

			t.IsType(dummyHeader{}, h)

			_, ok := h.(dummyHeader)
			t.True(ok)

			{
				b, err := io.ReadAll(r)
				t.NoError(err)
				t.T().Log("body:", string(b))
			}

			b, err := enc.Marshal(address)
			t.NoError(err)

			buf := bytes.NewBuffer(b)
			defer buf.Reset()

			return WriteResponseBody(w, newDummyResponseHeader(true, nil, HinterContentType, id), t.enc, buf)
		},
	)

	t.Run("ok", func() {
		c := NewHeaderClient(t.encs, t.enc, t.writef(prefix, handler))

		header := newDummyHeader(prefix, id)

		ab, err := t.enc.Marshal(address)
		t.NoError(err)

		buf := bytes.NewBuffer(ab)
		defer buf.Reset()

		rh, r, _, renc, err := c.RequestBody(context.Background(), localci, header, buf)
		t.NoError(err)

		t.T().Log("response header:", util.MustMarshalJSONIndentString(rh))
		t.T().Logf("response header type: %T", rh)
		t.T().Log("response encoder:", renc.Hint())

		t.NoError(rh.Err())
		t.True(rh.OK())

		rdh, ok := rh.(dummyResponseHeader)
		t.True(ok)
		t.Equal(id, rdh.ID)

		b, err := io.ReadAll(r)
		t.NoError(err)
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
	localci := NewUDPConnInfo(nil, true)

	prefix := util.UUID().String()
	id := util.UUID().String()

	node := base.NewBaseNode(dummyNodeHint,
		base.NewMPrivatekey().Publickey(),
		base.RandomAddress("showme"),
	)

	handler := NewHeaderHandler(
		t.encs,
		time.Second,
		func(_ net.Addr, r io.Reader, w io.Writer, h Header, encs *encoder.Encoders, enc encoder.Encoder) error {
			t.T().Log("request header:", util.MustMarshalJSONIndentString(h))
			t.T().Logf("request header type: %T", h)
			t.T().Log("request encoder:", enc.Hint())

			t.IsType(dummyHeader{}, h)

			_, ok := h.(dummyHeader)
			t.True(ok)

			var body json.RawMessage
			{
				b, err := io.ReadAll(r)
				t.NoError(err)
				t.T().Log("body:", string(b))

				t.NoError(enc.Unmarshal(b, &body))
			}
			buf := bytes.NewBuffer(body)
			defer buf.Reset()

			return WriteResponseBody(w, newDummyResponseHeader(true, nil, HinterContentType, id), t.enc, buf)
		},
	)

	t.Run("ok", func() {
		c := NewHeaderClient(t.encs, t.enc, t.writef(prefix, handler))

		header := newDummyHeader(prefix, id)

		var u base.Node

		rh, enc, err := c.RequestDecode(context.Background(), localci, header, node, &u)
		t.NoError(err)

		t.T().Log("response header:", util.MustMarshalJSONIndentString(rh))
		t.T().Logf("response header type: %T", rh)
		t.T().Logf("response encoder: %s", enc.Hint())

		t.NoError(rh.Err())
		t.True(rh.OK())
		t.NotNil(u)

		rdh, ok := rh.(dummyResponseHeader)
		t.True(ok)
		t.Equal(id, rdh.ID)

		t.T().Log("response node:", util.MustMarshalJSONIndentString(u))

		t.True(base.IsEqualNode(node, u))
	})

	t.Run("nil", func() {
		c := NewHeaderClient(t.encs, t.enc, t.writef(prefix, handler))

		header := newDummyHeader(prefix, id)

		var u base.Node

		rh, enc, err := c.RequestDecode(context.Background(), localci, header, nil, &u)
		t.NoError(err)

		t.T().Log("response header:", util.MustMarshalJSONIndentString(rh))
		t.T().Logf("response header type: %T", rh)
		t.T().Logf("response encoder: %s", enc.Hint())

		t.NoError(rh.Err())
		t.True(rh.OK())
		t.Nil(u)

		rdh, ok := rh.(dummyResponseHeader)
		t.True(ok)
		t.Equal(id, rdh.ID)

		t.T().Log("response node:", util.MustMarshalJSONIndentString(u))
	})
}

func TestHeaderClient(t *testing.T) {
	defer goleak.VerifyNone(t,
		goleak.IgnoreTopFunction("github.com/syndtr/goleveldb/leveldb.(*DB).mpoolDrain"),
	)

	suite.Run(t, new(testHeaderClient))
}
