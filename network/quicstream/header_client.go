package quicstream

import (
	"bytes"
	"context"
	"io"

	"github.com/pkg/errors"
	"github.com/quic-go/quic-go"
	"github.com/spikeekips/mitum/util"
	"github.com/spikeekips/mitum/util/encoder"
	"github.com/spikeekips/mitum/util/hint"
)

type OpenStreamFunc func(context.Context, UDPConnInfo) (io.ReadCloser, io.WriteCloser, error)

type HeaderClient struct {
	Encoders    *encoder.Encoders
	Encoder     encoder.Encoder
	openStreamf OpenStreamFunc
}

func NewHeaderClient(encs *encoder.Encoders, enc encoder.Encoder, openStreamf OpenStreamFunc) *HeaderClient {
	return &HeaderClient{
		Encoders:    encs,
		Encoder:     enc,
		openStreamf: openStreamf,
	}
}

func (c *HeaderClient) OpenStream(ctx context.Context, ci UDPConnInfo) (io.ReadCloser, io.WriteCloser, error) {
	return c.openStreamf(ctx, ci)
}

func (c *HeaderClient) Request( //revive:disable-line:function-result-limit
	ctx context.Context,
	ci UDPConnInfo,
	header RequestHeader,
	bodyf func() (DataFormat, io.Reader, uint64, error),
) (
	ResponseHeader,
	encoder.Encoder,
	io.Reader, // response data body
	io.ReadCloser,
	io.WriteCloser,
	error,
) {
	e := util.StringErrorFunc("request")

	if header == nil {
		return nil, nil, nil, nil, nil, e(nil, "empty header")
	}

	var r io.ReadCloser
	var w io.WriteCloser

	switch i, j, err := c.openStreamf(ctx, ci); {
	case err != nil:
		return nil, nil, nil, nil, nil, e(err, "open stream")
	default:
		r = i
		w = j
	}

	switch rh, renc, datar, err := c.request(header, r, w, bodyf); {
	case err != nil:
		_ = r.Close()
		_ = w.Close()

		var serr *quic.StreamError
		if errors.As(err, &serr) {
			return nil, nil, nil, nil, nil, util.JoinErrors(e(err, ""), context.DeadlineExceeded)
		}

		return nil, nil, nil, nil, nil, e(err, "")
	case rh == nil:
		return rh, renc, datar, r, w, nil
	case rh.Err() != nil:
		_ = r.Close()
		_ = w.Close()

		return rh, renc, nil, r, w, nil
	default:
		return rh, renc, datar, r, w, nil
	}
}

func (c *HeaderClient) request(
	header RequestHeader,
	r io.Reader,
	w io.Writer,
	bodyf func() (DataFormat, io.Reader, uint64, error),
) (
	ResponseHeader,
	encoder.Encoder,
	io.Reader, // response data body
	error,
) {
	if err := c.send(header, w, bodyf); err != nil {
		return nil, nil, nil, err
	}

	switch h, enc, dataFormat, bodyLength, err := c.read(r); {
	case err != nil:
		return nil, nil, nil, err
	case h == nil:
		return h, enc, r, nil
	case h.Err() != nil:
		return h, enc, nil, nil
	case dataFormat == StreamDataFormat:
		return h, enc, r, nil
	default:
		return h, enc, io.NewSectionReader(readerAt{r}, 0, int64(bodyLength)), nil
	}
}

func (c *HeaderClient) send(
	header RequestHeader,
	w io.Writer,
	bodyf func() (DataFormat, io.Reader, uint64, error),
) error {
	var dataFormat DataFormat
	var bodyr io.Reader
	var bodyLength uint64

	switch i, j, k, err := bodyf(); {
	case err != nil:
		return errors.WithMessage(err, "body")
	default:
		dataFormat = i
		bodyr = j
		bodyLength = k
	}

	if err := dataFormat.IsValid(nil); err != nil {
		return errors.WithMessage(err, "invalid DataFormat")
	}

	if err := WriteRequestHead(w, header.Handler(), c.Encoder, header); err != nil {
		return errors.WithMessage(err, "")
	}

	return WriteBody(w, dataFormat, bodyLength, bodyr)
}

func (c *HeaderClient) read(
	r io.Reader,
) (
	ResponseHeader,
	encoder.Encoder,
	DataFormat,
	uint64, // body length
	error,
) {
	var renc encoder.Encoder
	var ah Header
	var rdataFormat DataFormat
	var bodyLength uint64

	switch i, j, k, l, err := ReadHead(r, c.Encoders); {
	case err != nil:
		return nil, nil, 0, 0, errors.WithMessage(err, "read response")
	default:
		renc = i
		ah = j
		rdataFormat = k
		bodyLength = l
	}

	if renc == nil {
		return nil, nil, 0, 0, errors.Errorf("empty response encoder")
	}

	var rh ResponseHeader

	if ah != nil {
		if err := ah.IsValid(nil); err != nil {
			return nil, nil, 0, 0, errors.WithMessage(err, "invalid response header")
		}

		switch i, ok := ah.(ResponseHeader); {
		case !ok:
			return nil, nil, 0, 0, errors.Errorf("expected response header, but %T", ah)
		default:
			rh = i
		}
	}

	if err := rdataFormat.IsValid(nil); err != nil {
		return nil, nil, 0, 0, errors.WithMessage(err, "invalid response DataFormat")
	}

	return rh, renc, rdataFormat, bodyLength, nil
}

func WriteRequestHead(w io.Writer, prefix string, enc encoder.Encoder, header Header) error {
	if err := WritePrefix(w, prefix); err != nil {
		return errors.WithMessage(err, "prefix")
	}

	return WriteHead(w, enc, header)
}

func WriteHead(w io.Writer, enc encoder.Encoder, header Header) error {
	var headerb []byte

	switch {
	case header == nil:
	default:
		b, err := enc.Marshal(header)
		if err != nil {
			return err
		}

		headerb = b
	}

	if err := util.WriteLengthedBytes(w, enc.Hint().Bytes()); err != nil {
		return errors.WithMessage(err, "encoder hint")
	}

	if err := util.WriteLengthedBytes(w, headerb); err != nil {
		return errors.WithMessage(err, "header")
	}

	return nil
}

func ReadHead(r io.Reader, encs *encoder.Encoders) (
	enc encoder.Encoder,
	header Header,
	dataFormat DataFormat,
	bodyLength uint64,
	_ error,
) {
	var hintb []byte

	switch b, err := util.ReadLengthedBytesFromReader(r); {
	case err != nil:
		return nil, nil, dataFormat, 0, errors.WithMessage(err, "encoder hint")
	default:
		hintb = b
	}

	var headerb []byte

	switch b, err := util.ReadLengthedBytesFromReader(r); {
	case err != nil:
		return nil, nil, dataFormat, 0, errors.WithMessage(err, "header")
	default:
		headerb = b
	}

	switch i, err := util.ReadLengthFromReader(r); {
	case err == nil, errors.Is(err, io.EOF):
		dataFormat = DataFormat(i)
	default:
		return nil, nil, dataFormat, 0, errors.WithMessage(err, "data format")
	}

	if dataFormat == LengthedDataFormat {
		i, err := util.ReadLengthFromReader(r)

		switch {
		case err == nil, errors.Is(err, io.EOF):
		default:
			return nil, nil, dataFormat, 0, errors.WithMessage(err, "body length")
		}

		bodyLength = i
	}

	ht, err := hint.ParseHint(string(hintb))
	if err != nil {
		return nil, nil, dataFormat, 0, errors.WithMessage(err, "encoder hint")
	}

	if enc = encs.Find(ht); enc == nil {
		return nil, nil, dataFormat, 0, util.ErrNotFound.Errorf("encoder not found for %q", ht)
	}

	if err := encoder.Decode(enc, headerb, &header); err != nil {
		return nil, nil, dataFormat, 0, errors.WithMessage(err, "header")
	}

	return enc, header, dataFormat, bodyLength, nil
}

func WriteBody(w io.Writer, dataFormat DataFormat, bodyLength uint64, r io.Reader) error {
	if bodyLength > 0 && r == nil {
		return errors.Errorf("bodyLength > 0, but nil reader")
	}

	if err := util.WriteLength(w, uint64(dataFormat)); err != nil {
		return errors.WithMessage(err, "dataformat")
	}

	switch dataFormat {
	case LengthedDataFormat:
		if err := util.WriteLength(w, bodyLength); err != nil {
			return errors.WithMessage(err, "body length")
		}
	case StreamDataFormat:
	default:
		return errors.Errorf("unknown DataFormat, %d", dataFormat)
	}

	if r != nil {
		if _, err := io.Copy(w, r); err != nil {
			return errors.WithMessage(err, "write body")
		}
	}

	return nil
}

type DataFormat uint64

const (
	_ DataFormat = iota
	LengthedDataFormat
	StreamDataFormat
)

func (d DataFormat) IsValid([]byte) error {
	switch d {
	case LengthedDataFormat:
	case StreamDataFormat:
	default:
		return util.ErrInvalid.Errorf("unknown DataFormat, %d", d)
	}

	return nil
}

func HeaderRequestEncode( //revive:disable-line:function-result-limit
	ctx context.Context,
	client *HeaderClient,
	ci UDPConnInfo,
	header RequestHeader,
	body interface{},
) (
	ResponseHeader,
	encoder.Encoder,
	io.Reader, // response data body
	io.ReadCloser,
	io.WriteCloser,
	error,
) {
	b, err := client.Encoder.Marshal(body)
	if err != nil {
		return nil, nil, nil, nil, nil, err
	}

	buf := bytes.NewBuffer(b)
	defer buf.Reset()

	return client.Request(ctx, ci, header,
		func() (DataFormat, io.Reader, uint64, error) {
			return LengthedDataFormat, buf, uint64(buf.Len()), nil
		},
	)
}

func HeaderRequestDecode(
	ctx context.Context,
	client *HeaderClient,
	ci UDPConnInfo,
	header RequestHeader,
	i, u interface{},
	decode func(encoder.Encoder, io.Reader, interface{}) error,
) (
	ResponseHeader,
	encoder.Encoder,
	io.ReadCloser,
	io.WriteCloser,
	error,
) {
	h, enc, body, r, w, err := HeaderRequestEncode(ctx, client, ci, header, i)
	if err != nil {
		return nil, nil, nil, nil, err
	}

	if !h.OK() {
		return h, enc, r, w, nil
	}

	switch err := decode(enc, body, u); {
	case err == nil:
	case errors.Is(err, io.EOF):
	default:
		_ = r.Close()
		_ = w.Close()

		return nil, nil, nil, nil, err
	}

	return h, enc, r, w, nil
}

type readerAt struct {
	io.Reader
}

func (r readerAt) ReadAt(p []byte, _ int64) (int, error) {
	n, err := r.Read(p)

	return n, errors.WithStack(err)
}
