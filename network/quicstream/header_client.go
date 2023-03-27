package quicstream

import (
	"bytes"
	"context"
	"io"
	"sync"

	"github.com/pkg/errors"
	"github.com/spikeekips/mitum/util"
	"github.com/spikeekips/mitum/util/encoder"
	"github.com/spikeekips/mitum/util/hint"
)

type HeaderBrokerFunc func(context.Context, UDPConnInfo) (*HeaderClientBroker, error)

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

func (c *HeaderClient) Broker(ctx context.Context, ci UDPConnInfo) (*HeaderClientBroker, error) {
	r, w, err := c.openStreamf(ctx, ci)
	if err != nil {
		return nil, err
	}

	return NewHeaderClientBroker(c.Encoders, c.Encoder, r, w), nil
}

type HeaderClientBroker struct {
	Encoders  *encoder.Encoders
	Encoder   encoder.Encoder
	r         io.ReadCloser
	w         io.WriteCloser
	closeonce sync.Once
}

func NewHeaderClientBroker(
	encs *encoder.Encoders,
	enc encoder.Encoder,
	r io.ReadCloser,
	w io.WriteCloser,
) *HeaderClientBroker {
	return &HeaderClientBroker{Encoders: encs, Encoder: enc, r: r, w: w}
}

func (h *HeaderClientBroker) Close() error {
	h.closeonce.Do(func() {
		_ = h.r.Close()
		_ = h.w.Close()
	})

	return nil
}

func (h *HeaderClientBroker) WriteRequestHead(header RequestHeader) error {
	if header == nil {
		return errors.Errorf("empty header")
	}

	return HeaderWriteRequestHead(h.w, header.Handler(), h.Encoder, header)
}

func (h *HeaderClientBroker) WriteHead(header Header) error {
	return HeaderWriteHead(h.w, h.Encoder, header)
}

func (h *HeaderClientBroker) ReadRequestHead() (enc encoder.Encoder, header RequestHeader, _ error) {
	enc, i, err := h.ReadHead()

	switch {
	case err != nil:
		return nil, nil, err
	case i == nil:
		return enc, nil, nil
	}

	rh, ok := i.(RequestHeader)
	if !ok {
		return nil, nil, errors.Errorf("expected RequestHeader, but %T", i)
	}

	return enc, rh, nil
}

func (h *HeaderClientBroker) ReadResponseHead() (enc encoder.Encoder, header ResponseHeader, _ error) {
	enc, i, err := h.ReadHead()

	switch {
	case err != nil:
		return nil, nil, err
	case i == nil:
		return enc, nil, nil
	}

	rh, ok := i.(ResponseHeader)
	if !ok {
		return nil, nil, errors.Errorf("expected ResponseHeader, but %T", i)
	}

	return enc, rh, nil
}

func (h *HeaderClientBroker) ReadHead() (enc encoder.Encoder, header Header, _ error) {
	return HeaderReadHead(h.r, h.Encoders)
}

func (h *HeaderClientBroker) WriteBody(dataFormat HeaderDataFormat, bodyLength uint64, r io.Reader) error {
	if err := HeaderWriteBody(h.w, dataFormat, bodyLength, r); err != nil {
		_ = h.Close()

		return err
	}

	if dataFormat == StreamDataFormat {
		_ = h.w.Close()
	}

	return nil
}

func (h *HeaderClientBroker) ReadBody() (
	HeaderDataFormat,
	uint64,
	io.Reader, // response data body
	error,
) {
	return HeaderReadBody(h.r)
}

func HeaderWriteRequestHead(w io.Writer, prefix []byte, enc encoder.Encoder, header Header) error {
	if err := WritePrefix(w, prefix); err != nil {
		return errors.WithMessage(err, "prefix")
	}

	return HeaderWriteHead(w, enc, header)
}

func HeaderWriteHead(w io.Writer, enc encoder.Encoder, header Header) error {
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

	if err := util.WriteLengthed(w, enc.Hint().Bytes()); err != nil {
		return errors.WithMessage(err, "encoder hint")
	}

	if err := util.WriteLengthed(w, headerb); err != nil {
		return errors.WithMessage(err, "header")
	}

	return nil
}

func HeaderReadHead(r io.Reader, encs *encoder.Encoders) (
	enc encoder.Encoder,
	header Header,
	_ error,
) {
	var hintb []byte

	switch b, err := util.ReadLengthed(r); {
	case err != nil && !errors.Is(err, io.EOF):
		return nil, nil, errors.WithMessage(err, "encoder hint")
	default:
		hintb = b
	}

	var headerb []byte

	switch b, err := util.ReadLengthed(r); {
	case err != nil && !errors.Is(err, io.EOF):
		return nil, nil, errors.WithMessage(err, "header")
	default:
		headerb = b
	}

	ht, err := hint.ParseHint(string(hintb))
	if err != nil {
		return nil, nil, errors.WithMessage(err, "encoder hint")
	}

	if enc = encs.Find(ht); enc == nil {
		return nil, nil, util.ErrNotFound.Errorf("encoder not found for %q", ht)
	}

	if err := encoder.Decode(enc, headerb, &header); err != nil {
		return nil, nil, errors.WithMessage(err, "header")
	}

	return enc, header, nil
}

func HeaderReadBody(r io.Reader) (
	HeaderDataFormat,
	uint64,
	io.Reader, // response data body
	error,
) {
	dataFormat := UnknownDataFormat

	var isEOF bool

	switch i, err := readHeaderDataFormat(r); {
	case err != nil && !errors.Is(err, io.EOF):
		return dataFormat, 0, nil, err
	default:
		isEOF = errors.Is(err, io.EOF)

		dataFormat = i

		if err := dataFormat.IsValid(nil); err != nil {
			return dataFormat, 0, nil, errors.WithMessage(err, "invalid data format")
		}
	}

	var body io.Reader = bytes.NewBuffer(nil)
	var bodyLength uint64

	switch dataFormat {
	case EmptyDataFormat:
	case LengthedDataFormat:
		i, err := util.ReadLength(r)

		switch {
		case err == nil, errors.Is(err, io.EOF):
			isEOF = errors.Is(err, io.EOF)
		default:
			return dataFormat, 0, nil, errors.WithMessage(err, "data format")
		}

		bodyLength = i

		if bodyLength > 0 && !isEOF {
			body = io.NewSectionReader(readerAt{r}, 0, int64(bodyLength))
		}
	case StreamDataFormat:
		if !isEOF {
			body = r
		}
	default:
		return dataFormat, 0, nil, errors.Errorf("unknown DataFormat, %d", dataFormat)
	}

	return dataFormat, bodyLength, body, nil
}

type HeaderDataFormat uint64

const (
	UnknownDataFormat HeaderDataFormat = iota
	EmptyDataFormat
	LengthedDataFormat
	StreamDataFormat
)

func (d HeaderDataFormat) String() string {
	switch d {
	case EmptyDataFormat:
		return "empty"
	case LengthedDataFormat:
		return "lengthed"
	case StreamDataFormat:
		return "stream"
	default:
		return "<unknown>"
	}
}

func (d HeaderDataFormat) IsValid([]byte) error {
	switch d {
	case EmptyDataFormat:
	case LengthedDataFormat:
	case StreamDataFormat:
	default:
		return util.ErrInvalid.Errorf("unknown DataFormat, %d", d)
	}

	return nil
}

func HeaderRequestWithBody( //revive:disable-line:function-result-limit
	ctx context.Context,
	client *HeaderClient,
	ci UDPConnInfo,
	header RequestHeader,
	body interface{},
) (
	io.ReadCloser,
	io.WriteCloser,
	error,
) {
	b, err := client.Encoder.Marshal(body)
	if err != nil {
		return nil, nil, err
	}

	buf := bytes.NewBuffer(b)
	defer buf.Reset()

	var r io.ReadCloser
	var w io.WriteCloser

	switch i, j, err := client.OpenStream(ctx, ci); {
	case err != nil:
		return nil, nil, err
	default:
		r = i
		w = j
	}

	closef := func() {
		_ = r.Close()
		_ = w.Close()
	}

	if err := HeaderWriteHead(w, client.Encoder, header); err != nil {
		closef()

		return nil, nil, err
	}

	if err := HeaderWriteBody(w, LengthedDataFormat, uint64(buf.Len()), buf); err != nil {
		closef()

		return nil, nil, err
	}

	return r, w, nil
}

// HeaderWriteBody writes data body to remote. If dataFormat is
// StreamDataFormat, w, io.Writer should be closed after writing. If not, remote
// could be hanged up.
func HeaderWriteBody(w io.Writer, dataFormat HeaderDataFormat, bodyLength uint64, r io.Reader) error {
	bl := bodyLength

	if bl > 0 && r == nil {
		return errors.Errorf("bodyLength > 0, but nil reader")
	}

	if dataFormat == EmptyDataFormat {
		bl = 0
	}

	if err := util.WriteLength(w, uint64(dataFormat)); err != nil {
		return errors.WithMessage(err, "dataformat")
	}

	var body io.Reader

	switch dataFormat {
	case EmptyDataFormat:
	case LengthedDataFormat:
		if err := util.WriteLength(w, bl); err != nil {
			return errors.WithMessage(err, "body length")
		}

		body = r
	case StreamDataFormat:
		body = r
	default:
		return errors.Errorf("unknown DataFormat, %d", dataFormat)
	}

	if body != nil {
		if _, err := io.Copy(w, body); err != nil {
			return errors.WithMessage(err, "write body")
		}
	}

	return nil
}

func WriteResponse(
	w io.Writer,
	enc encoder.Encoder,
	header ResponseHeader,
	dataFormat HeaderDataFormat,
	bodyLength uint64,
	body io.Reader,
) error {
	if err := HeaderWriteHead(w, enc, header); err != nil {
		return err
	}

	return HeaderWriteBody(w, dataFormat, bodyLength, body)
}

type readerAt struct {
	io.Reader
}

func (r readerAt) ReadAt(p []byte, _ int64) (int, error) {
	n, err := r.Read(p)

	return n, err //nolint:wrapcheck //...
}

func readHeaderDataFormat(r io.Reader) (HeaderDataFormat, error) {
	switch i, err := util.ReadLength(r); {
	case err != nil && !errors.Is(err, io.EOF):
		return UnknownDataFormat, errors.WithMessage(err, "data format")
	default:
		dataFormat := HeaderDataFormat(i)

		if dataFormat == UnknownDataFormat && errors.Is(err, io.EOF) {
			dataFormat = EmptyDataFormat
		}

		return dataFormat, err
	}
}
