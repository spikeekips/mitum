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

type OpenStreamFunc func(context.Context, UDPConnInfo) (io.Reader, io.WriteCloser, error)

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

func (c *HeaderClient) OpenStream(ctx context.Context, ci UDPConnInfo) (io.Reader, io.WriteCloser, error) {
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
	r         io.Reader
	w         io.WriteCloser
	closeonce sync.Once
}

func NewHeaderClientBroker(
	encs *encoder.Encoders,
	enc encoder.Encoder,
	r io.Reader,
	w io.WriteCloser,
) *HeaderClientBroker {
	return &HeaderClientBroker{Encoders: encs, Encoder: enc, r: r, w: w}
}

func (h *HeaderClientBroker) Close() error {
	h.closeonce.Do(func() {
		_ = h.w.Close()
	})

	return nil
}

func (h *HeaderClientBroker) WriteRequestHead(ctx context.Context, header RequestHeader) error {
	if header == nil {
		return errors.Errorf("empty header")
	}

	return h.closeByContextError(
		HeaderWriteRequestHead(ctx, h.w, header.Handler(), h.Encoder, header),
	)
}

func (h *HeaderClientBroker) WriteHead(ctx context.Context, header Header) error {
	return h.closeByContextError(
		HeaderWriteHead(ctx, h.w, h.Encoder, header),
	)
}

func (h *HeaderClientBroker) ReadRequestHead(ctx context.Context) (enc encoder.Encoder, header RequestHeader, _ error) {
	enc, i, err := h.ReadHead(ctx)

	switch {
	case err != nil:
		return nil, nil, h.closeByContextError(err)
	case i == nil:
		return enc, nil, nil
	}

	rh, ok := i.(RequestHeader)
	if !ok {
		return nil, nil, errors.Errorf("expected RequestHeader, but %T", i)
	}

	return enc, rh, nil
}

func (h *HeaderClientBroker) ReadResponseHead(ctx context.Context) (
	enc encoder.Encoder,
	header ResponseHeader,
	_ error,
) {
	enc, i, err := h.ReadHead(ctx)

	switch {
	case err != nil:
		return nil, nil, h.closeByContextError(err)
	case i == nil:
		return enc, nil, nil
	}

	rh, ok := i.(ResponseHeader)
	if !ok {
		return nil, nil, errors.Errorf("expected ResponseHeader, but %T", i)
	}

	return enc, rh, nil
}

func (h *HeaderClientBroker) ReadHead(ctx context.Context) (encoder.Encoder, Header, error) {
	enc, header, err := HeaderReadHead(ctx, h.r, h.Encoders)

	return enc, header, h.closeByContextError(err)
}

func (h *HeaderClientBroker) WriteBody(
	ctx context.Context, dataFormat HeaderDataFormat, bodyLength uint64, r io.Reader,
) error {
	if err := HeaderWriteBody(ctx, h.w, dataFormat, bodyLength, r); err != nil {
		return h.closeByContextError(err)
	}

	if dataFormat == StreamDataFormat {
		_ = h.w.Close()
	}

	return nil
}

func (h *HeaderClientBroker) ReadBody(ctx context.Context) (
	HeaderDataFormat,
	uint64,
	io.Reader, // NOTE response data body
	error,
) {
	dataFormat, bodyLength, r, err := HeaderReadBody(ctx, h.r)

	return dataFormat, bodyLength, r, h.closeByContextError(err)
}

func (h *HeaderClientBroker) closeByContextError(err error) error {
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		_ = h.Close()
	}

	return err
}

func HeaderWriteRequestHead(ctx context.Context, w io.Writer, prefix []byte, enc encoder.Encoder, header Header) error {
	return util.AwareContext(ctx, func(ctx context.Context) error {
		if err := WritePrefix(ctx, w, prefix); err != nil {
			return errors.WithMessage(err, "prefix")
		}

		return HeaderWriteHead(ctx, w, enc, header)
	})
}

func HeaderWriteHead(ctx context.Context, w io.Writer, enc encoder.Encoder, header Header) error {
	return util.AwareContext(ctx, func(ctx context.Context) error {
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
	})
}

func HeaderReadHead(ctx context.Context, r io.Reader, encs *encoder.Encoders) (
	encoder.Encoder,
	Header,
	error,
) {
	i, err := util.AwareContextValue[[2]interface{}](ctx, func(ctx context.Context) ([2]interface{}, error) {
		var i [2]interface{}
		var hintb []byte

		switch b, err := util.ReadLengthed(r); {
		case err != nil && !errors.Is(err, io.EOF):
			return i, errors.WithMessage(err, "encoder hint")
		default:
			hintb = b
		}

		var headerb []byte

		switch b, err := util.ReadLengthed(r); {
		case err != nil && !errors.Is(err, io.EOF):
			return i, errors.WithMessage(err, "header")
		default:
			headerb = b
		}

		ht, err := hint.ParseHint(string(hintb))
		if err != nil {
			return i, errors.WithMessage(err, "encoder hint")
		}

		var enc encoder.Encoder
		if enc = encs.Find(ht); enc == nil {
			return i, util.ErrNotFound.Errorf("encoder not found for %q", ht)
		}

		var header Header
		if err := encoder.Decode(enc, headerb, &header); err != nil {
			return i, errors.WithMessage(err, "header")
		}

		return [2]interface{}{enc, header}, nil
	})
	if err != nil {
		return nil, nil, err
	}

	var header Header

	if i[1] != nil {
		header = i[1].(Header) //nolint:forcetypeassert //...
	}

	return i[0].(encoder.Encoder), header, nil //nolint:forcetypeassert //...
}

func HeaderReadBody(ctx context.Context, r io.Reader) (
	HeaderDataFormat,
	uint64,
	io.Reader, // response data body
	error,
) {
	i, err := util.AwareContextValue[[3]interface{}](ctx, func(ctx context.Context) ([3]interface{}, error) {
		var v [3]interface{}
		var body io.Reader = bytes.NewBuffer(nil)
		var bodyLength uint64
		dataFormat := UnknownDataFormat
		var isEOF bool

		switch i, err := readHeaderDataFormat(ctx, r); {
		case err != nil && !errors.Is(err, io.EOF):
			return v, err
		default:
			isEOF = errors.Is(err, io.EOF)

			dataFormat = i

			if err := dataFormat.IsValid(nil); err != nil {
				return v, errors.WithMessage(err, "invalid data format")
			}
		}

		switch dataFormat {
		case EmptyDataFormat:
		case LengthedDataFormat:
			i, err := util.ReadLength(r)

			switch {
			case err == nil, errors.Is(err, io.EOF):
				isEOF = errors.Is(err, io.EOF)
			default:
				return v, errors.WithMessage(err, "data format")
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
			return v, errors.Errorf("unknown DataFormat, %d", dataFormat)
		}

		return [3]interface{}{dataFormat, bodyLength, body}, nil
	})
	if err != nil {
		return UnknownDataFormat, 0, nil, err
	}

	var body io.Reader
	if i[2] != nil {
		body = i[2].(io.Reader) //nolint:forcetypeassert //...
	}

	return i[0].(HeaderDataFormat), i[1].(uint64), body, nil //nolint:forcetypeassert //...
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

// HeaderWriteBody writes data body to remote. If dataFormat is
// StreamDataFormat, w, io.Writer should be closed after writing. If not, remote
// could be hanged up.
func HeaderWriteBody(
	ctx context.Context, w io.Writer, dataFormat HeaderDataFormat, bodyLength uint64, r io.Reader,
) error {
	return util.AwareContext(ctx, func(ctx context.Context) error {
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
	})
}

func WriteResponse(
	ctx context.Context,
	w io.Writer,
	enc encoder.Encoder,
	header ResponseHeader,
	dataFormat HeaderDataFormat,
	bodyLength uint64,
	body io.Reader,
) error {
	if err := HeaderWriteHead(ctx, w, enc, header); err != nil {
		return err
	}

	return HeaderWriteBody(ctx, w, dataFormat, bodyLength, body)
}

type readerAt struct {
	io.Reader
}

func (r readerAt) ReadAt(p []byte, _ int64) (int, error) {
	n, err := r.Read(p)

	return n, err //nolint:wrapcheck //...
}

func readHeaderDataFormat(ctx context.Context, r io.Reader) (HeaderDataFormat, error) {
	return util.AwareContextValue[HeaderDataFormat](ctx, func(ctx context.Context) (HeaderDataFormat, error) {
		switch i, err := util.ReadLength(r); {
		case err != nil && !errors.Is(err, io.EOF):
			return UnknownDataFormat, errors.WithMessage(err, "data format")
		default:
			dataFormat := HeaderDataFormat(i)

			if derr := dataFormat.IsValid(nil); derr != nil && errors.Is(err, io.EOF) {
				dataFormat = EmptyDataFormat
			}

			return dataFormat, err
		}
	})
}
