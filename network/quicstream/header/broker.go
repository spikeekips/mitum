package quicstreamheader

import (
	"bytes"
	"context"
	"io"
	"sync"

	"github.com/pkg/errors"
	"github.com/spikeekips/mitum/network/quicstream"
	"github.com/spikeekips/mitum/util"
	"github.com/spikeekips/mitum/util/encoder"
	"github.com/spikeekips/mitum/util/hint"
)

type ClientBroker struct {
	*baseBroker
}

func NewClientBroker(
	encs *encoder.Encoders,
	enc encoder.Encoder,
	r io.Reader,
	w io.WriteCloser,
) *ClientBroker {
	return &ClientBroker{baseBroker: newBaseBroker(encs, enc, r, w)}
}

func (broker *ClientBroker) WriteRequestHead(ctx context.Context, header RequestHeader) error {
	if header.Handler() == quicstream.ZeroPrefix {
		return errors.Errorf("write request; empty prefix")
	}

	if err := quicstream.WritePrefix(ctx, broker.Writer, header.Handler()); err != nil {
		return broker.closeIfError(quicstream.ErrNetwork.WithMessage(err, "write request; prefix"))
	}

	return errors.WithMessage(
		broker.writeHead(ctx, RequestHeaderDataType, header),
		"write request",
	)
}

func (broker *ClientBroker) ReadResponseHead(ctx context.Context) (enc encoder.Encoder, res ResponseHeader, _ error) {
	if err := func() error {
		var dataType DataType
		var err error

		switch dataType, err = broker.readDataType(ctx); {
		case err != nil:
			return err
		case dataType != ResponseHeaderDataType:
			return errors.Errorf("expected response header data type, but %v", dataType)
		}

		var header Header

		switch enc, header, err = broker.readHead(ctx, dataType); {
		case err == nil, errors.Is(err, io.EOF):
			res = header.(ResponseHeader) //nolint:forcetypeassert //...

			return nil
		default:
			return err
		}
	}(); err != nil {
		return nil, nil, errors.WithMessage(err, "read response head")
	}

	return enc, res, nil
}

type HandlerBroker struct {
	*baseBroker
}

func NewHandlerBroker(
	encs *encoder.Encoders,
	enc encoder.Encoder,
	r io.Reader,
	w io.Writer,
) *HandlerBroker {
	return &HandlerBroker{baseBroker: newBaseBroker(encs, enc, r, w)}
}

func (broker *HandlerBroker) WriteResponseHead(ctx context.Context, res ResponseHeader) error {
	if broker.Encoder == nil {
		return errors.Errorf("empty encoder; read request head first")
	}

	return errors.WithMessage(
		broker.writeHead(ctx, ResponseHeaderDataType, res),
		"write response head",
	)
}

func (broker *HandlerBroker) WriteResponseHeadOK(ctx context.Context, ok bool, err error) error {
	return broker.WriteResponseHead(ctx, NewDefaultResponseHeader(ok, err))
}

func (broker *HandlerBroker) ReadRequestHead(ctx context.Context) (header RequestHeader, _ error) {
	if err := func() error {
		var dataType DataType

		switch t, err := broker.readDataType(ctx); {
		case err != nil:
			return err
		case t != RequestHeaderDataType:
			return errors.Errorf("expected request header data type, but %v", dataType)
		default:
			dataType = t
		}

		switch i, h, err := broker.readHead(ctx, dataType); {
		case err != nil:
			return err
		default:
			broker.Encoder = i
			header = h.(RequestHeader) //nolint:forcetypeassert //...
		}

		return nil
	}(); err != nil {
		return nil, errors.WithMessage(err, "read request head")
	}

	return header, nil
}

type baseBroker struct {
	Encoders *encoder.Encoders
	Encoder  encoder.Encoder
	Reader   io.Reader
	Writer   io.Writer
	closef   func() error
}

func newBaseBroker(
	encs *encoder.Encoders,
	enc encoder.Encoder,
	r io.Reader,
	w io.Writer,
) *baseBroker {
	var wclose func() error

	switch i, ok := w.(io.WriteCloser); {
	case ok:
		wclose = i.Close
	default:
		wclose = func() error { return nil }
	}

	var closeonce sync.Once

	closef := func() error {
		var err error

		closeonce.Do(func() {
			err = wclose()
		})

		return errors.WithStack(err)
	}

	return &baseBroker{
		Encoders: encs,
		Encoder:  enc,
		Reader:   r,
		Writer:   w,
		closef:   closef,
	}
}

func (broker *baseBroker) Close() error {
	return broker.closef()
}

func (broker *baseBroker) WriteBody(
	ctx context.Context,
	bodyType BodyType,
	bodyLength uint64,
	body io.Reader,
) error {
	if err := broker.writeBody(ctx, bodyType, bodyLength, body); err != nil {
		return errors.WithMessage(err, "write body")
	}

	if bodyType == StreamBodyType {
		// NOTE stream body type will close broker
		if err := broker.Close(); err != nil {
			return errors.WithMessage(err, "close broker")
		}
	}

	return nil
}

func (broker *baseBroker) ReadBody(ctx context.Context) (
	bodyType BodyType,
	bodyLength uint64,
	body io.Reader, // NOTE response data body
	enc encoder.Encoder,
	res ResponseHeader,
	_ error,
) {
	err := func() error {
		switch dataType, err := broker.readDataType(ctx); {
		case err != nil:
			return err
		case dataType == BodyDataType:
		case dataType == ResponseHeaderDataType:
			switch i, header, err := broker.readHead(ctx, dataType); {
			case err != nil:
				return err
			default:
				enc = i
				res = header.(ResponseHeader) //nolint:forcetypeassert //...

				return nil
			}
		default:
			return errors.Errorf("expected body data type, but %v", dataType)
		}

		return nil
	}()
	if err != nil {
		return bodyType, bodyLength, body, enc, res, errors.WithMessage(err, "read body")
	}

	if res != nil {
		return bodyType, bodyLength, body, enc, res, nil
	}

	bodyType, bodyLength, body, res, err = broker.readBody(ctx)

	return bodyType, bodyLength, body, enc, res, errors.WithMessage(err, "read body")
}

func (broker *baseBroker) ReadBodyErr(ctx context.Context) (
	bodyType BodyType,
	bodyLength uint64,
	body io.Reader,
	_ error,
) {
	switch bodyType, bodyLength, body, _, res, err := broker.ReadBody(ctx); {
	case err != nil:
		return bodyType, bodyLength, body, err
	case res == nil:
		return bodyType, bodyLength, body, nil
	case res.Err() != nil:
		return bodyType, bodyLength, body, res.Err()
	default:
		return bodyType, bodyLength, body, errors.Errorf("error response")
	}
}

func (broker *baseBroker) writeHead(ctx context.Context, dataType DataType, header Header) error {
	if err := dataType.IsValid(nil); err != nil {
		return errors.Errorf("unknown data type, %d", dataType)
	}

	if header == nil {
		return errors.Errorf("empty header")
	}

	var headerb []byte

	switch b, err := broker.Encoder.Marshal(header); {
	case err != nil:
		return err
	default:
		headerb = b
	}

	if _, err := broker.write(ctx, dataType[:]); err != nil {
		return errors.WithMessage(err, "data type")
	}

	if err := broker.writeLengthed(ctx, broker.Encoder.Hint().Bytes()); err != nil {
		return errors.WithMessage(err, "encoder hint")
	}

	if err := broker.writeLengthed(ctx, headerb); err != nil {
		return errors.WithMessage(err, "header bytes")
	}

	return nil
}

func (broker *baseBroker) writeBody(
	ctx context.Context,
	bodyType BodyType,
	bodyLength uint64,
	body io.Reader,
) error {
	if err := bodyType.IsValid(nil); err != nil {
		return errors.WithMessagef(err, "unknown body type, %d", bodyType)
	}

	bd := body

	switch {
	case bodyType == FixedLengthBodyType && bodyLength > 0 && bd == nil:
		return errors.Errorf("fixed length type and body length > 0, but nil body reader")
	case bodyType == EmptyBodyType:
		bd = nil
	case bodyType == StreamBodyType && bd == nil:
		return errors.Errorf("stream type, but nil body reader")
	}

	if _, err := broker.write(ctx, BodyDataType[:]); err != nil {
		return errors.WithMessage(err, "data type")
	}

	if _, err := broker.write(ctx, bodyType[:]); err != nil {
		return errors.WithMessage(err, "body type")
	}

	switch bodyType {
	case EmptyBodyType:
	case FixedLengthBodyType:
		if err := broker.writeLength(ctx, bodyLength); err != nil {
			return errors.WithMessage(err, "body length")
		}
	case StreamBodyType:
	default:
		return errors.Errorf("unknown body type, %d", bodyType)
	}

	if bd != nil {
		if _, err := broker.writeReader(ctx, bd); err != nil {
			return errors.WithMessage(err, "write body reader")
		}
	}

	return nil
}

func (broker *baseBroker) readDataType(ctx context.Context) (dataType DataType, _ error) {
	if err := func() error {
		if _, err := broker.read(ctx, dataType[:]); err != nil {
			return err
		}

		return dataType.IsValid(nil)
	}(); err != nil {
		return UnknownDataType, broker.closeByError(errors.WithMessage(err, "read data type"))
	}

	return dataType, nil
}

func (broker *baseBroker) readBodyType(ctx context.Context) (bodyType BodyType, _ error) {
	if err := func() error {
		switch _, err := broker.read(ctx, bodyType[:]); {
		case err == nil, errors.Is(err, io.EOF):
		default:
			return err
		}

		return bodyType.IsValid(nil)
	}(); err != nil {
		return UnknownBodyType, broker.closeByError(errors.WithMessage(err, "read body type"))
	}

	return bodyType, nil
}

func (broker *baseBroker) readEncoder(ctx context.Context) (encoder.Encoder, error) {
	var hintb []byte

	switch b, err := broker.readLengthed(ctx); {
	case err != nil:
		return nil, broker.closeByError(errors.WithMessage(err, "encoder hint"))
	default:
		hintb = b
	}

	encht, err := hint.ParseHint(string(hintb))
	if err != nil {
		return nil, errors.WithMessage(err, "encoder hint")
	}

	switch enc := broker.Encoders.Find(encht); {
	case enc == nil:
		return nil, util.ErrNotFound.Errorf("encoder not found for %q", encht)
	default:
		return enc, nil
	}
}

func (broker *baseBroker) readHead(
	ctx context.Context,
	dataType DataType,
) (enc encoder.Encoder, header Header, _ error) {
	switch dataType {
	case RequestHeaderDataType:
	case ResponseHeaderDataType:
	default:
		return nil, nil, errors.Errorf("expected header data type, but %v", dataType)
	}

	switch i, err := broker.readEncoder(ctx); {
	case err != nil:
		return nil, nil, err
	default:
		enc = i
	}

	switch b, err := broker.readLengthed(ctx); {
	case err != nil && !errors.Is(err, io.EOF):
		return nil, nil, errors.WithMessage(err, "header")
	default:
		if err := encoder.Decode(enc, b, &header); err != nil {
			return nil, nil, errors.WithMessage(err, "header")
		}
	}

	switch dataType {
	case RequestHeaderDataType:
		if _, ok := header.(RequestHeader); !ok {
			return nil, nil, errors.Errorf("expected request header, but %T", header)
		}
	case ResponseHeaderDataType:
		if _, ok := header.(ResponseHeader); !ok {
			return nil, nil, errors.Errorf("expected response header, but %T", header)
		}
	default:
		return nil, nil, errors.Errorf("expected header, but %v", header)
	}

	return enc, header, nil
}

func (broker *baseBroker) readBody(ctx context.Context) (
	bodyType BodyType,
	bodyLength uint64,
	body io.Reader, // NOTE response data body
	res ResponseHeader,
	_ error,
) {
	err := func() error {
		var isEOF bool

		switch i, err := broker.readBodyType(ctx); {
		case err == nil, errors.Is(err, io.EOF):
			bodyType = i
			isEOF = errors.Is(err, io.EOF)
		default:
			return errors.WithMessage(err, "read body type")
		}

		switch bodyType {
		case EmptyBodyType:
		case FixedLengthBodyType:
			if isEOF {
				return errors.Errorf("read fixed body length; eof")
			}

			switch i, err := broker.readLength(ctx); {
			case err != nil && !errors.Is(err, io.EOF):
				return errors.WithMessage(err, "read fixed body length")
			default:
				bodyLength = i
				isEOF = errors.Is(err, io.EOF)

				body = &bytes.Buffer{}

				if bodyLength > 0 && !isEOF {
					body = io.NewSectionReader(readerAt{broker.Reader}, 0, int64(bodyLength))
				}
			}
		case StreamBodyType:
			body = broker.Reader

			if isEOF {
				body = &bytes.Buffer{}
			}
		default:
			return errors.Errorf("unknown body type, %d", bodyType)
		}

		return nil
	}()

	return bodyType, bodyLength, body, res, err
}

func (broker *baseBroker) write(_ context.Context, b []byte) (int, error) {
	i, err := util.EnsureWrite(broker.Writer, b)

	return i, broker.closeIfError(err)
}

func (broker *baseBroker) writeLength(_ context.Context, i uint64) error {
	return util.WriteLength(broker.Writer, i)
}

func (broker *baseBroker) writeLengthed(_ context.Context, b []byte) error {
	return util.WriteLengthed(broker.Writer, b)
}

func (broker *baseBroker) writeReader(_ context.Context, r io.Reader) (int64, error) {
	i, err := io.Copy(broker.Writer, r)

	return i, broker.closeIfError(err)
}

func (broker *baseBroker) read(_ context.Context, p []byte) (int, error) {
	i, err := broker.Reader.Read(p)

	return i, broker.closeIfError(err)
}

func (broker *baseBroker) readLength(context.Context) (uint64, error) {
	i, err := util.ReadLength(broker.Reader)

	return i, broker.closeIfError(err)
}

func (broker *baseBroker) readLengthed(context.Context) ([]byte, error) {
	b, err := util.ReadLengthed(broker.Reader)

	return b, broker.closeIfError(err)
}

func (broker *baseBroker) closeByError(err error) error {
	if errors.Is(err, io.EOF) {
		err = quicstream.ErrNetwork.Wrap(err) //revive:disable-line:modifies-parameter
	}

	if err != nil {
		_ = broker.Close()
	}

	return errors.WithStack(err)
}

func (broker *baseBroker) closeIfError(err error) error {
	switch {
	case errors.Is(err, io.EOF):
		err = quicstream.ErrNetwork.Wrap(err) //revive:disable-line:modifies-parameter
	case
		quicstream.IsNetworkError(err),
		errors.Is(err, context.Canceled),
		errors.Is(err, context.DeadlineExceeded):
		_ = broker.Close()
	}

	return errors.WithStack(err)
}

type readerAt struct {
	io.Reader
}

func (r readerAt) ReadAt(p []byte, _ int64) (int, error) {
	n, err := r.Read(p)

	return n, err //nolint:wrapcheck //...
}
