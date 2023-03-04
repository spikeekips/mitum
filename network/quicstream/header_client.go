package quicstream

import (
	"bytes"
	"context"
	"io"
	"sync"

	"github.com/pkg/errors"
	"github.com/spikeekips/mitum/network"
	"github.com/spikeekips/mitum/util"
	"github.com/spikeekips/mitum/util/encoder"
	"github.com/spikeekips/mitum/util/hint"
)

type HeaderClientWriteFunc func(
	ctx context.Context,
	conninfo UDPConnInfo,
	writef ClientWriteFunc,
) (_ io.ReadCloser, cancel func() error, _ error)

type HeaderClient struct {
	Encoders  *encoder.Encoders
	Encoder   encoder.Encoder
	writeFunc HeaderClientWriteFunc
}

func NewHeaderClient(
	encs *encoder.Encoders,
	enc encoder.Encoder,
	writef HeaderClientWriteFunc,
) *HeaderClient {
	return &HeaderClient{
		Encoders:  encs,
		Encoder:   enc,
		writeFunc: writef,
	}
}

func (c *HeaderClient) RequestEncode(
	ctx context.Context,
	ci UDPConnInfo,
	header Header,
	i interface{},
) (
	ResponseHeader,
	io.ReadCloser,
	func() error,
	encoder.Encoder,
	error,
) {
	b, err := c.Encoder.Marshal(i)
	if err != nil {
		return nil, nil, util.EmptyCancelFunc, nil, err
	}

	return c.RequestBody(ctx, ci, header, bytes.NewBuffer(b))
}

func (c *HeaderClient) RequestDecode(
	ctx context.Context,
	ci UDPConnInfo,
	header Header,
	i, u interface{},
) (ResponseHeader, encoder.Encoder, error) {
	var body io.Reader

	if i != nil {
		buf := bytes.NewBuffer(nil)
		defer buf.Reset()

		if err := c.Encoder.StreamEncoder(buf).Encode(i); err != nil {
			return nil, nil, err
		}

		body = buf
	}

	return c.RequestBodyDecode(ctx, ci, header, body, u)
}

func (c *HeaderClient) RequestBody(
	ctx context.Context,
	ci UDPConnInfo,
	header Header,
	body io.Reader,
) (
	ResponseHeader,
	io.ReadCloser,
	func() error,
	encoder.Encoder,
	error,
) {
	e := util.StringErrorFunc("failed to request")

	var r io.ReadCloser
	cancel := util.EmptyCancelFunc

	switch i, j, err := c.write(ctx, ci, c.Encoder, header, body); {
	case err != nil:
		return nil, r, util.EmptyCancelFunc, nil, e(err, "failed to send request")
	default:
		r = i
		cancel = j
	}

	h, enc, err := c.readResponseHeader(ctx, r)

	switch {
	case err != nil:
		defer func() {
			_ = cancel()
		}()

		return h, r, util.EmptyCancelFunc, nil, e(err, "failed to read stream")
	case h == nil:
		return h, r, cancel, enc, nil
	case h.Err() != nil, !h.OK():
		return h, r, util.EmptyCancelFunc, enc, nil
	}

	switch h.ContentType() {
	case HinterContentType, RawContentType:
		return h, r, cancel, enc, nil
	default:
		defer func() {
			_ = cancel()
		}()

		return nil, r, util.EmptyCancelFunc, nil, errors.Errorf("unknown content type, %q", h.ContentType())
	}
}

func (c *HeaderClient) RequestBodyDecode(
	ctx context.Context,
	ci UDPConnInfo,
	header Header,
	body io.Reader,
	u interface{},
) (ResponseHeader, encoder.Encoder, error) {
	h, r, cancel, enc, err := c.RequestBody(ctx, ci, header, body)
	if err != nil {
		return nil, nil, err
	}

	defer func() {
		_ = cancel()
	}()

	if !h.OK() {
		return h, enc, nil
	}

	return h, enc, encoder.DecodeReader(enc, r, u)
}

func (c *HeaderClient) RequestBodyUnmarshal(
	ctx context.Context,
	ci UDPConnInfo,
	header Header,
	body io.Reader,
	u interface{},
) (ResponseHeader, encoder.Encoder, error) {
	h, r, cancel, enc, err := c.RequestBody(ctx, ci, header, body)
	if err != nil {
		return nil, nil, err
	}

	defer func() {
		_ = cancel()
	}()

	if !h.OK() {
		return h, enc, nil
	}

	b, err := io.ReadAll(r)
	if err != nil {
		return h, enc, errors.WithStack(err)
	}

	return h, enc, enc.Unmarshal(b, u)
}

func (c *HeaderClient) write(
	ctx context.Context,
	ci UDPConnInfo,
	enc encoder.Encoder,
	header Header,
	body io.Reader,
) (
	io.ReadCloser,
	func() error,
	error,
) {
	if header == nil {
		return nil, nil, errors.Errorf("empty header")
	}

	b, err := enc.Marshal(header)
	if err != nil {
		return nil, nil, err
	}

	var r io.ReadCloser
	cancel := util.EmptyCancelFunc

	donech := make(chan error, 1)

	go func() {
		var err error

		r, cancel, err = c.writeFunc(ctx, ci, func(w io.Writer) error {
			return clientWrite(w, header.Handler(), enc.Hint(), b, body)
		})

		donech <- errors.WithMessage(err, "failed to write")
	}()

	select {
	case <-ctx.Done():
		return nil, util.EmptyCancelFunc, ctx.Err()
	case err := <-donech:
		var once sync.Once

		cancelf := func() error {
			var e error

			once.Do(func() {
				if r != nil {
					if e = r.Close(); err != nil {
						return
					}
				}

				if cancel != nil {
					e = cancel()

					return
				}
			})

			return errors.WithStack(e)
		}

		if err != nil {
			if cancel != nil {
				_ = cancelf()
			}

			return r, util.EmptyCancelFunc, err
		}

		return r, cancelf, nil
	}
}

func (c *HeaderClient) readResponseHeader(
	ctx context.Context,
	r io.ReadCloser,
) (ResponseHeader, encoder.Encoder, error) {
	donech := make(chan error, 1)

	var h Header
	var enc encoder.Encoder

	go func() {
		var err error

		h, enc, err = readHeader(ctx, c.Encoders, r)

		donech <- err
	}()

	select {
	case <-ctx.Done():
		return nil, nil, ctx.Err()
	case err := <-donech:
		if err != nil {
			return nil, nil, err
		}
	}

	switch rh, ok := h.(ResponseHeader); {
	case !ok:
		return nil, nil, nil
	default:
		return rh, enc, nil
	}
}

func clientWrite(w io.Writer, prefix string, enchint hint.Hint, header []byte, body io.Reader) error {
	if err := WritePrefix(w, prefix); err != nil {
		return err
	}

	return writeTo(w, enchint, header, func(w io.Writer) error {
		if body != nil {
			if _, err := io.Copy(w, body); err != nil {
				return errors.WithStack(err)
			}
		}

		return nil
	})
}

func writeTo(
	w io.Writer,
	enchint hint.Hint,
	header []byte,
	writebodyf func(w io.Writer) error,
) error {
	if err := writeHint(w, enchint); err != nil {
		return err
	}

	if err := writeHeader(w, header); err != nil {
		return err
	}

	if writebodyf != nil {
		if err := writebodyf(w); err != nil {
			return err
		}
	}

	return nil
}

func writeHint(w io.Writer, ht hint.Hint) error {
	h := make([]byte, hint.MaxHintLength)
	copy(h, ht.Bytes())

	if _, err := ensureWrite(w, h); err != nil {
		return errors.Wrap(err, "failed to write hint")
	}

	return nil
}

func writeHeader(w io.Writer, header []byte) error {
	e := util.StringErrorFunc("failed to write header")

	l := util.Uint64ToBytes(uint64(len(header)))

	if _, err := ensureWrite(w, l); err != nil {
		return e(err, "")
	}

	if len(header) > 0 {
		if _, err := ensureWrite(w, header); err != nil {
			return e(err, "")
		}
	}

	return nil
}

func ensureWrite(w io.Writer, b []byte) (int, error) {
	switch n, err := w.Write(b); {
	case err != nil:
		return n, errors.WithStack(err)
	case n != len(b):
		return n, errors.Errorf("failed to write")
	default:
		return n, nil
	}
}

func readHeader(ctx context.Context, encs *encoder.Encoders, r io.Reader) (Header, encoder.Encoder, error) {
	enc, b, err := readHead(ctx, encs, r)
	if err != nil {
		return nil, nil, err
	}

	var h Header
	if err := encoder.Decode(enc, b, &h); err != nil {
		return h, enc, err
	}

	return h, enc, nil
}

func readHead(ctx context.Context, encs *encoder.Encoders, r io.Reader) (encoder.Encoder, []byte, error) {
	enc, err := readEncoder(ctx, encs, r)
	if err != nil {
		return nil, nil, err
	}

	b, err := readHeaderBytes(ctx, r)
	if err != nil {
		return enc, nil, err
	}

	return enc, b, nil
}

func readHint(ctx context.Context, r io.Reader) (ht hint.Hint, _ error) {
	e := util.StringErrorFunc("failed to read hint")

	b := make([]byte, hint.MaxHintLength)
	if n, err := network.EnsureRead(ctx, r, b); n != len(b) {
		return ht, e(err, "")
	}

	ht, err := hint.ParseHint(string(b))
	if err != nil {
		return ht, e(err, "")
	}

	return ht, nil
}

func readEncoder(ctx context.Context, encs *encoder.Encoders, r io.Reader) (encoder.Encoder, error) {
	e := util.StringErrorFunc("failed to read encoder")

	ht, err := readHint(ctx, r)
	if err != nil {
		return nil, e(err, "")
	}

	switch enc := encs.Find(ht); {
	case enc == nil:
		return nil, e(util.ErrNotFound.Errorf("encoder not found for %q", ht), "")
	default:
		return enc, nil
	}
}

func readHeaderBytes(ctx context.Context, r io.Reader) ([]byte, error) {
	l := make([]byte, 8)

	if n, err := network.EnsureRead(ctx, r, l); n != len(l) {
		return nil, errors.Wrap(err, "failed to read header length")
	}

	length, err := util.BytesToUint64(l)
	if err != nil {
		return nil, errors.Wrap(err, "failed to parse header length")
	}

	if length < 1 {
		return nil, nil
	}

	h := make([]byte, length)

	if n, err := network.EnsureRead(ctx, r, h); n != len(h) {
		return nil, errors.Wrap(err, "failed to read header")
	}

	return h, nil
}
