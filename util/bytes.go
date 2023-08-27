package util

import (
	"bytes"
	"context"
	"io"
	"math"

	"github.com/pkg/errors"
)

const (
	maxLengthBytes   = math.MaxInt16
	maxLengthedBytes = math.MaxInt32
)

type Byter interface {
	Bytes() []byte
}

func ConcatByters(bs ...Byter) []byte {
	b := make([][]byte, len(bs))

	for i := range bs {
		j := bs[i]
		if j == nil {
			continue
		}

		b[i] = j.Bytes()
	}

	return ConcatBytesSlice(b...)
}

func ConcatByterSlice[T Byter](bs []T) []byte {
	b := make([][]byte, len(bs))

	for i := range bs {
		j := bs[i]
		if (interface{})(j) == nil { //nolint:govet //...
			continue
		}

		b[i] = j.Bytes()
	}

	return ConcatBytesSlice(b...)
}

type BytesToByter []byte

func (b BytesToByter) Bytes() []byte {
	return b
}

type DummyByter func() []byte

func (d DummyByter) Bytes() []byte {
	return d()
}

func ConcatBytesSlice(sl ...[]byte) []byte {
	var t int

	for i := range sl {
		j := sl[i]
		if j == nil {
			continue
		}

		t += len(j)
	}

	if t < 1 {
		return nil
	}

	n := make([]byte, t)
	var j int

	for i := range sl {
		k := sl[i]
		if k == nil {
			continue
		}

		j += copy(n[j:], k)
	}

	return n
}

func EnsureRead(ctx context.Context, r io.Reader, b []byte) (n uint64, _ error) {
	if len(b) < 1 {
		return 0, nil
	}

	readch := make(chan [3]interface{})

	read := func() {
		l := make([]byte, uint64(len(b))-n)

		i, err := r.Read(l)

		readch <- [3]interface{}{i, err, l[:i]}
	}

	go read()

	for {
		select {
		case <-ctx.Done():
			return n, errors.WithStack(ctx.Err())
		case j, notclosed := <-readch:
			if !notclosed {
				return n, errors.Errorf("readch closed")
			}

			var err error
			if e, ok := j[1].(error); ok {
				err = e
			}

			i := j[0].(int) //nolint:forcetypeassert //...
			iseof := errors.Is(err, io.EOF)

			if err != nil && !iseof {
				n += uint64(i)

				return n, errors.WithStack(err)
			}

			l := j[2].([]byte) //nolint:forcetypeassert //...

			if i > 0 {
				copy(b[n:], l)

				n += uint64(i)
			}

			switch {
			case n == uint64(len(b)):
				return n, errors.WithStack(err)
			case iseof:
				return n, errors.Errorf("insufficient read")
			default:
				go read()
			}
		}
	}
}

func EnsureWrite(w io.Writer, b []byte) (int, error) {
	switch n, err := w.Write(b); {
	case err != nil:
		return n, errors.WithStack(err)
	case n != len(b):
		return n, errors.Errorf("write")
	default:
		return n, nil
	}
}

func WriteLength(w io.Writer, i uint64) error {
	e := StringError("LengthedBytes")

	if _, err := w.Write(Uint64ToBytes(i)); err != nil {
		return e.WithMessage(err, "write length")
	}

	return nil
}

func WriteLengthed(w io.Writer, b []byte) error {
	e := StringError("LengthedBytes")

	i := uint64(len(b))

	if _, err := w.Write(Uint64ToBytes(i)); err != nil {
		return e.WithMessage(err, "write length")
	}

	if i < 1 {
		return nil
	}

	if _, err := w.Write(b); err != nil {
		return e.WithMessage(err, "write body")
	}

	return nil
}

func ReadLengthedBytes(b []byte) (_ []byte, left []byte, _ error) {
	i, err := ReadLengthBytes(b)
	if err != nil {
		return nil, nil, err
	}

	if uint64(len(b)-8) < i { //nolint:gomnd //...
		return nil, nil, errors.Errorf("wrong format; left not enough")
	}

	return b[8 : i+8], b[i+8:], nil
}

func ReadLengthBytes(b []byte) (uint64, error) {
	i := uint64(len(b))

	if i < 8 { //nolint:gomnd //...
		return 0, errors.Errorf("wrong format; missing length part")
	}

	j, err := BytesToUint64(b[:8])
	if err != nil {
		return 0, errors.Errorf("wrong format; invalid length part")
	}

	return j, nil
}

func ReadLength(r io.Reader) (read uint64, length uint64, _ error) {
	p := make([]byte, 8)

	switch n, err := EnsureRead(context.Background(), r, p); {
	case err == nil, errors.Is(err, io.EOF):
		i, err := ReadLengthBytes(p) //nolint:govet //...

		return n, i, err
	default:
		return n, 0, err
	}
}

func ReadLengthed(r io.Reader) (read uint64, _ []byte, _ error) {
	n, i, err := ReadLength(r)
	if err != nil {
		return n, nil, err
	}

	if i < 1 {
		return n, nil, nil
	}

	if i > maxLengthedBytes {
		return n, nil, errors.Errorf("huge size, %v", i)
	}

	p := make([]byte, i)

	m, err := EnsureRead(context.Background(), r, p)

	return n + m, p, err
}

func NewLengthedBytesSlice(m [][]byte) ([]byte, error) {
	w := bytes.NewBuffer(nil)
	defer w.Reset()

	if err := WriteLengthedSlice(w, m); err != nil {
		return nil, err
	}

	return w.Bytes(), nil
}

func WriteLengthedSlice(w io.Writer, m [][]byte) error {
	e := StringError("WriteLengthedBytesSlice")

	if _, err := w.Write(Uint64ToBytes(uint64(len(m)))); err != nil {
		return e.WithMessage(err, "write body length")
	}

	for i := range m {
		if err := WriteLengthed(w, m[i]); err != nil {
			return e.WithMessage(err, "write body")
		}
	}

	return nil
}

func ReadLengthedBytesSlice(b []byte) (m [][]byte, left []byte, _ error) {
	e := StringError("ReadLengthedBytesSlice")

	if len(b) < 8 { //nolint:gomnd //...
		return nil, nil, e.Errorf("empty m length")
	}

	switch i, err := ReadLengthBytes(b); {
	case err != nil:
		return nil, nil, e.WithMessage(err, "wrong m length")
	case i > maxLengthBytes:
		return nil, nil, errors.Errorf("huge size, %v", i)
	default:
		m = make([][]byte, i)

		left = b[8:]
	}

	for i := range m {
		j, k, err := ReadLengthedBytes(left)
		if err != nil {
			return nil, nil, e.WithMessage(err, "read left")
		}

		m[i] = j

		left = k
	}

	return m, left, nil
}

func ReadLengthedSlice(r io.Reader) (read uint64, hs [][]byte, _ error) {
	switch n, i, err := ReadLength(r); {
	case err == nil, errors.Is(err, io.EOF):
		if i > 0 && errors.Is(err, io.EOF) {
			return n, nil, errors.Wrap(err, "insufficient read")
		}

		if i < 1 {
			return n, nil, nil
		}

		if i > maxLengthBytes {
			return n, nil, errors.Errorf("huge size, %v", i)
		}

		hs = make([][]byte, i)
		read += n
	default:
		return n, nil, errors.Wrap(err, "bytes length")
	}

	for i := range hs {
		switch n, b, err := ReadLengthed(r); {
		case errors.Is(err, io.EOF) && i < len(hs)-1:
			return read, nil, errors.Wrap(err, "insufficient read")
		case err == nil, errors.Is(err, io.EOF):
			hs[i] = b
			read += n
		default:
			return read, nil, errors.Wrap(err, "read bytes")
		}
	}

	return read, hs, nil
}

var bytesFrameVersion = [2]byte{0x00, 0x00}

type BytesFrameWriter struct {
	w             io.Writer
	headerWritten bool
}

func NewBytesFrameWriter(w io.Writer) (*BytesFrameWriter, error) {
	if _, err := w.Write(bytesFrameVersion[:]); err != nil {
		return nil, errors.Wrap(err, "version")
	}

	return &BytesFrameWriter{w: w}, nil
}

func NewBufferBytesFrameWriter() (*BytesFrameWriter, *bytes.Buffer) {
	w := bytes.NewBuffer(nil)

	f, _ := NewBytesFrameWriter(w)

	return f, w
}

func (f *BytesFrameWriter) Header(bs ...[]byte) error {
	if f.headerWritten {
		return errors.Errorf("header already written")
	}

	defer func() {
		f.headerWritten = true
	}()

	if _, err := f.w.Write(Uint64ToBytes(uint64(len(bs)))); err != nil {
		return errors.Wrap(err, "[]bytes length")
	}

	for i := range bs {
		if err := WriteLengthed(f.w, bs[i]); err != nil {
			return errors.Wrap(err, "[]bytes")
		}
	}

	return nil
}

// Writer returns io.Writer. Writer is for not lengthed body.
func (f *BytesFrameWriter) Writer() io.Writer {
	defer func() {
		f.headerWritten = true
	}()

	return f.w
}

// Lengthed writes bytes body with length. Lengthed can be used multiple times.
func (f *BytesFrameWriter) Lengthed(b []byte) error {
	defer func() {
		f.headerWritten = true
	}()

	return WriteLengthed(f.w, b)
}

type BytesFrameReader struct {
	r          io.Reader
	version    [2]byte
	headerRead bool
}

func NewBytesFrameReader(r io.Reader) (*BytesFrameReader, error) {
	var version [2]byte

	switch _, err := r.Read(version[:]); {
	case errors.Is(err, io.EOF):
	case err != nil:
		return nil, errors.Wrap(err, "version")
	}

	return &BytesFrameReader{
		r:       r,
		version: version,
	}, nil
}

func NewBufferBytesFrameReader(b []byte) (*BytesFrameReader, *bytes.Buffer, error) {
	r := bytes.NewBuffer(b)

	f, err := NewBytesFrameReader(r)
	if err != nil {
		r.Reset()

		return nil, nil, err
	}

	return f, r, nil
}

func NewBufferBytesNoHeadersFrameReader(b []byte) (*BytesFrameReader, *bytes.Buffer, error) {
	fr, buf, err := NewBufferBytesFrameReader(b)
	if err != nil {
		return nil, nil, err
	}

	fr.headerRead = true

	return fr, buf, nil
}

func (f *BytesFrameReader) Version() [2]byte {
	return f.version
}

func (f *BytesFrameReader) Header() ([][]byte, error) {
	if f.headerRead {
		return nil, errors.Errorf("header already read")
	}

	defer func() {
		f.headerRead = true
	}()

	_, hs, err := ReadLengthedSlice(f.r)

	return hs, err
}

func (f *BytesFrameReader) Reader() io.Reader {
	return f.r
}

func (f *BytesFrameReader) BodyReader() (io.Reader, error) {
	if err := f.exhaustHeader(); err != nil {
		return nil, err
	}

	return f.r, nil
}

func (f *BytesFrameReader) Lengthed(read func([]byte) error) error {
	if err := f.exhaustHeader(); err != nil {
		return err
	}

	switch n, i, err := ReadLengthed(f.r); {
	case err == nil, errors.Is(err, io.EOF):
		if n < 1 && errors.Is(err, io.EOF) {
			return nil
		}

		if read != nil {
			if rerr := read(i); rerr != nil {
				return rerr
			}
		}

		return nil
	default:
		return err
	}
}

func (f *BytesFrameReader) exhaustHeader() error {
	if f.headerRead {
		return nil
	}

	defer func() {
		f.headerRead = true
	}()

	_, _, err := ReadLengthedSlice(f.r)

	return err
}
