package util

import (
	"bytes"
	"context"
	"io"

	"github.com/pkg/errors"
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

	for {
		select {
		case <-ctx.Done():
			return n, errors.WithStack(ctx.Err())
		default:
			l := make([]byte, uint64(len(b))-n)

			i, err := r.Read(l)

			iseof := errors.Is(err, io.EOF)

			switch {
			case err == nil:
			case !iseof:
				n += uint64(i)

				return n, errors.WithStack(err)
			}

			n += uint64(i)

			copy(b[len(b)-len(l):], l)

			switch {
			case n == uint64(len(b)):
				return n, errors.WithStack(err)
			case iseof:
				return n, errors.Errorf("insufficient read")
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
