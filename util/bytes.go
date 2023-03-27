package util

import (
	"bytes"
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

func EnsureRead(r io.Reader, b []byte) (int, error) {
	if len(b) < 1 {
		return 0, nil
	}

	var n int

	for {
		l := make([]byte, len(b)-n)

		i, err := r.Read(l)

		switch {
		case err == nil:
		case !errors.Is(err, io.EOF):
			return n, errors.WithStack(err)
		}

		n += i

		copy(b[len(b)-len(l):], l)

		if n == len(b) || errors.Is(err, io.EOF) {
			return n, errors.WithStack(err)
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
	e := StringErrorFunc("LengthedBytes")

	if _, err := w.Write(Uint64ToBytes(i)); err != nil {
		return e(err, "write length")
	}

	return nil
}

func WriteLengthed(w io.Writer, b []byte) error {
	e := StringErrorFunc("LengthedBytes")

	i := uint64(len(b))

	if _, err := w.Write(Uint64ToBytes(i)); err != nil {
		return e(err, "write length")
	}

	if i < 1 {
		return nil
	}

	if _, err := w.Write(b); err != nil {
		return e(err, "write body")
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

func ReadLength(r io.Reader) (uint64, error) {
	p := make([]byte, 8)

	if _, err := EnsureRead(r, p); err != nil {
		return 0, err
	}

	return ReadLengthBytes(p)
}

func ReadLengthed(r io.Reader) ([]byte, error) {
	n, err := ReadLength(r)
	if err != nil {
		return nil, err
	}

	p := make([]byte, n)
	_, err = EnsureRead(r, p)

	return p, err
}

func NewLengthedBytesSlice(version byte, m [][]byte) ([]byte, error) {
	w := bytes.NewBuffer(nil)
	defer w.Reset()

	if err := WriteLengthedSlice(w, version, m); err != nil {
		return nil, err
	}

	return w.Bytes(), nil
}

func WriteLengthedSlice(w io.Writer, version byte, m [][]byte) error {
	e := StringErrorFunc("WriteLengthedBytesSlice")

	if err := WriteLengthed(w, []byte{version}); err != nil {
		return e(err, "write version")
	}

	if _, err := w.Write(Uint64ToBytes(uint64(len(m)))); err != nil {
		return e(err, "write body length")
	}

	for i := range m {
		if err := WriteLengthed(w, m[i]); err != nil {
			return e(err, "write body")
		}
	}

	return nil
}

func ReadLengthedBytesSlice(b []byte) (version byte, m [][]byte, left []byte, _ error) {
	e := StringErrorFunc("ReadLengthedBytesSlice")

	switch i, j, err := ReadLengthedBytes(b); {
	case err != nil:
		return version, nil, nil, e(err, "wrong version")
	case len(i) < 1:
		return version, nil, nil, e(err, "empty version")
	default:
		version = i[0]

		left = j
	}

	if len(left) < 8 { //nolint:gomnd //...
		return version, nil, nil, e(nil, "empty m length")
	}

	switch k, err := BytesToUint64(left[:8]); {
	case err != nil:
		return version, nil, nil, e(err, "wrong m length")
	default:
		m = make([][]byte, k)

		left = left[8:]
	}

	for i := range m {
		j, k, err := ReadLengthedBytes(left)
		if err != nil {
			return version, nil, nil, e(err, "read left")
		}

		m[i] = j

		left = k
	}

	return version, m, left, nil
}
