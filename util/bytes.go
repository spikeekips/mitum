package util

import (
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

func LengthedBytes(w io.Writer, b []byte) error { // FIXME set to network handler
	e := StringErrorFunc("failed to write LengthedBytes")

	i := uint64(len(b))

	if _, err := w.Write(Uint64ToBytes(i)); err != nil {
		return e(err, "")
	}

	if i < 1 {
		return nil
	}

	if _, err := w.Write(b); err != nil {
		return e(err, "")
	}

	return nil
}

func ReadLengthedBytes(b []byte) (_ []byte, left []byte, _ error) {
	i := uint64(len(b))

	if i < 8 { //nolint:gomnd //...
		return nil, nil, errors.Errorf("wrong format; missing length part")
	}

	j, err := BytesToUint64(b[:8])
	if err != nil {
		return nil, nil, errors.Errorf("wrong format; invalid length part")
	}

	if i-8 < j {
		return nil, nil, errors.Errorf("wrong format; left not enough")
	}

	return b[8 : j+8], b[j+8:], nil
}
