package util

import (
	"bytes"
	"testing"

	"github.com/stretchr/testify/suite"
)

type testLengthedBytes struct {
	suite.Suite
}

func (t *testLengthedBytes) TestNew() {
	t.Run("nil", func() {
		w := bytes.NewBuffer(nil)
		t.NoError(LengthedBytes(w, nil))

		b := w.Bytes()

		t.Equal(8, len(b))

		l, err := BytesToUint64(b)
		t.NoError(err)
		t.Equal(uint64(0), l)
	})

	t.Run("empty", func() {
		w := bytes.NewBuffer(nil)
		t.NoError(LengthedBytes(w, []byte{}))

		b := w.Bytes()

		t.Equal(8, len(b))

		l, err := BytesToUint64(b)
		t.NoError(err)
		t.Equal(uint64(0), l)
	})

	t.Run("1", func() {
		w := bytes.NewBuffer(nil)
		t.NoError(LengthedBytes(w, []byte{0x00}))

		b := w.Bytes()

		t.Equal(9, len(b))

		l, err := BytesToUint64(b)
		t.NoError(err)
		t.Equal(uint64(1), l)
		t.Equal([]byte{0x00}, b[8:])
	})

	t.Run("uuid", func() {
		w := bytes.NewBuffer(nil)
		i := UUID().Bytes()

		t.NoError(LengthedBytes(w, i))

		b := w.Bytes()

		t.Equal(8+len(i), len(b))

		l, err := BytesToUint64(b)
		t.NoError(err)
		t.Equal(uint64(len(i)), l)
		t.Equal(i, b[8:])
	})
}

func (t *testLengthedBytes) TestRead() {
	t.Run("nil", func() {
		w := bytes.NewBuffer(nil)
		t.NoError(LengthedBytes(w, nil))

		b := w.Bytes()

		r, rleft, err := ReadLengthedBytes(b)
		t.NoError(err)
		t.Equal(0, len(r))
		t.Equal(0, len(rleft))
	})

	t.Run("invalid legth; nil", func() {
		_, _, err := ReadLengthedBytes(nil)
		t.Error(err)
		t.ErrorContains(err, "missing length part")
	})

	t.Run("invalid legth", func() {
		_, _, err := ReadLengthedBytes(UUID().Bytes())
		t.Error(err)
		t.ErrorContains(err, "left not enough")
	})

	t.Run("empty", func() {
		w := bytes.NewBuffer(nil)
		t.NoError(LengthedBytes(w, []byte{}))

		b := w.Bytes()

		r, rleft, err := ReadLengthedBytes(b)
		t.NoError(err)
		t.Equal(0, len(r))
		t.Equal(0, len(rleft))
	})

	t.Run("1", func() {
		w := bytes.NewBuffer(nil)
		t.NoError(LengthedBytes(w, []byte{0x01}))

		b := w.Bytes()

		r, rleft, err := ReadLengthedBytes(b)
		t.NoError(err)
		t.Equal(1, len(r))
		t.Equal(0, len(rleft))
	})

	t.Run("empty and none empty left", func() {
		w := bytes.NewBuffer(nil)
		t.NoError(LengthedBytes(w, []byte{}))

		b := w.Bytes()

		left := UUID().Bytes()
		b = append(b, left...)

		r, rleft, err := ReadLengthedBytes(b)
		t.NoError(err)
		t.Equal(0, len(r))
		t.Equal(left, rleft)
	})

	t.Run("none empty and none empty left", func() {
		i := UUID().Bytes()
		w := bytes.NewBuffer(nil)
		t.NoError(LengthedBytes(w, i))

		b := w.Bytes()

		left := UUID().Bytes()
		b = append(b, left...)

		r, rleft, err := ReadLengthedBytes(b)
		t.NoError(err)
		t.Equal(i, r)
		t.Equal(left, rleft)
	})
}

func TestLengthedBytes(t *testing.T) {
	suite.Run(t, new(testLengthedBytes))
}
