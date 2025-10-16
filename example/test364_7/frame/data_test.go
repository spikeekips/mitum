package frame

import (
	"bytes"
	"io"
	"math"
	"slices"
	"testing"

	"github.com/stretchr/testify/suite"
	"go.uber.org/goleak"
)

type eofReader struct {
	*bytes.Reader
}

func newEOFReader(b []byte) eofReader {
	return eofReader{Reader: bytes.NewReader(b)}
}

func (r eofReader) Read(p []byte) (int, error) {
	switch n, err := r.Reader.Read(p); {
	case err != nil:
		return n, err
	case r.Len() < 1:
		return n, io.EOF
	default:
		return n, err
	}
}

func TestRWLength(tt *testing.T) {
	t := new(suite.Suite)
	t.SetT(tt)

	buf := bytes.NewBuffer(nil)

	t.Run("write", func() {
		var u int64 = 64

		n, err := writeDataLength(buf, u)
		t.NoError(err)
		t.Equal(8, n)
		t.Equal([]byte{0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x40}, buf.Bytes())
	})

	t.Run("read", func() {
		u, n, err := readDataLength(bytes.NewReader(buf.Bytes()))
		t.NoError(err)
		t.Equal(8, n)
		t.EqualValues(64, u)
	})

	t.Run("eof", func() {
		u, n, err := readDataLength(newEOFReader(buf.Bytes()))
		t.NoError(err)
		t.Equal(8, n)
		t.EqualValues(64, u)
	})
}

type testRWBytes struct {
	suite.Suite
}

func (t *testRWBytes) TestCases() {
	cases := []struct {
		name string
		b    []byte
	}{
		{name: "empty", b: nil},
	}

	buf := bytes.NewBuffer(nil)

	for c := range slices.Values(cases) {
		buf.Reset()

		t.Run(c.name, func() {
			n, err := WriteBytes(buf, c.b)
			if err != nil {
				panic(err)
			}

			t.Equal(len(c.b), n)

			l, lr, err := ReadBytes(bytes.NewReader(buf.Bytes()))
			t.NoError(err)
			t.EqualValues(len(c.b), l)

			if l > 0 {
				p := make([]byte, l)

				n, err = lr.Read(p)
				t.NoError(err)
				t.EqualValues(len(c.b), n)
				t.Equal(c.b, p)
			}
		})
	}
}

func TestRWBytes(t *testing.T) {
	defer goleak.VerifyNone(t)

	suite.Run(t, new(testRWBytes))
}

type testRWUint64 struct {
	suite.Suite
}

func (t *testRWUint64) TestEOF() {
	buf := bytes.NewBuffer(nil)
	defer buf.Reset()

	n, err := WriteUint64(buf, 9)
	t.NoError(err)

	ur, n, err := ReadUint64(newEOFReader(buf.Bytes()))
	t.NoError(err)
	t.Equal(8, n)
	t.EqualValues(9, ur)
}

func (t *testRWUint64) TestReadCases() {
	cases := []struct {
		name     string
		b        []byte
		expected uint64
		err      string
	}{
		{name: "0", b: []byte{0, 0, 0, 0, 0, 0, 0, 0}, expected: 0},
		{name: "1", b: []byte{0, 0, 0, 0, 0, 0, 0, 1}, expected: 1},
		{name: "not enough", b: []byte{0, 0, 0, 0, 0, 0, 0}, err: "unexpected EOF"},
		{name: "more", b: []byte{0, 0, 0, 0, 0, 0, 0, 9, 1}, expected: 9},
	}

	for c := range slices.Values(cases) {
		r := newEOFReader(c.b)

		t.Run(c.name, func() {
			u, n, err := ReadUint64(r)
			switch {
			case err != nil:
				t.Equal(c.err, err.Error())

				return
			default:
				if c.err != "" {
					t.Equal(c.err, "")

					return
				}
			}

			t.Equal(8, n)

			t.Equal(c.expected, u)
		})
	}
}

func (t *testRWUint64) TestCases() {
	cases := []struct {
		name      string
		u         uint64
		expectedB []byte
		err       string
	}{
		{name: "0", u: 0, expectedB: []byte{0, 0, 0, 0, 0, 0, 0, 0}},
		{name: "1", u: 1, expectedB: []byte{0, 0, 0, 0, 0, 0, 0, 1}},
		{name: "max uint64", u: math.MaxUint64, expectedB: []byte{0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff}},
	}

	buf := bytes.NewBuffer(nil)

	for c := range slices.Values(cases) {
		buf.Reset()

		t.Run(c.name, func() {
			n, err := WriteUint64(buf, c.u)
			switch {
			case err != nil:
				t.Equal(c.err, err.Error())

				return
			default:
				if c.err != "" {
					t.Equal(c.err, "")

					return
				}
			}

			t.Equal(8, n)

			bw := buf.Bytes()
			t.Equal(c.expectedB, bw)

			ur, n, err := ReadUint64(bytes.NewReader(bw))
			t.NoError(err)
			t.Equal(8, n)
			t.Equal(c.u, ur)
		})
	}
}

func TestRWUint64(t *testing.T) {
	defer goleak.VerifyNone(t)

	suite.Run(t, new(testRWUint64))
}
