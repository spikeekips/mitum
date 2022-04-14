package util

import (
	"bytes"
	"compress/gzip"
	"crypto/sha256"
	"io"
	"strings"
	"testing"

	"github.com/pkg/errors"
	"github.com/stretchr/testify/suite"
)

type testChecksumReader struct {
	suite.Suite
}

func (t *testChecksumReader) TestNew() {
	s := UUID().String()

	checksum := SHA256Checksum([]byte(s))

	r := io.NopCloser(strings.NewReader(s))
	cr := NewHashChecksumReader(r, sha256.New())

	_ = (interface{})(cr).(ChecksumReader)

	t.Run("readall", func() {
		us, err := io.ReadAll(cr)
		t.NoError(err)

		t.NoError(cr.Close())

		t.Equal(s, string(us))

		t.Equal(checksum, cr.Checksum())
	})

	t.Run("read after close", func() {
		p := make([]byte, 1024)
		n, err := cr.Read(p)

		t.Equal(0, n)
		t.True(errors.Is(err, io.EOF))
	})
}

func (t *testChecksumReader) TestChecksumBeforeReadAll() {
	s := UUID().String()

	checksum := SHA256Checksum([]byte(s))

	r := io.NopCloser(strings.NewReader(s))
	cr := NewHashChecksumReader(r, sha256.New())
	defer cr.Close()

	p := make([]byte, 1)
	n, err := cr.Read(p)
	t.NoError(err)
	t.Equal(len(p), n)

	t.Equal(s[:1], string(p))

	t.NotEqual(checksum, checksumstring(cr.h))
	t.Equal(checksum, cr.Checksum())
}

func (t *testChecksumReader) TestDummyChecksumReader() {
	s := UUID().String()

	buf := bytes.NewBuffer(nil)

	gw := gzip.NewWriter(buf)
	_, err := io.Copy(gw, strings.NewReader(s))
	t.NoError(err)

	gw.Close()

	checksum := SHA256Checksum(buf.Bytes())

	cr := NewHashChecksumReader(io.NopCloser(buf), sha256.New())
	defer cr.Close()

	gr, err := NewGzipReader(cr)
	t.NoError(err)

	dummy := NewDummyChecksumReader(gr, cr)
	_ = (interface{})(dummy).(ChecksumReader)

	us, err := io.ReadAll(dummy)
	t.NoError(err)

	t.Equal(s, string(us))
	t.Equal(checksum, cr.Checksum())
}

func TestChecksumReader(t *testing.T) {
	suite.Run(t, new(testChecksumReader))
}
