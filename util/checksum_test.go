package util

import (
	"bufio"
	"bytes"
	"crypto/sha256"
	"io"
	"strings"
	"testing"

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
		t.ErrorIs(err, io.EOF)
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

func TestChecksumReader(t *testing.T) {
	suite.Run(t, new(testChecksumReader))
}

type testChecksumWriter struct {
	suite.Suite
}

func (t *testChecksumWriter) TestFlush() {
	s := UUID().Bytes()

	checksum := SHA256Checksum(s)

	ow := bytes.NewBuffer(nil)
	bw := bufio.NewWriterSize(ow, len(s))

	cw := NewHashChecksumWriterWithWriter("showme", bw, sha256.New())
	_, err := cw.Write(s)
	t.NoError(err)
	cw.Close()

	t.Equal(checksum, cw.Checksum())

	t.Equal(s, ow.Bytes())
}

func TestChecksumWriter(t *testing.T) {
	suite.Run(t, new(testChecksumWriter))
}
