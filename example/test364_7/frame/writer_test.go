package frame

import (
	"bytes"
	"io"
	"testing"

	"github.com/stretchr/testify/suite"
	"go.uber.org/goleak"
)

type testFramesWriter struct {
	suite.Suite
}

func (t *testFramesWriter) newWriter(chunkLength uint16) (
	*bytes.Buffer,
	writeFunc,
	func(error) error,
) {
	buf := bytes.NewBuffer(nil)

	fw, ew := NewFramesWriterWithChunkLength(buf, chunkLength)

	return buf, fw, ew
}

func (t *testFramesWriter) TestWrite() {
	t.Run("empty", func() {
		buf, _, ew := t.newWriter(3)

		err := ew(nil)
		t.NoError(err)

		t.Equal(0, buf.Len())

		err = ew(nil)
		t.ErrorIs(err, ErrClosed)
	})

	t.Run("ok", func() {
		var chunkLength uint16 = 3

		buf, fw, ew := t.newWriter(chunkLength)

		b0 := sampleBytes(7)
		b1 := sampleBytes(8)

		n, err := fw(b0)
		t.NoError(err)
		t.Equal(len(b0), n)

		n, err = fw(b1)
		t.NoError(err)
		t.Equal(len(b1), n)

		t.NoError(ew(nil))

		r := bytes.NewReader(buf.Bytes())

		// read 1st frame
		ft, n, err := readFrameType(r)
		t.NoError(err)
		t.Equal(1, n)
		t.Equal(streamFrameType, ft)

		p0 := make([]byte, len(b0))
		b, _, err := readChunk(r)
		t.NoError(err)
		copy(p0, b)
		b, _, err = readChunk(r)
		t.NoError(err)
		copy(p0[3:], b)
		b, _, err = readChunk(r)
		t.NoError(err)
		copy(p0[6:], b)

		t.Equal(b0, p0)

		b, _, err = readChunk(r) // eof
		t.NoError(err)
		t.Empty(b)

		// read 2nd frame
		ft, n, err = readFrameType(r)
		t.NoError(err)
		t.Equal(1, n)
		t.Equal(streamFrameType, ft)

		p1 := make([]byte, len(b1))
		b, _, err = readChunk(r)
		t.NoError(err)
		copy(p1, b)
		b, _, err = readChunk(r)
		t.NoError(err)
		copy(p1[3:], b)
		b, _, err = readChunk(r)
		t.NoError(err)
		copy(p1[6:], b)

		t.Equal(b1, p1)

		b, _, err = readChunk(r) // eof
		t.NoError(err)
		t.Empty(b)

		// read end(error) frame
		ft, n, err = readFrameType(r)
		// t.NoError(err)
		t.Equal(1, n)
		t.Equal(errorFrameType, ft)

		b, _, err = readChunk(r) // eof
		t.NoError(err)
		t.Empty(b)

		_, _, err = readChunk(r)
		t.ErrorIs(err, io.EOF)
	})
}

func TestFramesWriter(t *testing.T) {
	defer goleak.VerifyNone(t)

	suite.Run(t, new(testFramesWriter))
}
