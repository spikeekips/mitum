package frame

import (
	"bytes"
	"io"
	"slices"
	"sync"
	"testing"

	"github.com/pkg/errors"
	"github.com/stretchr/testify/suite"
	"go.uber.org/goleak"
)

type testWriter struct {
	suite.Suite
}

func (t *testWriter) defaultWriter() (*bytes.Buffer, WriteCloserFunc) {
	buf := bytes.NewBuffer(nil)

	return buf, DefaultWriter(buf, dataFrameType)
}

func (t *testWriter) newWriter(chunkLength uint16) (*bytes.Buffer, WriteCloserFunc) {
	buf := bytes.NewBuffer(nil)

	return buf, NewWriterWithChunkLength(buf, dataFrameType, chunkLength)
}

func (t *testWriter) TestEmpty() {
	t.Run("once", func() {
		buf, w := t.defaultWriter()

		n, err := w.Write(nil)
		t.NoError(err)
		t.Equal(0, n)
		t.Empty(buf.Bytes())

		t.NoError(w.Close())
		t.Empty(buf.Bytes())
	})

	t.Run("more than once", func() {
		buf, w := t.defaultWriter()

		n, err := w.Write(nil)
		t.NoError(err)
		t.Equal(0, n)
		t.Empty(buf.Bytes())

		n, err = w.Write(nil)
		t.NoError(err)
		t.Equal(0, n)
		t.Empty(buf.Bytes())

		t.NoError(w.Close())
		t.Empty(buf.Bytes())
	})
}

func (t *testWriter) TestLengthJustRight() {
	buf, w := t.newWriter(3)

	b0 := []byte{0x01, 0x02, 0x03}

	n, err := w.Write(b0)
	t.NoError(err)
	t.Equal(len(b0), n)

	b1 := []byte{0x04, 0x05, 0x06}
	n, err = w.Write(b1)
	t.NoError(err)
	t.Equal(len(b1), n)

	t.NoError(w.Close())

	r := bytes.NewReader(buf.Bytes())

	// check frame type
	ft, n, err := readFrameType(r)
	t.NoError(err)
	t.Equal(dataFrameType, ft)
	t.Equal(1, n)

	// check b0 chunk length
	b0l, n, err := readLength(r)
	t.NoError(err)
	t.EqualValues(3, b0l)

	// check b0 chunk data
	b0r := make([]byte, len(b0))
	n, err = io.ReadFull(r, b0r)
	t.NoError(err)
	t.Equal(len(b0), n)
	t.Equal(b0, b0r)

	// check b1 chunk length
	b1l, n, err := readLength(r)
	t.NoError(err)
	t.EqualValues(3, b1l)

	// check b1 chunk data
	b1r := make([]byte, len(b1))
	n, err = io.ReadFull(r, b1r)
	t.NoError(err)
	t.Equal(len(b1), n)
	t.Equal(b1, b1r)

	// check eof chunk
	eofl, n, err := readLength(r)
	t.NoError(err)
	t.EqualValues(0, eofl)

	p := make([]byte, 1)
	n, err = io.ReadFull(r, p)
	t.ErrorIs(err, io.EOF)
	t.Equal(0, n)
}

func (t *testWriter) TestOverLength() {
	var chunkLength uint16 = 3

	buf, w := t.newWriter(chunkLength)

	b0 := []byte{0x01, 0x02, 0x03, 0x04}

	n, err := w.Write(b0)
	t.NoError(err)
	t.Equal(len(b0), n)

	b1 := []byte{0x05, 0x06, 0x07, 0x08}

	n, err = w.Write(b1)
	t.NoError(err)
	t.Equal(len(b1), n)

	t.NoError(w.Close())

	r := bytes.NewReader(buf.Bytes())

	// check frame type
	ft, n, err := readFrameType(r)
	t.NoError(err)
	t.Equal(dataFrameType, ft)
	t.Equal(1, n)

	// check 1st chunk length
	bl, n, err := readLength(r)
	t.NoError(err)
	t.EqualValues(chunkLength, bl)

	// check 1st chunk data
	br := make([]byte, bl)
	n, err = io.ReadFull(r, br)
	t.NoError(err)
	t.EqualValues(bl, n)
	t.Equal([]byte{0x01, 0x02, 0x03}, br)

	// check 2nd chunk length
	bl, n, err = readLength(r)
	t.NoError(err)
	t.EqualValues(1, bl)

	// check 2nd chunk data
	br = make([]byte, bl)
	n, err = io.ReadFull(r, br)
	t.NoError(err)
	t.EqualValues(bl, n)
	t.Equal([]byte{0x04}, br)

	// check 3rd chunk length
	bl, n, err = readLength(r)
	t.NoError(err)
	t.EqualValues(chunkLength, bl)

	// check 3rd chunk data
	br = make([]byte, bl)
	n, err = io.ReadFull(r, br)
	t.NoError(err)
	t.EqualValues(bl, n)
	t.Equal([]byte{0x05, 0x06, 0x07}, br)

	// check 4th chunk length
	bl, n, err = readLength(r)
	t.NoError(err)
	t.EqualValues(1, bl)

	// check 4th chunk data
	br = make([]byte, bl)
	n, err = io.ReadFull(r, br)
	t.NoError(err)
	t.EqualValues(bl, n)
	t.Equal([]byte{0x08}, br)

	// check eof chunk
	eofl, n, err := readLength(r)
	t.NoError(err)
	t.EqualValues(0, eofl)

	p := make([]byte, 1)
	n, err = io.ReadFull(r, p)
	t.ErrorIs(err, io.EOF)
	t.Equal(0, n)
}

func (t *testWriter) TestWriteAsync() {
	var wg sync.WaitGroup

	b := make([]byte, 256)
	for i := range b {
		b[i] = byte(i)
	}

	buf, w := t.newWriter(3)

	for i := 0; i < len(b); i += 5 {
		wg.Go(func() {
			if _, err := w.Write(b[i:min(i+5, len(b))]); err != nil {
				panic(err)
			}
		})
	}

	wg.Wait()
	t.NoError(w.Close())

	r := bytes.NewReader(buf.Bytes())

	// check frame type
	ft, n, err := readFrameType(r)
	t.NoError(err)
	t.Equal(dataFrameType, ft)
	t.Equal(1, n)

	bufr := bytes.NewBuffer(nil)

	for {
		b, n, err := readChunk(r)
		if err != nil {
			t.NoError(err)
		}
		if len(b) < 1 {
			break
		}

		t.True(n <= 5, n)

		_, _ = bufr.Write(b)
	}

	br := bufr.Bytes()
	slices.Sort(br)

	t.Equal(b, br)
}

func (t *testWriter) TestClose() {
	t.Run("close after close", func() {
		buf, w := t.defaultWriter()
		t.NoError(w.Close())

		err := w.Close()
		t.ErrorIs(err, ErrClosed)
		t.Empty(buf.Bytes())
	})

	t.Run("write nil after close", func() {
		buf, w := t.defaultWriter()
		t.NoError(w.Close())

		n, err := w.Write(nil)
		t.Equal(0, n)
		t.ErrorIs(err, ErrClosed)
		t.Empty(buf.Bytes())
	})

	t.Run("write after close", func() {
		buf, w := t.defaultWriter()
		t.NoError(w.Close())

		n, err := w.Write([]byte{0x00, 0x01})
		t.Equal(0, n)
		t.ErrorIs(err, ErrClosed)
		t.Empty(buf.Bytes())
	})
}

func TestWriter(t *testing.T) {
	defer goleak.VerifyNone(t)

	suite.Run(t, new(testWriter))
}

type testFramesWriter struct {
	suite.Suite
}

func (t *testFramesWriter) newWriter(chunkLength uint16) (
	*bytes.Buffer,
	WriterFunc,
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
		t.Equal(dataFrameType, ft)

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
		t.Equal(dataFrameType, ft)

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

func readChunk(r io.Reader) ([]byte, int, error) {
	var rn int

	switch l, n, err := readLength(r); {
	case err != nil:
		return nil, n, err
	case l < 1:
		return nil, n, nil
	default:
		rn += n

		b := make([]byte, 0, l)

		n, err = io.ReadFull(r, b[len(b):cap(b)])
		if err != nil && !errors.Is(err, io.EOF) {
			clear(b)
			b = nil

			return nil, rn + n, err
		}

		b = b[:len(b)+n]

		return b, rn + n, err
	}
}
