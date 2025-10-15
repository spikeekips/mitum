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

type testStreamWriter struct {
	suite.Suite
}

func (t *testStreamWriter) newWriter(chunkLength uint16) (*bytes.Buffer, writeFunc, func() error) {
	buf := bytes.NewBuffer(nil)

	fw, cw := newStreamWriterWithChunkLength(buf, streamFrameType, chunkLength)

	return buf, fw, cw
}

func (t *testStreamWriter) TestEmpty() {
	t.Run("once", func() {
		buf, fw, cw := t.newWriter(3)

		n, err := fw(nil)
		t.NoError(err)
		t.Equal(0, n)
		t.Empty(buf.Bytes())

		t.NoError(cw())
		t.Empty(buf.Bytes())
	})

	t.Run("more than once", func() {
		buf, fw, cw := t.newWriter(3)

		n, err := fw(nil)
		t.NoError(err)
		t.Equal(0, n)
		t.Empty(buf.Bytes())

		n, err = fw(nil)
		t.NoError(err)
		t.Equal(0, n)
		t.Empty(buf.Bytes())

		t.NoError(cw())
		t.Empty(buf.Bytes())
	})
}

func (t *testStreamWriter) TestLengthJustRight() {
	buf, fw, cw := t.newWriter(3)

	b0 := []byte{0x01, 0x02, 0x03}

	n, err := fw(b0)
	t.NoError(err)
	t.Equal(len(b0), n)

	b1 := []byte{0x04, 0x05, 0x06}
	n, err = fw(b1)
	t.NoError(err)
	t.Equal(len(b1), n)

	t.NoError(cw())

	r := bytes.NewReader(buf.Bytes())

	// check frame type
	ft, n, err := readFrameType(r)
	t.NoError(err)
	t.Equal(streamFrameType, ft)
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

func (t *testStreamWriter) TestOverLength() {
	var chunkLength uint16 = 3

	buf, fw, cw := t.newWriter(chunkLength)

	b0 := []byte{0x01, 0x02, 0x03, 0x04}

	n, err := fw(b0)
	t.NoError(err)
	t.Equal(len(b0), n)

	b1 := []byte{0x05, 0x06, 0x07, 0x08}

	n, err = fw(b1)
	t.NoError(err)
	t.Equal(len(b1), n)

	t.NoError(cw())

	r := bytes.NewReader(buf.Bytes())

	// check frame type
	ft, n, err := readFrameType(r)
	t.NoError(err)
	t.Equal(streamFrameType, ft)
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

func (t *testStreamWriter) TestWriteAsync() {
	var wg sync.WaitGroup

	b := make([]byte, 256)
	for i := range b {
		b[i] = byte(i)
	}

	buf, fw, cw := t.newWriter(3)

	for i := 0; i < len(b); i += 5 {
		wg.Go(func() {
			if _, err := fw(b[i:min(i+5, len(b))]); err != nil {
				panic(err)
			}
		})
	}

	wg.Wait()
	t.NoError(cw())

	r := bytes.NewReader(buf.Bytes())

	// check frame type
	ft, n, err := readFrameType(r)
	t.NoError(err)
	t.Equal(streamFrameType, ft)
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

func (t *testStreamWriter) TestClose() {
	t.Run("close after close", func() {
		buf, _, cw := t.newWriter(3)
		t.NoError(cw())

		err := cw()
		t.ErrorIs(err, ErrClosed)
		t.Empty(buf.Bytes())
	})

	t.Run("write nil after close", func() {
		buf, fw, cw := t.newWriter(3)
		t.NoError(cw())

		n, err := fw(nil)
		t.Equal(0, n)
		t.ErrorIs(err, ErrClosed)
		t.Empty(buf.Bytes())
	})

	t.Run("write after close", func() {
		buf, fw, cw := t.newWriter(3)
		t.NoError(cw())

		n, err := fw([]byte{0x00, 0x01})
		t.Equal(0, n)
		t.ErrorIs(err, ErrClosed)
		t.Empty(buf.Bytes())
	})
}

func TestStreamWriter(t *testing.T) {
	defer goleak.VerifyNone(t)

	suite.Run(t, new(testStreamWriter))
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
