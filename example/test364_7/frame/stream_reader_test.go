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

type testStreamReader struct {
	suite.Suite
}

func (t *testStreamReader) writeFrame(b []byte, chunkLength uint16) io.Reader {
	buf := bytes.NewBuffer(nil)
	defer buf.Reset()

	if err := writeStreamFrame(streamFrameType, buf, b, chunkLength); err != nil {
		panic(err)
	}

	r := bytes.NewReader(buf.Bytes())

	if _, _, err := readFrameType(r); err != nil {
		panic(err)
	}

	return newStreamChunksReader(r, chunkLength)
}

func (t *testStreamReader) checkEOF(r io.Reader, expected error) {
	t.T().Helper()

	p := make([]byte, 0, 9)
	n, err := r.Read(p[:cap(p)])
	p = p[:n]
	t.ErrorIs(err, expected)
	t.Equal(0, n)
	t.Empty(p)
}

func (t *testStreamReader) TestChunkReader() {
	var chunkLength uint16 = 5

	t.Run("read empty", func() {
		b := sampleBytes(4)
		cr := t.writeFrame(b, chunkLength)

		n, err := cr.Read(nil)
		t.NoError(err)
		t.Equal(0, n)
	})

	t.Run("read same length", func() {
		b := sampleBytes(4)
		cr := t.writeFrame(b, chunkLength)

		p := make([]byte, len(b))
		n, err := cr.Read(p)
		t.NoError(err)
		t.Equal(len(p), n)
		t.Equal(b, p)

		// read more
		t.checkEOF(cr, io.EOF)
		t.checkEOF(cr, ErrClosed)
	})

	t.Run("read less", func() {
		b := sampleBytes(4)
		cr := t.writeFrame(b, chunkLength)

		p := make([]byte, len(b)-1)
		n, err := cr.Read(p)
		t.NoError(err)
		t.Equal(len(p), n)
		t.Equal(b[:len(b)-1], p)

		// read left
		p = make([]byte, 0, len(b)-1)
		n, err = cr.Read(p[:cap(p)])
		t.ErrorIs(err, io.EOF)
		t.Equal(1, n)
		t.Equal(b[len(b)-1:], p[:n])

		// read again
		t.checkEOF(cr, ErrClosed)
	})

	t.Run("read more", func() {
		b := sampleBytes(4)
		cr := t.writeFrame(b, chunkLength)

		p := make([]byte, 0, 8)
		n, err := cr.Read(p[:cap(p)])
		t.ErrorIs(err, io.EOF)
		t.Equal(len(b), n)
		p = p[:n]
		t.Equal(b, p)

		// read again
		t.checkEOF(cr, ErrClosed)
	})

	t.Run("read all", func() {
		b := sampleBytes(4)
		cr := t.writeFrame(b, chunkLength)

		br, err := io.ReadAll(cr)
		t.NoError(err)
		t.Equal(b, br)

		// read again
		t.checkEOF(cr, ErrClosed)
	})
}

func (t *testStreamReader) TestChunkReaderAsync() {
	var chunkLength uint16 = 3

	b := sampleBytes(256)
	cr := t.writeFrame(b, chunkLength)

	var wg sync.WaitGroup

	buf := bytes.NewBuffer(nil)

	ch := make(chan []byte, len(b))
	done := make(chan struct{})

	go func() {
		for p := range ch {
			buf.Write(p)
		}

		done <- struct{}{}
	}()

	for i := 0; i < len(b); i += 5 {
		wg.Go(func() {
			p := make([]byte, 0, 5)

			n, err := cr.Read(p[:cap(p)])
			if err != nil && !errors.Is(err, io.EOF) {
				panic(err)
			}

			ch <- p[:n]
		})
	}

	wg.Wait()
	close(ch)
	<-done

	br := buf.Bytes()
	slices.Sort(br)
	t.Equal(b, br)
}

func TestStreamReader(t *testing.T) {
	defer goleak.VerifyNone(t)

	suite.Run(t, new(testStreamReader))
}

func sampleBytes(l int64) []byte {
	b := make([]byte, l)
	for i := range b {
		b[i] = byte(i)
	}

	return b
}

func writeStreamFrame(t frameType, w io.Writer, b []byte, chunkLength uint16) error {
	fw, cw := newStreamWriterWithChunkLength(w, t, chunkLength)

	if _, err := fw(b); err != nil {
		return err
	}

	return cw()
}
