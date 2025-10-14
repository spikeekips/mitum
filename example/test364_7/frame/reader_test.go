package frame

import (
	"bytes"
	"io"
	"iter"
	"slices"
	"sync"
	"testing"

	"github.com/pkg/errors"
	"github.com/stretchr/testify/suite"
	"go.uber.org/goleak"
)

type testReader struct {
	suite.Suite
}

func (t *testReader) writeFrame(b []byte, chunkLength uint16) ReaderFunc {
	buf := bytes.NewBuffer(nil)
	defer buf.Reset()

	if err := writeFrame(dataFrameType, buf, b, chunkLength); err != nil {
		panic(err)
	}

	r := bytes.NewReader(buf.Bytes())

	switch t, cr, _, err := NewReaderWithChunkLength(r, chunkLength); {
	case err != nil:
		panic(err)
	case t != dataFrameType:
		panic(errors.Errorf("unknown frame type, %v", t))
	default:
		return cr
	}
}

func (t *testReader) checkEOF(r io.Reader, expected error) {
	t.T().Helper()

	p := make([]byte, 0, 9)
	n, err := r.Read(p[:cap(p)])
	p = p[:n]
	t.ErrorIs(err, expected)
	t.Equal(0, n)
	t.Empty(p)
}

func (t *testReader) TestChunkReader() {
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
		p = p[:n]
		t.Equal(b[len(b)-1:], p)

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

func (t *testReader) TestChunkReaderAsync() {
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

func TestReader(t *testing.T) {
	defer goleak.VerifyNone(t)

	suite.Run(t, new(testReader))
}

type testIterFrames struct {
	suite.Suite
}

func (t *testIterFrames) TestRead() {
	t.Run("empty", func() {
		r := bytes.NewReader(nil)

		for cr, err := range NewFramesIterWithChunkLength(r, 3) {
			t.NoError(err)

			t.Fail("reader found", "cr == nil=%v err=%v", cr == nil, err)
		}
	})

	t.Run("ok", func() {
		var chunkLength uint16 = 3

		b0 := sampleBytes(8)
		b1 := sampleBytes(9)

		buf := bytes.NewBuffer(nil)
		defer buf.Reset()

		t.NoError(writeFrame(dataFrameType, buf, b0, chunkLength))
		t.NoError(writeFrame(dataFrameType, buf, b1, chunkLength))

		r := bytes.NewReader(buf.Bytes())

		bufr := bytes.NewBuffer(nil)
		defer bufr.Reset()

		for cr, err := range NewFramesIterWithChunkLength(r, chunkLength) {
			t.NoError(err)

			br, err := io.ReadAll(cr)
			t.NoError(err)

			bufr.Write(br)
		}

		t.Equal(b0, bufr.Bytes()[:len(b0)])
		t.Equal(b1, bufr.Bytes()[len(b0):])
	})

	t.Run("not read reader", func() {
		var chunkLength uint16 = 3

		b0 := sampleBytes(8)
		b1 := sampleBytes(9)

		buf := bytes.NewBuffer(nil)
		defer buf.Reset()

		t.NoError(writeFrame(dataFrameType, buf, b0, chunkLength))
		t.NoError(writeFrame(dataFrameType, buf, b1, chunkLength))

		r := bytes.NewReader(buf.Bytes())

		bufr := bytes.NewBuffer(nil)
		defer bufr.Reset()

		next, stop := iter.Pull2(NewFramesIterWithChunkLength(r, chunkLength))
		defer stop()

		_, err, hasNext := next()
		t.True(hasNext)
		t.NoError(err)

		cr, err, hasNext := next()
		t.True(hasNext)
		t.NoError(err)

		b1r, err := io.ReadAll(cr)
		t.NoError(err)

		t.Equal(b1, b1r)
	})

	t.Run("error", func() {
		var chunkLength uint16 = 3

		b0 := sampleBytes(8)
		b1 := []byte("showme")

		buf := bytes.NewBuffer(nil)
		defer buf.Reset()

		t.NoError(writeFrame(dataFrameType, buf, b0, chunkLength))
		t.NoError(writeFrame(errorFrameType, buf, b1, chunkLength))

		r := bytes.NewReader(buf.Bytes())

		bufr := bytes.NewBuffer(nil)
		defer bufr.Reset()

		var errr error
		for cr, err := range NewFramesIterWithChunkLength(r, chunkLength) {
			if err != nil {
				errr = err

				continue
			}

			if errr != nil {
				t.Fail("reader found after error")
			}

			br, err := io.ReadAll(cr)
			t.NoError(err)

			bufr.Write(br)
		}

		t.Equal(b0, bufr.Bytes())
		t.Equal(string(b1), errr.Error())
	})

	t.Run("nil error", func() {
		var chunkLength uint16 = 3

		b0 := sampleBytes(8)

		buf := bytes.NewBuffer(nil)
		defer buf.Reset()

		t.NoError(writeFrame(dataFrameType, buf, b0, chunkLength))
		t.NoError(writeFrame(errorFrameType, buf, nil, chunkLength))

		r := bytes.NewReader(buf.Bytes())

		bufr := bytes.NewBuffer(nil)
		defer bufr.Reset()

		for cr, err := range NewFramesIterWithChunkLength(r, chunkLength) {
			t.NoError(err)

			br, err := io.ReadAll(cr)
			t.NoError(err)

			bufr.Write(br)
		}

		t.Equal(b0, bufr.Bytes())
	})

	t.Run("data after error", func() {
		var chunkLength uint16 = 3

		b0 := sampleBytes(8)
		b1 := []byte("showme")
		b2 := sampleBytes(9)

		buf := bytes.NewBuffer(nil)
		defer buf.Reset()

		t.NoError(writeFrame(dataFrameType, buf, b0, chunkLength))
		t.NoError(writeFrame(errorFrameType, buf, b1, chunkLength))
		t.NoError(writeFrame(dataFrameType, buf, b2, chunkLength))

		r := bytes.NewReader(buf.Bytes())

		bufr0 := bytes.NewBuffer(nil)
		defer bufr0.Reset()

		var errr error
		for cr, err := range NewFramesIterWithChunkLength(r, chunkLength) {
			if err != nil {
				errr = err

				continue
			}

			if errr != nil {
				t.Fail("reader found after error")
			}

			br, err := io.ReadAll(cr)
			t.NoError(err)

			bufr0.Write(br)
		}

		t.Equal(b0, bufr0.Bytes())
		t.Equal(string(b1), errr.Error())

		bufr1 := bytes.NewBuffer(nil)
		defer bufr1.Reset()

		for cr, err := range NewFramesIterWithChunkLength(r, chunkLength) {
			t.NoError(err)

			br, err := io.ReadAll(cr)
			t.NoError(err)

			bufr1.Write(br)
		}

		t.Equal(b2, bufr1.Bytes())
	})
}

func TestIterFrames(t *testing.T) {
	defer goleak.VerifyNone(t)

	suite.Run(t, new(testIterFrames))
}

func sampleBytes(l uint64) []byte {
	b := make([]byte, l)
	for i := range b {
		b[i] = byte(i)
	}

	return b
}

func writeFrame(t frameType, w io.Writer, b []byte, chunkLength uint16) error {
	fw := NewWriterWithChunkLength(w, t, chunkLength)

	_, err := fw.Write(b)
	if err != nil {
		return err
	}

	return fw.Close()
}
