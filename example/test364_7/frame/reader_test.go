package frame

import (
	"bytes"
	"io"
	"iter"
	"testing"

	"github.com/stretchr/testify/suite"
	"go.uber.org/goleak"
)

type testIter struct {
	suite.Suite
}

func (t *testIter) TestRead() {
	t.Run("empty", func() {
		r := bytes.NewReader(nil)

		for cr, err := range NewIterWithChunkLength(r, 3) {
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

		t.NoError(writeStreamFrame(streamFrameType, buf, b0, chunkLength))
		t.NoError(writeStreamFrame(streamFrameType, buf, b1, chunkLength))

		r := bytes.NewReader(buf.Bytes())

		bufr := bytes.NewBuffer(nil)
		defer bufr.Reset()

		for cr, err := range NewIterWithChunkLength(r, chunkLength) {
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

		t.NoError(writeStreamFrame(streamFrameType, buf, b0, chunkLength))
		t.NoError(writeStreamFrame(streamFrameType, buf, b1, chunkLength))

		r := bytes.NewReader(buf.Bytes())

		bufr := bytes.NewBuffer(nil)
		defer bufr.Reset()

		next, stop := iter.Pull2(NewIterWithChunkLength(r, chunkLength))
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

		t.NoError(writeStreamFrame(streamFrameType, buf, b0, chunkLength))
		t.NoError(writeStreamFrame(errorFrameType, buf, b1, chunkLength))

		r := bytes.NewReader(buf.Bytes())

		bufr := bytes.NewBuffer(nil)
		defer bufr.Reset()

		var errr error
		for cr, err := range NewIterWithChunkLength(r, chunkLength) {
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

		t.NoError(writeStreamFrame(streamFrameType, buf, b0, chunkLength))
		t.NoError(writeStreamFrame(errorFrameType, buf, nil, chunkLength))

		r := bytes.NewReader(buf.Bytes())

		bufr := bytes.NewBuffer(nil)
		defer bufr.Reset()

		for cr, err := range NewIterWithChunkLength(r, chunkLength) {
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

		t.NoError(writeStreamFrame(streamFrameType, buf, b0, chunkLength))
		t.NoError(writeStreamFrame(errorFrameType, buf, b1, chunkLength))
		t.NoError(writeStreamFrame(streamFrameType, buf, b2, chunkLength))

		r := bytes.NewReader(buf.Bytes())

		bufr0 := bytes.NewBuffer(nil)
		defer bufr0.Reset()

		var errr error
		for cr, err := range NewIterWithChunkLength(r, chunkLength) {
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

		for cr, err := range NewIterWithChunkLength(r, chunkLength) {
			t.NoError(err)

			br, err := io.ReadAll(cr)
			t.NoError(err)

			bufr1.Write(br)
		}

		t.Equal(b2, bufr1.Bytes())
	})
}

func TestIter(t *testing.T) {
	defer goleak.VerifyNone(t)

	suite.Run(t, new(testIter))
}
