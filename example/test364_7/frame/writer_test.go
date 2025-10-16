package frame

import (
	"bytes"
	"io"
	"slices"
	"testing"

	"github.com/stretchr/testify/suite"
	"go.uber.org/goleak"
)

type testWriter struct {
	suite.Suite
}

func (t *testWriter) newWriter(chunkLength uint16) (
	*bytes.Buffer,
	func() (io.WriteCloser, error),
	func(error) error,
) {
	buf := bytes.NewBuffer(nil)

	fw, ew := NewWriterWithChunkLength(buf, chunkLength)

	return buf, fw, ew
}

func (t *testWriter) readFrame(r io.Reader) (frameType, []byte, error) {
	ft, _, err := readFrameType(r)
	if err != nil {
		return ft, nil, err
	}

	var p []byte

end:
	for {
		switch i, _, err := readChunkLength(r); {
		case err != nil:
			return ft, nil, err
		case i < 1:
			break end
		default:
			b := make([]byte, i)
			if _, err := io.ReadFull(r, b); err != nil {
				return ft, nil, err
			}

			p = append(p, b...)
		}
	}

	return ft, p, nil
}

func (t *testWriter) TestWrite() {
	t.Run("empty", func() {
		buf, _, ew := t.newWriter(3)

		err := ew(nil)
		t.NoError(err)

		t.Equal(0, buf.Len())

		err = ew(nil)
		t.ErrorIs(err, ErrClosed)
	})

	t.Run("one stream", func() {
		var chunkLength uint16 = 3

		buf, fw, ew := t.newWriter(chunkLength)

		w, err := fw()
		t.NoError(err)

		b := sampleBytes(7)
		b = append(b, sampleBytes(8)...)

		n, err := w.Write(b[:7])
		t.NoError(err)
		t.Equal(7, n)

		n, err = w.Write(b[7:])
		t.NoError(err)
		t.Equal(8, n)

		t.NoError(w.Close())

		t.NoError(ew(nil))

		r := bytes.NewReader(buf.Bytes())

		ft, br, err := t.readFrame(r)
		t.NoError(err)
		t.Equal(streamFrameType, ft)
		t.Equal(b, br)

		ft, br, err = t.readFrame(r)
		t.NoError(err)
		t.Equal(errorFrameType, ft)
		t.Empty(br)

		_, _, err = readChunk(r)
		t.ErrorIs(err, io.EOF)
	})

	t.Run("multiple stream", func() {
		var chunkLength uint16 = 3

		buf, fw, ew := t.newWriter(chunkLength)

		bs := [][]byte{
			sampleBytes(7),
			sampleBytes(8),
			sampleBytes(9),
		}

		for b := range slices.Values(bs) {
			w, err := fw()
			t.NoError(err)

			n, err := w.Write(b)
			t.NoError(err)
			t.Equal(len(b), n)

			t.NoError(w.Close())
		}

		t.NoError(ew(nil))

		r := bytes.NewReader(buf.Bytes())

		for i := range len(bs) {
			ft, br, err := t.readFrame(r)
			t.NoError(err)
			t.Equal(streamFrameType, ft)
			t.Equal(bs[i], br)
		}

		ft, br, err := t.readFrame(r)
		t.NoError(err)
		t.Equal(errorFrameType, ft)
		t.Empty(br)

		_, _, err = readChunk(r)
		t.ErrorIs(err, io.EOF)
	})
}

func TestWriter(t *testing.T) {
	defer goleak.VerifyNone(t)

	suite.Run(t, new(testWriter))
}
