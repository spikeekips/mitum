package frame

import (
	"bytes"
	"crypto/rand"
	"io"
	"sync"
	"testing"

	"github.com/pkg/errors"
	"github.com/stretchr/testify/suite"
	"go.uber.org/goleak"
)

type testStreaming struct {
	suite.Suite
}

func (t *testStreaming) TestWriteError() {
	t.Run("zero chunk size", func() {
		_, _, err := NewStreamingWriter(nil, 0)
		t.ErrorContains(err, "zero chunk size")
	})

	t.Run("write first bytes", func() {
		write, _, err := NewStreamingWriter(
			ioWriterFunc(func([]byte) (int, error) {
				return 0, errors.Errorf("showme")
			}),
			1,
		)

		n, err := write(randBytes(16))
		t.ErrorContains(err, "showme")
		t.Equal(0, n)
	})

	t.Run("write next", func() {
		var i int
		write, _, err := NewStreamingWriter(
			ioWriterFunc(func(b []byte) (int, error) {
				i++
				if i == 3 {
					return 0, errors.Errorf("showme")
				}

				return len(b), nil
			}),
			15,
		)

		b := randBytes(16)

		n, err := write(b)
		t.NoError(err)
		t.Equal(len(b), n)

		n, err = write(randBytes(16))
		t.ErrorContains(err, "showme")
		t.Equal(0, n)

		b = randBytes(16)
		n, err = write(b)
		t.NoError(err) // previous write error ignored
		t.Equal(len(b), n)
	})
}

func (t *testStreaming) TestWrite() {
	t.Run("nil input", func() {
		w := bytes.NewBuffer(nil)

		write, closef, err := NewStreamingWriter(w, 3)
		t.NoError(err)

		n, err := write(nil)
		t.NoError(err)
		t.Equal(0, n)

		t.NoError(closef())

		out := w.Bytes()
		t.T().Log("out:", out)
		t.NotEmpty(out)

		t.Equal(5, len(out))
		t.EqualValues(streamingFrameType, out[0])

		outChunkSize, _ := sizeToBytes(0)
		t.Equal(outChunkSize, out[1:5])
	})

	t.Run("ok", func() {
		w := bytes.NewBuffer(nil)

		chunkSize := 3

		write, closef, err := NewStreamingWriter(w, chunkSize)
		t.NoError(err)

		b := randBytes(16)
		t.T().Log("input:", b, len(b))

		n, err := write(b[:8])
		t.NoError(err)
		t.Equal(8, n)

		n, err = write(b[8:])
		t.NoError(err)
		t.Equal(8, n)

		t.NoError(closef())

		out := w.Bytes()
		t.T().Log("output:", out)
		t.Equal(31, len(out))

		t.EqualValues(streamingFrameType, out[0])

		outChunkSize, _ := sizeToBytes(uint32(chunkSize))
		t.Equal(outChunkSize, out[1:5])

		t.EqualValues(bodyStreamingChunkType, out[5])
		t.Equal(b[:3], out[6:9])
		t.EqualValues(bodyStreamingChunkType, out[9])
		t.Equal(b[3:6], out[10:13])
		t.EqualValues(bodyStreamingChunkType, out[13])
		t.Equal(b[6:9], out[14:17])
		t.EqualValues(bodyStreamingChunkType, out[17])
		t.Equal(b[9:12], out[18:21])
		t.EqualValues(bodyStreamingChunkType, out[21])
		t.Equal(b[12:15], out[22:25])

		t.EqualValues(eofStreamingChunkType, out[25])

		lastData, _ := sizeToBytes(1)
		t.Equal(lastData, out[26:30])
		t.Equal(b[15:16], out[30:31])
	})

	t.Run("no left", func() {
		w := bytes.NewBuffer(nil)

		chunkSize := 3

		write, closef, err := NewStreamingWriter(w, chunkSize)
		t.NoError(err)

		b := randBytes(6)
		t.T().Log("input:", b, len(b))

		n, err := write(b[:3])
		t.NoError(err)
		t.Equal(3, n)

		n, err = write(b[3:])
		t.NoError(err)
		t.Equal(3, n)

		t.NoError(closef())

		out := w.Bytes()
		t.T().Log("output:", out)
		t.Equal(18, len(out))

		t.EqualValues(streamingFrameType, out[0])

		outChunkSize, _ := sizeToBytes(uint32(chunkSize))
		t.Equal(outChunkSize, out[1:5])

		t.EqualValues(bodyStreamingChunkType, out[5])
		t.Equal(b[:3], out[6:9])
		t.EqualValues(bodyStreamingChunkType, out[9])
		t.Equal(b[3:6], out[10:13])

		t.EqualValues(eofStreamingChunkType, out[13])

		lastData, _ := sizeToBytes(0)
		t.Equal(lastData, out[14:18])
	})

	t.Run("less than chunk size", func() {
		w := bytes.NewBuffer(nil)

		chunkSize := 9

		write, closef, err := NewStreamingWriter(w, chunkSize)
		t.NoError(err)

		b := randBytes(6)
		t.T().Log("input:", b, len(b))

		n, err := write(b)
		t.NoError(err)
		t.Equal(len(b), n)

		t.NoError(closef())

		out := w.Bytes()
		t.T().Log("output:", out)
		t.Equal(16, len(out))

		t.EqualValues(streamingFrameType, out[0])

		outChunkSize, _ := sizeToBytes(uint32(chunkSize))
		t.Equal(outChunkSize, out[1:5])

		t.EqualValues(eofStreamingChunkType, out[5])

		lastData, _ := sizeToBytes(uint32(len(b)))
		t.Equal(lastData, out[6:10])

		t.Equal(b, out[10:])
	})

	t.Run("async", func() {
		w := bytes.NewBuffer(nil)
		chunkSize := 3

		write, closef, err := NewStreamingWriter(w, chunkSize)
		t.NoError(err)

		b := []byte{1, 3, 5}

		count := 99
		expected := bytes.Repeat(b, count)

		var wg sync.WaitGroup

		for range count {
			wg.Add(1)

			go func() {
				defer wg.Done()

				n, err := write(b)
				if err != nil {
					panic(err)
				}
				if n != len(b) {
					panic("n != len(b)")
				}
			}()
		}

		wg.Wait()

		t.NoError(closef())

		out := w.Bytes()
		t.T().Log("output:", out)
		t.Equal(5+(count*4)+5, len(out))

		t.EqualValues(streamingFrameType, out[0])

		outChunkSize, _ := sizeToBytes(uint32(chunkSize))
		t.Equal(outChunkSize, out[1:5])

		i := 5

		for range count {
			t.EqualValues(bodyStreamingChunkType, out[i])
			t.Equal(b, out[i+1:i+1+len(b)])
			i += 1 + len(b)
		}

		t.EqualValues(eofStreamingChunkType, out[i])

		lastData, _ := sizeToBytes(0)
		t.Equal(lastData, out[i+1:i+5])
		t.Empty(out[i+5:])

		t.T().Log("read")
		ft, _, err := readFrameType(w)
		t.NoError(err)
		t.T().Log("frame type:", ft)

		read, err := NewStreamingReader(w)
		t.NoError(err)

		wg = sync.WaitGroup{}

		outbuf := bytes.NewBuffer(nil)
		var l sync.Mutex

		for range count {
			wg.Add(1)

			go func() {
				defer wg.Done()

				p := make([]byte, chunkSize)

				n, err := read(p)
				switch {
				case err == nil:
					if n != len(p) {
						panic("n != len(p)")
					}

					p = p[:n]
				case errors.Is(err, io.EOF):
					if n > len(p) {
						panic("n > len(p)")
					}
					p = p[:n]
				case err != nil:
					panic(err)
				}

				l.Lock()
				outbuf.Write(p)
				l.Unlock()
			}()
		}

		wg.Wait()

		t.Equal(expected, outbuf.Bytes())
	})
}

func (t *testStreaming) writeForRead(b []byte, chunkSize int) io.Reader {
	w := bytes.NewBuffer(nil)

	write, closef, err := NewStreamingWriter(w, chunkSize)
	t.NoError(err)
	_, err = write(b)
	t.NoError(err)
	t.NoError(closef())

	t.T().Log("wrote:", w.Bytes())

	ft, _, err := readFrameType(w)
	t.NoError(err)

	t.T().Log("frame type:", ft)

	return w
}

func (t *testStreaming) TestRead() {
	t.Run("nil", func() {
		r := t.writeForRead(nil, 333)

		read, err := NewStreamingReader(r)
		t.NoError(err)

		rb, err := io.ReadAll(read)
		t.NoError(err)
		t.T().Log("output:", rb)
	})

	t.Run("io.ReadAll", func() {
		b := randBytes(16)
		t.T().Log("input:", b)
		chunkSize := 3
		appended := randBytes(16)

		r := t.writeForRead(b, chunkSize)

		{
			b, err := io.ReadAll(r)
			t.NoError(err)
			t.T().Log("orig:", b)

			b = append(b, appended...)
			t.T().Log("appended:", b)

			r = bytes.NewBuffer(b)
		}

		read, err := NewStreamingReader(r)
		t.NoError(err)

		rb, err := io.ReadAll(read)
		t.NoError(err)
		t.T().Log("output:", rb)

		t.Equal(b, rb)

		p := make([]byte, 1)
		n, err := read(p)
		t.ErrorIs(err, io.EOF)
		t.Equal(0, n)

		rb, err = io.ReadAll(r)
		t.NoError(err)
		t.Equal(appended, rb)
	})

	t.Run("read less than chunk size", func() {
		b := randBytes(5)
		t.T().Log("input:", b)
		chunkSize := 3

		r := t.writeForRead(b, chunkSize)

		read, err := NewStreamingReader(r)
		t.NoError(err)

		p := make([]byte, chunkSize-1)

		n, err := read(p)
		t.T().Logf("read: p=%v n=%d err=%v", p, n, err)
		t.NoError(err)
		t.Equal(len(p), n)
		t.Equal(b[:2], p)

		p = make([]byte, len(p))
		n, err = read(p)
		t.T().Logf("read: p=%v n=%d err=%v", p, n, err)
		t.NoError(err)
		t.Equal(len(p), n)
		t.Equal(b[2:4], p)

		p = make([]byte, 1)
		n, err = read(p)
		t.T().Logf("read: p=%v n=%d err=%v", p, n, err)
		t.ErrorIs(err, io.EOF)
		t.Equal(1, n)
		t.Equal(b[4:], p[:n])
	})

	t.Run("read more than chunk size", func() {
		b := randBytes(5)
		t.T().Log("input:", b)
		chunkSize := 3

		r := t.writeForRead(b, chunkSize)

		read, err := NewStreamingReader(r)
		t.NoError(err)

		p := make([]byte, chunkSize+1)

		n, err := read(p)
		t.T().Logf("read: p=%v n=%d err=%v", p, n, err)
		t.NoError(err)
		t.Equal(len(p), n)
		t.Equal(b[:4], p)

		p = make([]byte, len(p))
		n, err = read(p)
		t.T().Logf("read: p=%v n=%d err=%v", p, n, err)
		t.ErrorIs(err, io.EOF)
		t.Equal(1, n)
		t.Equal(b[4:], p[:n])
	})
}

func TestStreaming(t *testing.T) {
	defer goleak.VerifyNone(t)

	suite.Run(t, new(testStreaming))
}

func randBytes(n int) []byte {
	b := make([]byte, n)
	rand.Read(b)

	return b
}

type ioWriterFunc func([]byte) (int, error)

func (f ioWriterFunc) Write(p []byte) (int, error) {
	return f(p)
}
