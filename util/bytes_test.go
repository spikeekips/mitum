package util

import (
	"bytes"
	"context"
	"io"
	"sync"
	"testing"
	"time"

	"github.com/pkg/errors"
	"github.com/stretchr/testify/suite"
)

type testLengthedBytes struct {
	suite.Suite
}

func (t *testLengthedBytes) TestNew() {
	t.Run("nil", func() {
		w := bytes.NewBuffer(nil)
		t.NoError(WriteLengthed(w, nil))

		b := w.Bytes()

		t.Equal(8, len(b))

		l, err := BytesToUint64(b)
		t.NoError(err)
		t.Equal(uint64(0), l)
	})

	t.Run("empty", func() {
		w := bytes.NewBuffer(nil)
		t.NoError(WriteLengthed(w, []byte{}))

		b := w.Bytes()

		t.Equal(8, len(b))

		l, err := BytesToUint64(b)
		t.NoError(err)
		t.Equal(uint64(0), l)
	})

	t.Run("1", func() {
		w := bytes.NewBuffer(nil)
		t.NoError(WriteLengthed(w, []byte{0x00}))

		b := w.Bytes()

		t.Equal(9, len(b))

		l, err := BytesToUint64(b)
		t.NoError(err)
		t.Equal(uint64(1), l)
		t.Equal([]byte{0x00}, b[8:])
	})

	t.Run("uuid", func() {
		w := bytes.NewBuffer(nil)
		i := UUID().Bytes()

		t.NoError(WriteLengthed(w, i))

		b := w.Bytes()

		t.Equal(8+len(i), len(b))

		l, err := BytesToUint64(b)
		t.NoError(err)
		t.Equal(uint64(len(i)), l)
		t.Equal(i, b[8:])
	})
}

func (t *testLengthedBytes) TestRead() {
	t.Run("nil", func() {
		w := bytes.NewBuffer(nil)
		t.NoError(WriteLengthed(w, nil))

		b := w.Bytes()

		r, rleft, err := ReadLengthedBytes(b)
		t.NoError(err)
		t.Equal(0, len(r))
		t.Equal(0, len(rleft))
	})

	t.Run("invalid legth; nil", func() {
		_, _, err := ReadLengthedBytes(nil)
		t.Error(err)
		t.ErrorContains(err, "missing length part")
	})

	t.Run("invalid legth", func() {
		_, _, err := ReadLengthedBytes(UUID().Bytes())
		t.Error(err)
		t.ErrorContains(err, "left not enough")
	})

	t.Run("empty", func() {
		w := bytes.NewBuffer(nil)
		t.NoError(WriteLengthed(w, []byte{}))

		b := w.Bytes()

		r, rleft, err := ReadLengthedBytes(b)
		t.NoError(err)
		t.Equal(0, len(r))
		t.Equal(0, len(rleft))
	})

	t.Run("1", func() {
		w := bytes.NewBuffer(nil)
		t.NoError(WriteLengthed(w, []byte{0x01}))

		b := w.Bytes()

		r, rleft, err := ReadLengthedBytes(b)
		t.NoError(err)
		t.Equal(1, len(r))
		t.Equal(0, len(rleft))
	})

	t.Run("empty and none empty left", func() {
		w := bytes.NewBuffer(nil)
		t.NoError(WriteLengthed(w, []byte{}))

		b := w.Bytes()

		left := UUID().Bytes()
		b = append(b, left...)

		r, rleft, err := ReadLengthedBytes(b)
		t.NoError(err)
		t.Equal(0, len(r))
		t.Equal(left, rleft)
	})

	t.Run("none empty and none empty left", func() {
		i := UUID().Bytes()
		w := bytes.NewBuffer(nil)
		t.NoError(WriteLengthed(w, i))

		b := w.Bytes()

		left := UUID().Bytes()
		b = append(b, left...)

		r, rleft, err := ReadLengthedBytes(b)
		t.NoError(err)
		t.Equal(i, r)
		t.Equal(left, rleft)
	})
}

func TestLengthedBytes(t *testing.T) {
	suite.Run(t, new(testLengthedBytes))
}

type testLengthedBytesSlice struct {
	suite.Suite
}

func (t *testLengthedBytesSlice) TestBytes() {
	t.Run("empty", func() {
		b, err := NewLengthedBytesSlice(nil)
		t.NoError(err)

		m, _, err := ReadLengthedBytesSlice(b)
		t.NoError(err)
		t.Equal(0, len(m))
	})

	t.Run("1 data", func() {
		m := [][]byte{UUID().Bytes()}

		b, err := NewLengthedBytesSlice(m)
		t.NoError(err)

		rm, _, err := ReadLengthedBytesSlice(b)
		t.NoError(err)
		t.Equal(len(m), len(rm))
		t.Equal(m, rm)
	})

	t.Run("over 1 data", func() {
		m := [][]byte{UUID().Bytes(), UUID().Bytes(), UUID().Bytes()}

		b, err := NewLengthedBytesSlice(m)
		t.NoError(err)

		rm, _, err := ReadLengthedBytesSlice(b)
		t.NoError(err)
		t.Equal(len(m), len(rm))
		t.Equal(m, rm)
	})
}

func TestLengthedBytesSlice(t *testing.T) {
	suite.Run(t, new(testLengthedBytesSlice))
}

type notEOFReader struct {
	l    sync.Mutex
	r    *bytes.Buffer
	done bool
}

func newNotEOFReader(b []byte) *notEOFReader {
	return &notEOFReader{
		r: bytes.NewBuffer(b),
	}
}

func (r *notEOFReader) Done() {
	r.l.Lock()
	defer r.l.Unlock()

	r.done = true
}

func (r *notEOFReader) Read(p []byte) (int, error) {
	r.l.Lock()
	defer r.l.Unlock()

	<-time.After(time.Millisecond * 33)

	n, err := r.r.Read(p)

	if r.done {
		if err == nil {
			err = io.EOF
		}

		return n, err
	}

	if errors.Is(err, io.EOF) {
		err = nil
	}

	return n, err
}

func (r *notEOFReader) Write(b []byte) (int, error) {
	r.l.Lock()
	defer r.l.Unlock()

	return r.r.Write(b)
}

type testEnsureRead struct {
	suite.Suite
}

func (t *testEnsureRead) TestRead() {
	t.Run("same size", func() {
		s := []byte("123")
		r := bytes.NewReader(s)

		b := make([]byte, len(s))
		n, err := EnsureRead(context.Background(), r, b)
		t.NoError(err)
		t.Equal(n, uint64(len(s)))
		t.Equal(s, b)
	})

	t.Run("stream", func() {
		s := []byte("123")
		r := newNotEOFReader(s)

		b := make([]byte, len(s)*3)

		donech := make(chan [2]interface{}, 1)
		go func() {
			n, err := EnsureRead(context.Background(), r, b)

			donech <- [2]interface{}{n, err}
		}()

		<-time.After(time.Millisecond * 44)
		s = append(s, []byte("456789")...)
		r.Write([]byte("456"))
		<-time.After(time.Millisecond * 44)
		r.Write([]byte("789"))

		select {
		case <-time.After(time.Millisecond * 100):
			t.NoError(errors.Errorf("waits read, but failed"))
		case i := <-donech:
			n, err := i[0].(uint64), i[1]

			t.Nil(err)
			t.Equal(n, uint64(len(s)))
			t.Equal(s, b)
		}
	})

	t.Run("less", func() {
		s := []byte("123")
		r := bytes.NewReader(s)

		b := make([]byte, len(s)-1)
		n, err := EnsureRead(context.Background(), r, b)
		t.NoError(err)
		t.Equal(n, uint64(len(s))-1)
		t.Equal(s[:len(s)-1], b)
	})

	t.Run("timeout", func() {
		s := []byte("123")

		r := newNotEOFReader(s)

		b := make([]byte, len(s)+1)

		ctx, cancel := context.WithTimeout(context.Background(), time.Millisecond*100)
		defer cancel()

		n, err := EnsureRead(ctx, r, b)
		t.Error(err)
		t.Equal(n, uint64(len(s)))

		t.True(errors.Is(err, context.DeadlineExceeded))
	})

	t.Run("EOF before full", func() {
		s := []byte("123")
		r := bytes.NewReader(s)

		b := make([]byte, len(s)+1)

		n, err := EnsureRead(context.Background(), r, b)
		t.Error(err)
		t.Equal(n, uint64(len(s)))

		t.ErrorContains(err, "insufficient read")
	})
}

func TestEnsureRead(t *testing.T) {
	suite.Run(t, new(testEnsureRead))
}
