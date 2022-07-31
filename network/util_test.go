package network

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
		t.Equal(n, len(s))
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
			n, err := i[0].(int), i[1]

			t.Nil(err)
			t.Equal(n, len(s))
			t.Equal(s, b)
		}
	})

	t.Run("less", func() {
		s := []byte("123")
		r := bytes.NewReader(s)

		b := make([]byte, len(s)-1)
		n, err := EnsureRead(context.Background(), r, b)
		t.NoError(err)
		t.Equal(n, len(s)-1)
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
		t.Equal(n, len(s))

		t.True(errors.Is(err, context.DeadlineExceeded))
	})

	t.Run("EOF before full", func() {
		s := []byte("123")
		r := bytes.NewReader(s)

		b := make([]byte, len(s)+1)

		n, err := EnsureRead(context.Background(), r, b)
		t.Error(err)
		t.Equal(n, len(s))

		t.True(errors.Is(err, io.EOF))
	})
}

func TestEnsureRead(t *testing.T) {
	suite.Run(t, new(testEnsureRead))
}
