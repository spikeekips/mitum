package util

import (
	"bufio"
	"bytes"
	"io"
	"os"
	"path/filepath"
	"sync"

	"github.com/pkg/errors"
)

func CleanDirectory(root string, filter func(path string) bool) error {
	switch fi, err := os.Stat(root); {
	case os.IsNotExist(err):
		return nil
	case err != nil:
		return errors.WithStack(err)
	case !fi.IsDir():
		return errors.Errorf("not directory")
	}

	subs, err := os.ReadDir(root)
	if err != nil {
		return errors.WithStack(err)
	}

	for i := range subs {
		n := subs[i].Name()

		if !filter(n) {
			continue
		}

		if err := os.RemoveAll(filepath.Join(root, n)); err != nil {
			return errors.WithStack(err)
		}
	}

	return nil
}

type BufferedResetReader struct {
	io.Reader
	r   io.Reader
	buf *bytes.Buffer
	sync.RWMutex
}

func NewBufferedResetReader(r io.Reader) *BufferedResetReader {
	buf := bytes.NewBuffer(nil)

	return &BufferedResetReader{
		Reader: io.TeeReader(r, buf),
		r:      r,
		buf:    buf,
	}
}

func (s *BufferedResetReader) Close() error {
	s.Lock()
	defer s.Unlock()

	if s.buf != nil {
		s.buf.Reset()
		s.buf = nil
	}

	return nil
}

func (s *BufferedResetReader) Reset() {
	s.Lock()
	defer s.Unlock()

	switch {
	case s.buf == nil:
		return
	case s.buf.Len() < 1:
		s.Reader = s.r
	default:
		s.Reader = io.MultiReader(s.buf, s.r)
	}
}

func (s *BufferedResetReader) Read(p []byte) (int, error) {
	s.RLock()
	defer s.RUnlock()

	if s.buf == nil {
		return 0, os.ErrClosed
	}

	return s.Reader.Read(p) //nolint:wrapcheck //...
}

type BufferedWriter struct {
	w io.Writer
	*bufio.Writer
}

func NewBufferedWriter(w io.Writer, size int) *BufferedWriter {
	return &BufferedWriter{
		w:      w,
		Writer: bufio.NewWriterSize(w, size),
	}
}

func (w *BufferedWriter) Close() error {
	if err := w.Writer.Flush(); err != nil {
		return errors.WithStack(err)
	}

	if i, ok := w.w.(interface{ Flush() error }); ok {
		if err := i.Flush(); err != nil {
			return errors.WithStack(err)
		}
	}

	if i, ok := w.w.(io.Closer); ok {
		if err := i.Close(); err != nil {
			return errors.WithStack(err)
		}
	}

	return nil
}
