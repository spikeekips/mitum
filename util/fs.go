package util

import (
	"bytes"
	"io"
	"os"
	"path/filepath"
	"sync"
)

func CleanDirectory(root string, filter func(path string) bool) error {
	e := StringError("clean directory")

	switch fi, err := os.Stat(root); {
	case os.IsNotExist(err):
		return nil
	case err != nil:
		return e.Wrap(err)
	case !fi.IsDir():
		return e.Errorf("not directory")
	}

	subs, err := os.ReadDir(root)
	if err != nil {
		return e.Wrap(err)
	}

	for i := range subs {
		n := subs[i].Name()

		if !filter(n) {
			continue
		}

		if err := os.RemoveAll(filepath.Join(root, n)); err != nil {
			return e.Wrap(err)
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
