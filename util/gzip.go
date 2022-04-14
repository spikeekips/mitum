package util

import (
	"compress/gzip"
	"io"
	"sync"

	"github.com/pkg/errors"
)

// GzipWriter closes the underlying reader too.
type GzipWriter struct {
	sync.Mutex
	io.Writer
	gw *gzip.Writer
}

func NewGzipWriter(f io.Writer) *GzipWriter {
	return &GzipWriter{Writer: f, gw: gzip.NewWriter(f)}
}

func (w *GzipWriter) Write(p []byte) (int, error) {
	w.Lock()
	defer w.Unlock()

	return w.gw.Write(p)
}

func (w *GzipWriter) Close() error {
	if err := w.gw.Close(); err != nil {
		return errors.Wrap(err, "")
	}

	j, ok := w.Writer.(io.Closer)
	if !ok {
		return nil
	}

	if err := j.Close(); err != nil {
		return errors.Wrap(err, "")
	}

	return nil
}

// GzipReader closes the underlying reader too.
type GzipReader struct {
	io.Reader
	gr *gzip.Reader
}

func NewGzipReader(f io.Reader) (*GzipReader, error) {
	r, err := gzip.NewReader(f)
	if err != nil {
		return nil, errors.Wrap(err, "")
	}

	return &GzipReader{Reader: f, gr: r}, nil
}

func (r *GzipReader) Read(p []byte) (int, error) {
	return r.gr.Read(p)
}

func (r *GzipReader) Close() error {
	if err := r.gr.Close(); err != nil {
		return errors.Wrap(err, "")
	}

	j, ok := r.Reader.(io.Closer)
	if !ok {
		return nil
	}

	if err := j.Close(); err != nil {
		return errors.Wrap(err, "")
	}

	return nil
}
