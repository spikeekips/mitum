package util

import (
	"compress/gzip"
	"io"
	"sync"

	"github.com/pkg/errors"
)

// GzipWriter closes the underlying reader too.
type GzipWriter struct {
	io.Writer
	gw *gzip.Writer
	l  sync.Mutex
}

func NewGzipWriter(f io.Writer, level int) (*GzipWriter, error) {
	gw, err := gzip.NewWriterLevel(f, level)
	if err != nil {
		return nil, errors.WithStack(err)
	}

	return &GzipWriter{Writer: f, gw: gw}, nil
}

func (w *GzipWriter) Write(p []byte) (int, error) {
	w.l.Lock()
	defer w.l.Unlock()

	return w.gw.Write(p) //nolint:wrapcheck //...
}

func (w *GzipWriter) Close() error {
	if err := w.gw.Close(); err != nil {
		return errors.WithStack(err)
	}

	j, ok := w.Writer.(io.Closer)
	if !ok {
		return nil
	}

	if err := j.Close(); err != nil {
		return errors.WithStack(err)
	}

	return nil
}

// SafeGzipReadCloser closes the underlying reader too.
type SafeGzipReadCloser struct {
	io.Reader
	gr *gzip.Reader
}

func NewSafeGzipReadCloser(f io.Reader) (*SafeGzipReadCloser, error) {
	r, err := gzip.NewReader(f)
	if err != nil {
		return nil, errors.WithStack(err)
	}

	return &SafeGzipReadCloser{Reader: f, gr: r}, nil
}

func (r *SafeGzipReadCloser) Read(p []byte) (int, error) {
	return r.gr.Read(p) //nolint:wrapcheck //...
}

func (r *SafeGzipReadCloser) Close() error {
	if err := r.gr.Close(); err != nil {
		return errors.WithStack(err)
	}

	j, ok := r.Reader.(io.Closer)
	if !ok {
		return nil
	}

	if err := j.Close(); err != nil {
		return errors.WithStack(err)
	}

	return nil
}

type DecompressReaderFunc func(format string) (func(io.Reader) (io.Reader, error), error)

type CompressedReader struct {
	io.Reader
	cr               io.Reader
	decompressReader func(io.Reader) (io.Reader, error)
	Format           string // NOTE compress format, usually file extension without '.' prefix
	decompressed     int    // 1=decompress 2=tee
	l                sync.RWMutex
}

func NewCompressedReader(
	r io.Reader,
	format string,
	decompressReader DecompressReaderFunc,
) (*CompressedReader, error) {
	f := func(r io.Reader) (io.Reader, error) {
		return r, nil
	}

	if format != "" {
		if decompressReader == nil {
			decompressReader = DefaultDecompressReaderFunc //revive:disable-line:modifies-parameter
		}

		switch i, err := decompressReader(format); {
		case err != nil:
			return nil, err
		default:
			f = i
		}
	}

	return &CompressedReader{
		Reader:           r,
		Format:           format,
		decompressReader: f,
	}, nil
}

// Close only closes decompress Reader.
func (r *CompressedReader) Close() error {
	r.l.RLock()
	defer r.l.RUnlock()

	if r.cr != nil {
		if i, ok := r.cr.(io.Closer); ok {
			if err := i.Close(); err != nil {
				return errors.WithStack(err)
			}
		}
	}

	return nil
}

// Exaust read all unread bytes from Decompress().
func (r *CompressedReader) Exaust() error {
	r.l.RLock()
	defer r.l.RUnlock()

	if r.cr == nil {
		return nil
	}

	_, err := io.ReadAll(r.cr)

	return errors.WithStack(err)
}

func (r *CompressedReader) Tee(tee, decompressed io.Writer) (io.Reader, error) {
	r.l.Lock()
	defer r.l.Unlock()

	switch r.decompressed {
	case 2:
		return nil, errors.Errorf("already Teed")
	case 1:
		return nil, errors.Errorf("already Decompressed")
	}

	if r.cr == nil {
		r.decompressed = 2

		var nr io.Reader

		switch {
		case tee == nil:
			nr = r.Reader
		default:
			nr = io.TeeReader(r.Reader, tee)
		}

		switch i, err := r.decompressReader(nr); {
		case err != nil:
			return nil, err
		case decompressed != nil:
			r.cr = io.TeeReader(i, decompressed)
		default:
			r.cr = i
		}
	}

	return r.cr, nil
}

func (r *CompressedReader) Decompress() (io.Reader, error) {
	r.l.Lock()
	defer r.l.Unlock()

	if r.decompressed == 2 {
		return r.cr, nil
	}

	if r.cr == nil {
		r.decompressed = 1

		switch i, err := r.decompressReader(r.Reader); {
		case err != nil:
			return nil, err
		default:
			r.cr = i
		}
	}

	return r.cr, nil
}

func DefaultDecompressReaderFunc(format string) (func(io.Reader) (io.Reader, error), error) {
	switch format {
	case "":
		return func(r io.Reader) (io.Reader, error) {
			return r, nil
		}, nil
	case "gz":
	default:
		return nil, errors.Errorf("not supported compress format, %q", format)
	}

	return func(r io.Reader) (io.Reader, error) {
		i, err := gzip.NewReader(r)

		return i, errors.WithStack(err)
	}, nil
}
