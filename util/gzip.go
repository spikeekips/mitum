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
	sync.Mutex
}

func NewGzipWriter(f io.Writer, level int) (*GzipWriter, error) {
	gw, err := gzip.NewWriterLevel(f, level)
	if err != nil {
		return nil, errors.WithStack(err)
	}

	return &GzipWriter{Writer: f, gw: gw}, nil
}

func (w *GzipWriter) Write(p []byte) (int, error) {
	w.Lock()
	defer w.Unlock()

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
	sync.RWMutex
}

func NewCompressedReader(
	r io.Reader,
	format string,
	decompressReader DecompressReaderFunc,
) (*CompressedReader, error) {
	f := func(r io.Reader) (io.Reader, error) {
		return r, nil
	}

	if len(format) > 0 {
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

func (r *CompressedReader) Tee(tee io.Writer) (io.Reader, error) {
	r.Lock()
	defer r.Unlock()

	switch r.decompressed {
	case 2:
		return nil, errors.Errorf("already Teed")
	case 1:
		return nil, errors.Errorf("already Decompressed")
	}

	if r.cr == nil {
		r.decompressed = 2

		switch i, err := r.decompressReader(io.TeeReader(r.Reader, tee)); {
		case err != nil:
			return nil, err
		default:
			r.cr = i
		}
	}

	return r.cr, nil
}

func (r *CompressedReader) Decompress() (io.Reader, error) {
	r.Lock()
	defer r.Unlock()

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
