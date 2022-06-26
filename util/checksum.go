package util

import (
	"crypto/sha256"
	"fmt"
	"hash"
	"io"
	"sync"

	"github.com/pkg/errors"
)

func SHA256Checksum(b []byte) string {
	sha := sha256.New()
	_, _ = sha.Write(b)

	return checksumstring(sha)
}

type ChecksumWriter interface {
	io.WriteCloser
	Name() string
	Checksum() string
}

type HashChecksumWriter struct {
	w        io.WriteCloser
	h        hash.Hash
	m        io.Writer
	fname    string
	checksum string
	sync.Mutex
}

func NewHashChecksumWriter(fname string, w io.WriteCloser, h hash.Hash) *HashChecksumWriter {
	return &HashChecksumWriter{
		m:     io.MultiWriter(h, w),
		w:     w,
		fname: fname,
		h:     h,
	}
}

func (w *HashChecksumWriter) Close() error {
	if err := w.w.Close(); err != nil {
		return errors.WithStack(err)
	}

	return nil
}

func (w *HashChecksumWriter) Write(b []byte) (int, error) {
	w.Lock()
	defer w.Unlock()

	n, err := w.m.Write(b)
	if err != nil {
		return 0, errors.WithStack(err)
	}

	return n, nil
}

func (w *HashChecksumWriter) Name() string {
	return w.fname
}

func (w *HashChecksumWriter) Checksum() string {
	w.Lock()
	defer w.Unlock()

	if len(w.checksum) > 0 {
		return w.checksum
	}

	w.checksum = checksumstring(w.h)
	w.h.Reset()

	return w.checksum
}

type DummyChecksumWriter struct {
	io.WriteCloser
	cw ChecksumWriter
}

func NewDummyChecksumWriter(f io.WriteCloser, cw ChecksumWriter) *DummyChecksumWriter {
	return &DummyChecksumWriter{
		WriteCloser: f,
		cw:          cw,
	}
}

func (w *DummyChecksumWriter) Name() string {
	return w.cw.Name()
}

func (w *DummyChecksumWriter) Checksum() string {
	return w.cw.Checksum()
}

type ChecksumReader interface {
	io.ReadCloser
	Checksum() string
}

type HashChecksumReader struct {
	r        io.ReadCloser
	h        hash.Hash
	m        io.Reader
	checksum string
	sync.Mutex
}

func NewHashChecksumReader(r io.ReadCloser, h hash.Hash) *HashChecksumReader {
	return &HashChecksumReader{
		m: io.TeeReader(r, h),
		r: r,
		h: h,
	}
}

func (r *HashChecksumReader) Close() error {
	if err := r.r.Close(); err != nil {
		return errors.WithStack(err)
	}

	return nil
}

func (r *HashChecksumReader) Read(b []byte) (int, error) {
	r.Lock()
	defer r.Unlock()

	n, err := r.m.Read(b)

	switch {
	case err == nil:
	case errors.Is(err, io.EOF):
	default:
		return 0, errors.WithStack(err)
	}

	return n, err //nolint:wrapcheck // nil || io.EOF
}

func (r *HashChecksumReader) Checksum() string {
	r.Lock()
	defer r.Unlock()

	if len(r.checksum) > 0 {
		return r.checksum
	}

	if _, err := io.ReadAll(r.m); err != nil { // NOTE read rest parts if Checksum() called before fully reading
		return ""
	}

	r.checksum = checksumstring(r.h)
	r.h.Reset()

	return r.checksum
}

type DummyChecksumReader struct {
	io.ReadCloser
	cr ChecksumReader
}

func NewDummyChecksumReader(f io.ReadCloser, cr ChecksumReader) *DummyChecksumReader {
	return &DummyChecksumReader{
		ReadCloser: f,
		cr:         cr,
	}
}

func (r *DummyChecksumReader) Checksum() string {
	return r.cr.Checksum()
}

func checksumstring(h hash.Hash) string {
	return fmt.Sprintf("%x", h.Sum(nil))
}
