package util

import (
	"crypto/sha256"
	"fmt"
	"hash"
	"io"
	"sync"
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
	sync.Mutex
	fname    string
	w        io.WriteCloser
	h        hash.Hash
	m        io.Writer
	checksum string
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
	return w.w.Close()
}

func (w *HashChecksumWriter) Write(b []byte) (int, error) {
	w.Lock()
	defer w.Unlock()

	return w.m.Write(b)
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
	sync.Mutex
	r        io.ReadCloser
	h        hash.Hash
	m        io.Reader
	checksum string
}

func NewHashChecksumReader(r io.ReadCloser, h hash.Hash) *HashChecksumReader {
	return &HashChecksumReader{
		m: io.TeeReader(r, h),
		r: r,
		h: h,
	}
}

func (r *HashChecksumReader) Close() error {
	return r.r.Close()
}

func (r *HashChecksumReader) Read(b []byte) (int, error) {
	r.Lock()
	defer r.Unlock()

	return r.m.Read(b)
}

func (r *HashChecksumReader) Checksum() string {
	r.Lock()
	defer r.Unlock()

	if len(r.checksum) > 0 {
		return r.checksum
	}

	_, _ = io.ReadAll(r.m) // NOTE read rest parts if Checksum() called before fully reading

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
