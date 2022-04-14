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

	return fmt.Sprintf("%x", sha.Sum(nil))
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

	w.checksum = fmt.Sprintf("%x", w.h.Sum(nil))
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
