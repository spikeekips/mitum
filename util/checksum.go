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

type ChecksumWriter struct {
	sync.Mutex
	m        io.Writer
	w        io.WriteCloser
	fname    string
	sha      hash.Hash
	checksum string
}

func NewChecksumWriter(fname string, w io.WriteCloser) *ChecksumWriter {
	sha := sha256.New()

	return &ChecksumWriter{
		m:     io.MultiWriter(sha, w),
		w:     w,
		fname: fname,
		sha:   sha,
	}
}

func (w *ChecksumWriter) Close() error {
	return w.w.Close()
}

func (w *ChecksumWriter) Write(b []byte) (int, error) {
	w.Lock()
	defer w.Unlock()

	return w.m.Write(b)
}

func (w *ChecksumWriter) Name() string {
	return w.fname
}

func (w *ChecksumWriter) Checksum() string {
	w.Lock()
	defer w.Unlock()

	if len(w.checksum) > 0 {
		return w.checksum
	}

	w.checksum = fmt.Sprintf("%x", w.sha.Sum(nil))
	w.sha.Reset()

	return w.checksum
}
