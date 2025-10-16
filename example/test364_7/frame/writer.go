package frame

import (
	"fmt"
	"io"
	"sync"
	"sync/atomic"
)

type writeCloseFuncs [2]writeFunc

func (w writeCloseFuncs) Write(b []byte) (int, error) {
	return w[0](b)
}

func (w writeCloseFuncs) Close() error {
	_, err := w[1](nil)
	return err
}

func DefaultWriter(w io.Writer) (
	func() (io.WriteCloser, error),
	func(error) error,
) {
	return NewWriterWithChunkLength(w, maxChunkLength)
}

func NewWriterWithChunkLength(w io.Writer, chunkLength uint16) (
	func() (io.WriteCloser, error),
	func(error) error,
) {
	var l sync.RWMutex
	var isClosed bool
	var wrote int64

	return func() (io.WriteCloser, error) {
			l.RLock()
			defer l.RUnlock()

			if isClosed {
				return nil, ErrClosed
			}

			fw, cw := newStreamWriterWithChunkLength(w, streamFrameType, chunkLength)

			return writeCloseFuncs{
					func(b []byte) (int, error) {
						_ = atomic.CompareAndSwapInt64(&wrote, 0, 1)

						return fw(b)
					},
					func([]byte) (int, error) {
						return 0, cw()
					},
				},
				nil
		},
		func(err error) error {
			l.Lock()
			defer l.Unlock()

			if isClosed {
				return ErrClosed
			}

			isClosed = true

			if atomic.LoadInt64(&wrote) < 1 {
				return nil
			}

			fw, cw := newStreamWriterWithChunkLength(w, errorFrameType, chunkLength)

			if err != nil {
				errs := err.Error()
				if errs == "" { // prevent unexpected empty error message
					errs = fmt.Sprintf("%v(%T)", err, err)
				}

				if _, err := fw([]byte(errs)); err != nil {
					return err
				}

			}

			return cw()
		}
}
