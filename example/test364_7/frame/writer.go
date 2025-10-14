package frame

import (
	"fmt"
	"io"
	"sync"
)

func DefaultFramesWriterWithChunkLength(w io.Writer) (
	writeFunc,
	func(error) error,
) {
	return NewFramesWriterWithChunkLength(w, defaultChunkLength)
}

func NewFramesWriterWithChunkLength(w io.Writer, chunkLength uint16) (
	writeFunc,
	func(error) error,
) {
	var l sync.Mutex
	var wroteFrame, isClosed bool

	return func(b []byte) (int, error) {
			l.Lock()
			defer l.Unlock()

			if isClosed {
				return 0, ErrClosed
			}

			fw := NewStreamWriterWithChunkLength(w, streamFrameType, chunkLength)

			switch n, err := fw.Write(b); {
			case err != nil:
				return n, err
			default:
				if !wroteFrame {
					wroteFrame = true
				}

				return n, fw.Close()
			}
		},
		func(err error) error {
			l.Lock()
			defer l.Unlock()

			if isClosed {
				return ErrClosed
			}

			isClosed = true

			if !wroteFrame {
				return nil
			}

			fw := NewStreamWriterWithChunkLength(w, errorFrameType, chunkLength)

			if err != nil {
				errs := err.Error()
				if errs == "" { // prevent unexpected empty error message
					errs = fmt.Sprintf("%v(%T)", err, err)
				}

				if _, err := fw.Write([]byte(errs)); err != nil {
					return err
				}

			}

			return fw.Close()
		}
}
