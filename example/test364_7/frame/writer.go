package frame

import (
	"fmt"
	"io"
	"sync"
)

type (
	WriterFunc      func([]byte) (int, error)
	WriteCloserFunc [2]WriterFunc
)

func (w WriteCloserFunc) Write(b []byte) (int, error) {
	return w[0](b)
}

func (w WriteCloserFunc) Close() error {
	_, err := w[1](nil)

	return err
}

var _ io.WriteCloser = (*WriteCloserFunc)(nil)

func DefaultWriter(w io.Writer, t frameType) WriteCloserFunc {
	return NewWriterWithChunkLength(w, t, defaultChunkLength)
}

func NewWriterWithChunkLength(w io.Writer, t frameType, chunkLength uint16) WriteCloserFunc {
	var l sync.Mutex
	var wroteFrameType, isClosed bool

	return [2]WriterFunc{
		func(b []byte) (int, error) {
			l.Lock()
			defer l.Unlock()

			if isClosed {
				return 0, ErrClosed
			}

			bl := uint16(len(b))
			if bl < 1 {
				return 0, nil
			}

			if !wroteFrameType {
				if _, err := w.Write(frameTypeToBytes(t)); err != nil {
					return 0, err
				}

				wroteFrameType = true
			}

			var wn int

			for i := uint16(0); i < bl; i += chunkLength {
				c := b[i:min(i+chunkLength, bl)]
				if _, err := writeChunk(w, c); err != nil {
					return wn + len(c), err
				}

				wn += len(c)
			}

			return wn, nil
		},
		func([]byte) (int, error) {
			l.Lock()
			defer l.Unlock()

			if isClosed {
				return 0, ErrClosed
			}

			isClosed = true

			if !wroteFrameType {
				switch {
				case t != errorFrameType:
					return 0, nil
				default:
					if _, err := w.Write(frameTypeToBytes(t)); err != nil {
						return 0, err
					}
				}
			}

			return writeChunk(w, nil)
		},
	}
}

func writeChunk(w io.Writer, b []byte) (int, error) {
	bl := uint16(len(b))
	var wn int

	switch i, err := lengthToBytes(bl); {
	case err != nil:
		return wn, err
	default:
		n, err := w.Write(i)
		if err != nil {
			return wn + n, err
		}

		wn += n
	}

	if bl < 1 {
		return wn, nil
	}

	n, err := w.Write(b)

	return wn + n, err
}

func DefaultFramesWriterWithChunkLength(w io.Writer) (
	WriterFunc,
	func(error) error,
) {
	return NewFramesWriterWithChunkLength(w, defaultChunkLength)
}

func NewFramesWriterWithChunkLength(w io.Writer, chunkLength uint16) (
	WriterFunc,
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

			fw := NewWriterWithChunkLength(w, dataFrameType, chunkLength)

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

			fw := NewWriterWithChunkLength(w, errorFrameType, chunkLength)

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
