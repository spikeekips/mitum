package frame

import (
	"io"
	"sync"
)

type (
	writeFunc         func([]byte) (int, error)
	streamWriterFuncs [2]writeFunc
)

func (w streamWriterFuncs) Write(b []byte) (int, error) {
	return w[0](b)
}

func (w streamWriterFuncs) Close() error {
	_, err := w[1](nil)

	return err
}

var _ io.WriteCloser = (*streamWriterFuncs)(nil)

func DefaultStreamWriter(w io.Writer, t frameType) io.WriteCloser {
	return NewStreamWriterWithChunkLength(w, t, defaultChunkLength)
}

func NewStreamWriterWithChunkLength(w io.Writer, t frameType, chunkLength uint16) io.WriteCloser {
	var l sync.Mutex
	var wroteFrameType, isClosed bool

	return streamWriterFuncs{
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
				if _, err := writeStreamChunk(w, c); err != nil {
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

			return writeStreamChunk(w, nil)
		},
	}
}

func writeStreamChunk(w io.Writer, b []byte) (int, error) {
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
