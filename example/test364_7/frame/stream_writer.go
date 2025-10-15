package frame

import (
	"io"
	"sync"
)

type writeFunc = func([]byte) (int, error)

func newStreamWriterWithChunkLength(w io.Writer, t frameType, chunkLength uint16) (
	writeFunc,
	func() error,
) {
	var l sync.RWMutex
	var wroteFrameType, isClosed bool

	return func(b []byte) (int, error) {
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
		func() error {
			l.Lock()
			defer l.Unlock()

			if isClosed {
				return ErrClosed
			}

			isClosed = true

			if !wroteFrameType {
				switch {
				case t != errorFrameType:
					return nil
				default:
					if _, err := w.Write(frameTypeToBytes(t)); err != nil {
						return err
					}
				}
			}

			if _, err := writeStreamChunk(w, nil); err != nil {
				return err
			}

			return nil
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
