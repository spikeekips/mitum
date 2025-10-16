package frame

import (
	"io"
	"sync"

	"github.com/pkg/errors"
)

type readerFunc func([]byte) (int, error)

func (r readerFunc) Read(p []byte) (int, error) {
	return r(p)
}

func newStreamChunksReader(r io.Reader, chunkLength uint16) io.Reader {
	var l sync.Mutex
	var left uint16
	var isClosed bool

	return readerFunc(func(p []byte) (int, error) {
		if len(p) < 1 {
			return 0, nil
		}

		l.Lock()
		defer l.Unlock()

		if isClosed {
			return 0, ErrClosed
		}

		var rn, bc int

		for bc < len(p) {
			if left < 1 {
				switch i, _, err := readChunkLength(r); {
				case err != nil:
					isClosed = true

					return rn, err
				case i < 1:
					isClosed = true

					return rn, io.EOF
				default:
					left = i
				}
			}

			c := min(int(left), len(p)-bc)

			n, err := io.ReadFull(r, p[bc:bc+c])
			rn += n

			if err != nil && !errors.Is(err, io.EOF) {
				isClosed = true

				return rn, err
			}

			left -= uint16(n)
			bc += c

			if errors.Is(err, io.EOF) {
				isClosed = true

				if bc < len(p) {
					return rn, io.ErrShortBuffer
				}

				return rn, err
			}
		}

		return rn, nil
	})
}
