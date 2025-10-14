package frame

import (
	"io"
	"iter"
	"sync"

	"github.com/pkg/errors"
)

type ReaderFunc func([]byte) (int, error)

func (r ReaderFunc) Read(p []byte) (int, error) {
	return r(p)
}

func DefaultReader(r io.Reader) (frameType, ReaderFunc, int, error) {
	return NewReaderWithChunkLength(r, defaultChunkLength)
}

func NewReaderWithChunkLength(r io.Reader, chunkLength uint16) (frameType, ReaderFunc, int, error) {
	switch t, n, err := readFrameType(r); {
	case err != nil:
		return t, nil, n, err
	default:
		return t, newChunksReader(r, chunkLength), n, nil
	}
}

func newChunksReader(r io.Reader, chunkLength uint16) ReaderFunc {
	var l sync.Mutex
	var left uint16
	var isClosed bool

	return func(p []byte) (int, error) {
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
				switch i, _, err := readLength(r); {
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
	}
}

func DefaultFramesIter(r io.Reader) iter.Seq2[ReaderFunc, error] {
	return NewFramesIterWithChunkLength(r, defaultChunkLength)
}

func NewFramesIterWithChunkLength(r io.Reader, chunkLength uint16) iter.Seq2[ReaderFunc, error] {
	return func(yield func(ReaderFunc, error) bool) {
		for {
			switch t, cr, _, err := NewReaderWithChunkLength(r, chunkLength); {
			case err != nil:
				if cr == nil && errors.Is(err, io.EOF) {
					return
				}

				_ = yield(nil, err)

				return
			case t == errorFrameType:
				switch b, err := io.ReadAll(cr); {
				case err != nil:
					_ = yield(nil, err)

					return
				case len(b) < 1: // nil error
					return
				default:
					_ = yield(nil, errors.New(string(b)))

					return
				}
			default:
				if !func() bool {
					defer func() {
						_, _ = io.ReadAll(cr) // force exhaust if the reader, cr is not used
					}()

					return yield(cr, nil)
				}() {
					return
				}
			}
		}
	}
}
