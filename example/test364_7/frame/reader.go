package frame

import (
	"io"
	"iter"

	"github.com/pkg/errors"
)

func DefaultFramesIter(r io.Reader) iter.Seq2[io.Reader, error] {
	return NewFramesIterWithChunkLength(r, defaultChunkLength)
}

func NewFramesIterWithChunkLength(r io.Reader, chunkLength uint16) iter.Seq2[io.Reader, error] {
	return func(yield func(io.Reader, error) bool) {
		for {
			switch t, cr, _, err := NewStreamReaderWithChunkLength(r, chunkLength); {
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
