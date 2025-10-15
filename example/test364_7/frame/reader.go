package frame

import (
	"io"
	"iter"

	"github.com/pkg/errors"
)

func DefaultIter(r io.Reader) iter.Seq2[io.Reader, error] {
	return NewIterWithChunkLength(r, defaultChunkLength)
}

func NewIterWithChunkLength(r io.Reader, chunkLength uint16) iter.Seq2[io.Reader, error] {
	return func(yield func(io.Reader, error) bool) {
		for {
			switch t, _, err := readFrameType(r); {
			case err != nil:
				if errors.Is(err, io.EOF) {
					return
				}

				_ = yield(nil, err)

				return
			case t == streamFrameType:
				cr := newStreamChunksReader(r, chunkLength)
				if !func() bool {
					defer func() {
						_, _ = io.ReadAll(cr) // force exhaust if the reader, cr is not used
					}()

					return yield(cr, nil)
				}() {
					return
				}

			case t == errorFrameType:
				switch b, err := io.ReadAll(newStreamChunksReader(r, chunkLength)); {
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
				_ = yield(nil, errors.Errorf("unknown frame type, %v", t))

				return
			}
		}
	}
}
