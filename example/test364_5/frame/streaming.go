package frame

import (
	"io"
	"sync"

	"github.com/pkg/errors"
)

func NewStreamingReader(r io.Reader) (
	ioReadFunc,
	error,
) {
	var chunkSize int
	var c uint32

	switch _, err := ReadSize(r, &c); {
	case err != nil && !errors.Is(err, io.EOF):
		return nil, err
	case errors.Is(err, io.EOF) && c != 0:
		return nil, err
	case c == 0:
		return func([]byte) (int, error) { return 0, io.EOF }, nil
	default:
		chunkSize = int(c)
	}

	var gerr error
	var left []byte
	var isEOF bool

	var l sync.Mutex

	return func(p []byte) (int, error) {
			l.Lock()
			defer l.Unlock()

			switch {
			case gerr != nil:
				return 0, gerr
			case len(p) < 1:
				return 0, nil
			}

			nleft, np, l := len(left), len(p), 0

			copy(p, left)

			if nleft < np && isEOF {
				return nleft, io.EOF
			}

			switch {
			case np < nleft:
				left = left[:np]
				l = np
			default:
				l = nleft
			}

			left = left[:0]

			for l < np {
				var b []byte

				switch i, err := readStreamingChunk(r, chunkSize); {
				case err == nil, errors.Is(err, io.EOF):
					b = i
					isEOF = errors.Is(err, io.EOF)
				default:
					gerr = err

					return 0, err
				}

				copy(p[l:], b)

				switch {
				case len(b) > np-l:
					left = b[np-l:]
					l = np
				default:
					l += len(b)
				}

				if isEOF {
					break
				}
			}

			if len(left) < 1 && isEOF {
				gerr = io.EOF
			}

			return l, gerr
		},
		nil
}

func readStreamingChunk(r io.Reader, chunkSize int) ([]byte, error) {
	var t streamingChunkType

	switch i, err := readStreamingChunkType(r); {
	case err != nil:
		return nil, err
	default:
		t = i
	}

	switch t {
	case bodyStreamingChunkType:
		switch p, _, err := ensureRead(r, uint32(chunkSize)); {
		case err != nil:
			return nil, errors.Errorf("insufficient body chunk")
		default:
			return p, nil
		}
	case eofStreamingChunkType:
	default:
		return nil, errors.Errorf("unexpected chunk type, %d", t)
	}

	switch p, err := readFixedBytes(r); {
	case err != nil && !errors.Is(err, io.EOF):
		return nil, err
	default:
		return p, io.EOF
	}
}

func readStreamingChunkType(r io.Reader) (t streamingChunkType, _ error) {
	i, err := readTypeByte(r)
	switch {
	case err != nil && !errors.Is(err, io.EOF):
		return t, errors.Wrap(err, "read chunk type")
	default:
		t = streamingChunkType(i)
	}

	if err := t.IsValid(nil); err != nil {
		return t, err
	}

	return t, err
}

func NewStreamingWriter(w io.Writer, chunkSize int) (
	func([]byte) (int, error),
	func() error,
	error,
) {
	if chunkSize < 1 {
		return nil, nil, errors.Errorf("zero chunk size")
	}

	var gerr error
	var left []byte
	var headerWrote bool
	var l sync.Mutex

	return func(b []byte) (int, error) {
			l.Lock()
			defer l.Unlock()

			if gerr != nil {
				return 0, gerr
			}

			if len(b) < 1 {
				return 0, nil
			}

			if !headerWrote {
				if gerr = streamingWriterWriteHeader(w, chunkSize); gerr != nil {
					return 0, gerr
				}

				headerWrote = true
			}

			switch i, _, err := streamingWriterWrite(w, chunkSize, left, b); {
			case err != nil:
				defer clearSlice(left)

				return 0, err
			default:
				left = i

				return len(b), nil
			}
		},
		func() error {
			l.Lock()
			defer l.Unlock()

			if gerr != nil {
				return gerr
			}

			if !headerWrote {
				gerr = streamingWriterWriteHeader(w, 0)

				return gerr
			}

			gerr = streamingWriterClose(w, chunkSize, left)

			return gerr
		},
		nil
}

func streamingWriterWriteHeader(w io.Writer, chunkSize int) error {
	c := make([]byte, 5)
	defer clearSlice(c)

	c[0] = byte(streamingFrameType)

	switch i, err := sizeToBytes(uint32(chunkSize)); {
	case err != nil:
		return err
	default:
		copy(c[1:], i)

		_, err := w.Write(c)

		return errors.Wrap(err, "write streaming header")
	}
}

func streamingWriterWrite(w io.Writer, chunkSize int, left, b []byte) ([]byte, int, error) {
	var l int
	var c []byte
	defer clearSlice(c)

	for len(b)-l >= chunkSize {
		c = make([]byte, 1+chunkSize)
		c[0] = byte(bodyStreamingChunkType)

		copy(c[1:], left)
		copy(c[1+len(left):], b[l:])

		if _, err := w.Write(c); err != nil {
			return nil, 0, errors.WithStack(err)
		}

		switch i := len(c) - len(left) - 1; {
		case i > len(b[l:]):
			l += len(b[l:])
		default:
			l += i
		}

		if len(left) > 0 {
			left = left[:0]
		}
	}

	left = b[l:]

	return left, l, nil
}

func streamingWriterClose(w io.Writer, chunkSize int, left []byte) error {
	nleft := len(left)

	c := make([]byte, nleft+5) // NOTE <chunk type>(1) + <uint32 bytes size>(4) = 5
	defer clearSlice(c)

	c[0] = byte(eofStreamingChunkType)

	switch i, err := sizeToBytes(uint32(nleft)); {
	case err != nil:
		return err
	default:
		copy(c[1:], i)
	}

	if nleft > 0 {
		copy(c[5:], left)
	}

	_, err := w.Write(c)

	return errors.Wrap(err, "close straming frame")
}
