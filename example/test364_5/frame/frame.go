package frame

import (
	"encoding/binary"
	"fmt"
	"io"

	"github.com/pkg/errors"
)

var ErrInsufficientRead = errors.New("insufficient read")

type frameType byte

const (
	fixedLengthFrameType frameType = iota + 1
	streamingFrameType
)

func (t frameType) IsValid([]byte) error {
	switch t {
	case fixedLengthFrameType, streamingFrameType:
		return nil
	default:
		return errors.Errorf("invalid type, %d", t)
	}
}

func (t frameType) String() string {
	switch t {
	case fixedLengthFrameType:
		return "fixed-length"
	case streamingFrameType:
		return "streaming"
	default:
		return fmt.Sprintf("<%d>", t)
	}
}

type streamingChunkType byte

const (
	bodyStreamingChunkType streamingChunkType = iota + 10 // NOTE prevent to conflict with FrameType
	eofStreamingChunkType
)

func (t streamingChunkType) IsValid([]byte) error {
	switch t {
	case bodyStreamingChunkType, eofStreamingChunkType:
		return nil
	default:
		return errors.Errorf("invalid streaming chunk type, %d", t)
	}
}

func (t streamingChunkType) String() string {
	switch t {
	case bodyStreamingChunkType:
		return "body"
	case eofStreamingChunkType:
		return "eof"
	default:
		return fmt.Sprintf("<%d>", t)
	}
}

func LoadReader(r io.Reader) (t frameType, _ io.Reader, _ error) {
	switch i, n, err := readFrameType(r); {
	case err != nil && !errors.Is(err, io.EOF),
		n < 1 && errors.Is(err, io.EOF):
		return t, nil, err
	default:
		t = i
	}

	switch t {
	case fixedLengthFrameType:
		i, err := NewFixedReader(r)

		return t, i, err
	case streamingFrameType:
		i, err := NewStreamingReader(r)

		return t, i, err
	default:
		return t, nil, errors.Errorf("unknown frame type, %v", t)
	}
}

func readFrameTypeByte(r io.Reader) (t byte, _ int, _ error) {
	switch p, n, err := ReadSized(r, 1); {
	case err != nil && !errors.Is(err, io.EOF),
		n < 1 && errors.Is(err, io.EOF):
		return t, n, err
	default:
		return p[0], n, err
	}
}

func readFrameType(r io.Reader) (t frameType, _ int, _ error) {
	switch i, n, err := readFrameTypeByte(r); {
	case err != nil && !errors.Is(err, io.EOF),
		n < 1 && errors.Is(err, io.EOF):
		return t, n, err
	default:
		return frameType(i), n, err
	}
}

func ReadSize(r io.Reader, i *uint32) (int, error) {
	switch p, n, err := ReadSized(r, 4); {
	case err != nil && !errors.Is(err, io.EOF),
		n < 1 && errors.Is(err, io.EOF):
		return n, err
	default:
		_, err := binary.Decode(p, binary.BigEndian, i)

		return n, errors.WithStack(err)
	}
}

func EnsureReadSize(r io.Reader, i *uint32) (int, error) {
	switch n, err := ReadSize(r, i); {
	case n < 1 && errors.Is(err, io.EOF):
		return n, ErrInsufficientRead
	default:
		return n, err
	}
}

func sizeToBytes(i uint32) ([]byte, error) {
	b, err := binary.Append(nil, binary.BigEndian, i)

	return b, errors.WithStack(err)
}

func WriteSize(w io.Writer, i uint32) error {
	return errors.WithStack(binary.Write(w, binary.BigEndian, i))
}

type ioReadFunc func([]byte) (int, error)

func (f ioReadFunc) Read(p []byte) (int, error) {
	return f(p)
}

func ReadSized(r io.Reader, size uint32) ([]byte, int, error) {
	if size < 1 {
		return nil, 0, nil
	}

	p := make([]byte, size)

	n, err := ensureRead(r, p)

	return p, n, err
}

func ensureRead(r io.Reader, p []byte) (int, error) {
	var n int
	var gerr error
	var b []byte

	switch {
	case len(p) > 512:
		b = make([]byte, 512)
	default:
		b = make([]byte, len(p))
	}

end:
	for {
		switch i, err := r.Read(b); {
		case err != nil && !errors.Is(err, io.EOF):
			return n + i, errors.WithStack(err)
		default:
			gerr = err

			if i > 0 {
				copy(p[n:], b)

				n += i
			}
		}

		if n == len(p) || errors.Is(gerr, io.EOF) {
			break end
		}

		b = make([]byte, len(p)-n)
	}

	switch {
	case gerr != nil && !errors.Is(gerr, io.EOF),
		n < 1 && errors.Is(gerr, io.EOF):
		return n, gerr
	case n != len(p):
		return n, ErrInsufficientRead
	default:
		return n, gerr
	}
}

func readFixedBytes(r io.Reader) ([]byte, int, error) {
	var size uint32

	switch n, err := ReadSize(r, &size); {
	case err != nil && !errors.Is(err, io.EOF):
		return nil, n, errors.Wrap(err, "read frame size")
	case n < 1 && errors.Is(err, io.EOF):
		return nil, n, err
	case size > 0:
		if errors.Is(err, io.EOF) {
			return nil, n, errors.Wrap(ErrInsufficientRead, "read frame")
		}

		p, m, err := ReadSized(r, size)

		return p, n + m, err
	default:
		return nil, n, err
	}
}

func clearSlice[T any](s []T) {
	clear(s)

	s = s[:0]
}
