package frame

import (
	"encoding/binary"
	"fmt"
	"io"

	"github.com/pkg/errors"
)

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
	switch i, err := readType(r); {
	case err != nil:
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

func readTypeByte(r io.Reader) (t byte, _ error) {
	switch p, _, err := ensureRead(r, 1); {
	case err != nil && !errors.Is(err, io.EOF):
		return t, err
	default:
		return p[0], err
	}
}

func readType(r io.Reader) (t frameType, _ error) {
	i, err := readTypeByte(r)
	switch {
	case err != nil && !errors.Is(err, io.EOF):
		return t, errors.Wrap(err, "read frame type")
	default:
		t = frameType(i)
	}

	if err := t.IsValid(nil); err != nil {
		return t, err
	}

	return t, errors.WithStack(err)
}

func ReadSize(r io.Reader, i *uint32) (int, error) {
	switch p, n, err := ensureRead(r, 4); {
	case err != nil && !errors.Is(err, io.EOF):
		return n, err
	default:
		_, err := binary.Decode(p, binary.BigEndian, i)

		return n, errors.WithStack(err)
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

func ensureRead(r io.Reader, size uint32) ([]byte, int, error) {
	if size < 1 {
		return nil, 0, nil
	}

	p := make([]byte, size)

	switch n, err := r.Read(p); {
	case err != nil && !errors.Is(err, io.EOF):
		return p, n, errors.WithStack(err)
	case n != len(p):
		return p, n, errors.Errorf("insufficient read")
	default:
		return p, n, errors.WithStack(err)
	}
}

func readFixedBytes(r io.Reader) ([]byte, error) {
	var size uint32

	if _, err := ReadSize(r, &size); err != nil {
		return nil, errors.Wrap(err, "read frame size")
	}

	if size > 0 {
		p, _, err := ensureRead(r, size)

		return p, err
	}

	return nil, nil
}

func clearSlice[T any](s []T) {
	clear(s)

	s = s[:0]
}
