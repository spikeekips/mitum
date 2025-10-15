package frame

import (
	"encoding/binary"
	"io"
	"math"
	"sync"

	"github.com/pkg/errors"
)

var ErrClosed = errors.New("closed")

type frameType byte

var (
	unknownFrameType frameType = 0x00
	streamFrameType  frameType = 0x01
	errorFrameType   frameType = 0x02
)

var lengthBytesLength int

var defaultChunkLength uint16 = math.MaxUint16

func init() {
	b, err := binary.Append(nil, binary.BigEndian, defaultChunkLength)
	if err != nil {
		panic(err)
	}

	lengthBytesLength = len(b)
}

func frameTypeToBytes(t frameType) []byte {
	return []byte{byte(t)}
}

var frameTypePool = sync.Pool{
	New: func() any {
		return make([]byte, 1)
	},
}

func frameTypePoolPut(b []byte) {
	clear(b)

	frameTypePool.Put(b)
}

func readFrameType(r io.Reader) (frameType, int, error) {
	p := frameTypePool.Get().([]byte)
	defer frameTypePoolPut(p)

	switch n, err := io.ReadFull(r, p); {
	case err != nil && !errors.Is(err, io.EOF):
		return unknownFrameType, n, withStack(err)
	default:
		return frameType(p[0]), n, err
	}
}

func lengthToBytes(i uint16) ([]byte, error) {
	b, err := binary.Append(nil, binary.BigEndian, i)

	return b, errors.WithStack(err)
}

func bytesToLength(b []byte) (i uint16, _ error) {
	_, err := binary.Decode(b, binary.BigEndian, &i)

	return i, errors.WithStack(err)
}

var chunkLengthPool = sync.Pool{
	New: func() any {
		return make([]byte, lengthBytesLength)
	},
}

func chunkLengthPoolPut(b []byte) {
	clear(b)

	chunkLengthPool.Put(b)
}

func readLength(r io.Reader) (uint16, int, error) {
	p := chunkLengthPool.Get().([]byte)
	defer chunkLengthPoolPut(p)

	switch n, err := io.ReadFull(r, p); {
	case err != nil && !errors.Is(err, io.EOF):
		return 0, n, withStack(err)
	case err != nil && errors.Is(err, io.EOF) && n < 1:
		return 0, n, err
	default:
		i, err := bytesToLength(p)

		return i, n, err
	}
}

func withStack(err error) error {
	if err != nil && !errors.Is(err, io.EOF) {
		return errors.WithStack(err)
	}

	return err
}
