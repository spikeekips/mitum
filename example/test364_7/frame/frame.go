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

type chunkLengthType = uint16

var (
	lengthBytesLength int
	maxChunkLength    uint16 = math.MaxUint16
)

func init() {
	b, err := binary.Append(nil, binary.BigEndian, maxChunkLength)
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

func readFrameType(r io.Reader) (frameType, int, error) {
	p := frameTypePool.Get().([]byte)
	defer frameTypePool.Put(p)

	switch n, err := io.ReadFull(r, p); {
	case err != nil && !errors.Is(err, io.EOF):
		return unknownFrameType, n, errors.WithStack(err)
	default:
		return frameType(p[0]), n, err
	}
}

type bytableInts interface {
	~int8 | ~int16 | ~int32 | ~int64 | ~uint8 | ~uint16 | ~uint32 | ~uint64
}

func intToBytes[T bytableInts](i T) ([]byte, error) {
	b, err := binary.Append(nil, binary.BigEndian, i)

	return b, errors.WithStack(err)
}

func bytesToInt[T bytableInts](b []byte) (i T, _ error) {
	_, err := binary.Decode(b, binary.BigEndian, &i)

	return i, errors.WithStack(err)
}

var lengthBytesPool = sync.Pool{
	New: func() any {
		return make([]byte, 0, 8)
	},
}

var chunkLengthBytesPool = sync.Pool{
	New: func() any {
		return make([]byte, 0, lengthBytesLength)
	},
}

func writeChunkLength(w io.Writer, chunkLength chunkLengthType) (int, error) {
	switch i, err := intToBytes(chunkLength); {
	case err != nil:
		return 0, err
	default:
		return w.Write(i)
	}
}

func readChunkLength(r io.Reader) (chunkLengthType, int, error) {
	p := chunkLengthBytesPool.Get().([]byte)
	defer chunkLengthBytesPool.Put(p)

	switch n, err := io.ReadFull(r, p[:lengthBytesLength]); {
	case err != nil && !errors.Is(err, io.EOF):
		return 0, n, errors.WithStack(err)
	case err != nil && errors.Is(err, io.EOF) && n < 1:
		return 0, n, err
	default:
		var i chunkLengthType
		if _, err := binary.Decode(p[:n], binary.BigEndian, &i); err != nil {
			return i, n, errors.WithStack(err)
		}

		return i, n, nil
	}
}
