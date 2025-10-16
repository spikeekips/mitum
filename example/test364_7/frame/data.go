package frame

import (
	"encoding/binary"
	"io"
	"sync"

	"github.com/pkg/errors"
)

func writeDataLength(w io.Writer, i int64) (int, error) {
	b, err := binary.Append(nil, binary.BigEndian, i)
	if err != nil {
		return 0, errors.WithStack(err)
	}

	return w.Write(b)
}

func readDataLength(r io.Reader) (int64, int, error) {
	p := lengthBytesPool.Get().([]byte)
	defer lengthBytesPool.Put(p)

	switch n, err := io.ReadFull(r, p[:cap(p)]); {
	case err != nil && !errors.Is(err, io.EOF):
		return 0, n, errors.WithStack(err)
	case err != nil && errors.Is(err, io.EOF) && n < 1:
		return 0, n, err
	default:
		i, err := bytesToInt[int64](p[:n])

		return i, n, err
	}
}

func WriteBytes(w io.Writer, b []byte) (int, error) {
	bl := int64(len(b))
	if _, err := writeDataLength(w, bl); err != nil {
		return 0, err
	}

	if bl < 1 {
		return 0, nil
	}

	_, err := w.Write(b)

	return len(b), err
}

func ReadBytes(r io.Reader) (int64, io.Reader, error) {
	switch l, _, err := readDataLength(r); {
	case err != nil:
		return 0, nil, err
	case l < 1:
		return l, nil, nil
	default:
		return l, &io.LimitedReader{R: r, N: l}, nil
	}
}

func WriteUint64(w io.Writer, u uint64) (int, error) {
	switch b, err := intToBytes(u); {
	case err != nil:
		return 0, err
	default:
		return w.Write(b)
	}
}

var buint64Pool = sync.Pool{
	New: func() any {
		return make([]byte, 0, 8)
	},
}

func ReadUint64(r io.Reader) (uint64, int, error) {
	p := lengthBytesPool.Get().([]byte)
	defer func() {
		lengthBytesPool.Put(p)
	}()

	switch n, err := io.ReadFull(r, p[:cap(p)]); {
	case err != nil:
		return 0, n, err
	default:
		u, err := bytesToInt[uint64](p[:n])

		return u, n, err
	}
}
