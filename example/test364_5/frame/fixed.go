package frame

import (
	"io"
	"math"

	"github.com/pkg/errors"
)

func ReadFixed(r io.Reader) ([]byte, error) {
	return readFixedBytes(r)
}

func NewFixedReader(r io.Reader) (io.Reader, error) {
	var size uint32

	switch _, err := ReadSize(r, &size); {
	case err != nil && !errors.Is(err, io.EOF):
		return nil, errors.Wrap(err, "read frame size")
	case size < 1:
		return nil, errors.WithStack(err)
	default:
		return io.LimitReader(r, int64(size)), errors.WithStack(err)
	}
}

func WriteFixed(w io.Writer, b []byte) error {
	size := len(b)

	if int(size) > math.MaxUint32 {
		return errors.Errorf("size too large; < uint32")
	}

	i := make([]byte, size+4)

	switch s, err := sizeToBytes(uint32(size)); {
	case err != nil:
		return errors.Wrap(err, "append size")
	case size > 0:
		copy(i, s)
		copy(i[len(s):], b)

		_, err = w.Write(i)

		return errors.WithStack(err)
	default:
		return nil
	}
}
