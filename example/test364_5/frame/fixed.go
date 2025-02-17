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

	if _, err := ReadSize(r, &size); err != nil {
		return nil, errors.Wrap(err, "read frame size")
	}

	return io.LimitReader(r, int64(size)), nil
}

func WriteFixed(w io.Writer, b []byte) error {
	size := len(b)

	if int(size) > math.MaxUint32 {
		return errors.Errorf("size too large; < uint32")
	}

	i := make([]byte, size+5)
	i[0] = byte(fixedLengthFrameType)

	switch s, err := sizeToBytes(uint32(size)); {
	case err != nil:
		return errors.Wrap(err, "append size")
	case size > 0:
		copy(i[1:], s)
		copy(i[1+len(s):], b)

		_, err = w.Write(i)

		return err
	default:
		return nil
	}
}
