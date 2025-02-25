package frame

import (
	"io"
	"iter"
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

	if size > math.MaxUint32 {
		return errors.Errorf("size too large; <= uint32")
	}

	switch s, err := sizeToBytes(uint32(size)); {
	case err != nil:
		return err
	default:
		_, err := w.Write(s)
		if err == nil && size > 0 {
			_, err = w.Write(b)
		}

		return errors.WithStack(err)
	}
}

func ReadFixeds(r io.Reader) (uint32, iter.Seq2[[]byte, error], error) {
	var n uint32

	switch size, err := ReadSize(r, &n); {
	case err != nil:
		return 0, nil, err
	case size < 1, n < 1:
		return 0, func(func([]byte, error) bool) {}, nil
	}

	return n, func(yield func([]byte, error) bool) {
			for i := uint32(0); i < n; i++ {
				b, err := ReadFixed(r)
				if !yield(b, err) {
					return
				}
			}
		},
		nil
}

func WriteFixeds(w io.Writer, n uint32, seq iter.Seq[[]byte]) error {
	if err := WriteSize(w, n); err != nil {
		return err
	}

	var i uint32

	for b := range seq {
		if i == n {
			return errors.Errorf("over size")
		}

		if err := WriteFixed(w, b); err != nil {
			return err
		}

		i++
	}

	if i < n {
		return errors.Errorf("less size")
	}

	return nil
}
