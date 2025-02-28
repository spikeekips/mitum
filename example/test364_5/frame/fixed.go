package frame

import (
	"io"
	"iter"
	"math"
	"reflect"

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

func ReadFixedSlice(r io.Reader) (uint32, iter.Seq2[[]byte, error], error) {
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

func WriteFixedSlice(w io.Writer, n uint32, seq iter.Seq[[]byte]) error {
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

func ReadFixedValue[T any](r io.Reader, i *T, f func([]byte) (T, error)) error {
	b, err := ReadFixed(r)
	if err != nil && !errors.Is(err, io.EOF) {
		return err
	}

	switch v, ferr := f(b); {
	case ferr != nil:
		return ferr
	default:
		if serr := setValue(v, i); serr != nil {
			return serr
		}

		return err
	}
}

func ReadFixedSliceValue[T any](r io.Reader, i *[]T, f func([]byte) (T, error)) error {
	switch n, seq, err := ReadFixedSlice(r); {
	case err != nil:
		return err
	case n < 1:
		return nil
	default:
		l := make([]T, 0, n)

		for b, err := range seq {
			if err != nil {
				return err
			}

			switch i, err := f(b); {
			case err != nil:
				return err
			default:
				l = append(l, i)
			}
		}

		return setValue(l, i)
	}
}

func setValue[T any](v interface{}, t *T) error {
	if v == nil {
		return nil
	}

	i, ok := v.(T)
	if !ok {
		return errors.Errorf("expected %v, but %T", reflect.TypeOf(&t).Elem(), v) //nolint:gocritic //...
	}

	*t = i

	return nil
}
