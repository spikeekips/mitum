package frame

import (
	stderrors "errors"
	"io"
	"iter"
	"math"
	"slices"

	"github.com/pkg/errors"
)

func ReadFixed(r io.Reader) ([]byte, int, error) {
	return readFixedBytes(r)
}

func EnsureReadFixed(r io.Reader) ([]byte, int, error) {
	switch b, n, err := readFixedBytes(r); {
	case n < 1 && errors.Is(err, io.EOF):
		return b, n, ErrInsufficientRead
	default:
		return b, n, err
	}
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
	case size < 1:
		_, err = w.Write(s)

		return errors.WithStack(err)
	default:
		p := make([]byte, len(s)+len(b))
		copy(p, s)
		copy(p[len(s):], b)

		_, err = w.Write(p)

		return errors.WithStack(err)
	}
}

func ReadFixedSlice(r io.Reader) (uint32, iter.Seq2[[]byte, error], error) {
	var size uint32

	switch n, err := ReadSize(r, &size); {
	case err != nil && !errors.Is(err, io.EOF):
		return 0, nil, err
	case n < 1 && errors.Is(err, io.EOF):
		return 0, nil, err
	case size < 1:
		return 0, func(func([]byte, error) bool) {}, err
	}

	return size, func(yield func([]byte, error) bool) {
			for i := uint32(0); i < size; i++ {
				b, n, err := ReadFixed(r)

				if n < 1 && errors.Is(err, io.EOF) {
					err = ErrInsufficientRead
				}

				if !yield(b, err) || err != nil {
					return
				}
			}
		},
		nil
}

func WriteFixedSlice(w io.Writer, n uint32, seq iter.Seq2[[]byte, error]) error {
	if err := WriteSize(w, n); err != nil {
		return err
	}

	var i uint32

	for b, err := range seq {
		switch {
		case i == n:
			return errors.Errorf("over size")
		case err != nil:
			return err
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

func SimpleWriteFixedSliceSeq(bs [][]byte) iter.Seq2[[]byte, error] {
	return func(yield func([]byte, error) bool) {
		for b := range slices.Values(bs) {
			if !yield(b, nil) {
				return
			}
		}
	}
}

func ReadFixedValue[T any](r io.Reader, i *T, f func([]byte) (T, error)) (int, error) {
	switch b, n, err := ReadFixed(r); {
	case err != nil && !errors.Is(err, io.EOF),
		n < 1 && errors.Is(err, io.EOF):
		return n, err
	default:
		switch v, ferr := f(b); {
		case ferr != nil:
			return n, stderrors.Join(err, ferr)
		default:
			return n, stderrors.Join(err, setValue(v, i))
		}
	}
}

func EnsureReadFixedValue[T any](r io.Reader, i *T, f func([]byte) (T, error)) (int, error) {
	switch b, n, err := EnsureReadFixed(r); {
	case err != nil && !errors.Is(err, io.EOF):
		return n, err
	default:
		switch v, ferr := f(b); {
		case ferr != nil:
			return n, stderrors.Join(err, ferr)
		default:
			return n, stderrors.Join(err, setValue(v, i))
		}
	}
}

func ReadFixedSliceValue[T any](r io.Reader, i *[]T, f func([]byte) (T, error)) error {
	switch size, seq, err := ReadFixedSlice(r); {
	case err != nil, size < 1:
		return err
	default:
		l := make([]T, 0, size)

		for b, err := range seq {
			if err != nil && !errors.Is(err, io.EOF) {
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
		var o T

		return errors.Errorf("expected %T, but %T", o, v)
	}

	*t = i

	return nil
}
