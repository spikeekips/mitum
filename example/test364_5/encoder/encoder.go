package encoder

import (
	"slices"

	"github.com/pkg/errors"
	"github.com/spikeekips/mitum/util"
)

type errorFunc = func() error

type Encoder interface {
	Marshal(any) ([]byte, error)
	Unmarshal([]byte) (any, error)
	Decode(any) (any, error)
	DecodeString(string, int) (any, error)
}

func Assign[T any](v any, t *T) error {
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

func AssignSlice[T any](v any, t *[]T) error {
	var vi []any

	switch err := Assign(v, &vi); {
	case err != nil:
		return err
	case len(vi) < 1:
		return nil
	}

	ti := make([]T, len(vi))

	for i, j := range slices.All(vi) {
		if err := Assign(j, &ti[i]); err != nil {
			return err
		}
	}

	*t = ti

	return nil
}

func Unmarshal[T any](enc Encoder, b []byte, t *T) error {
	switch i, err := enc.Unmarshal(b); {
	case err != nil:
		return err
	default:
		return Assign(i, t)
	}
}

func UnmarshalString[T any](enc Encoder, b []byte, t *T, size int) error {
	var s string
	if err := util.UnmarshalJSON(b, &s); err != nil {
		return err
	}

	switch i, err := enc.DecodeString(s, size); {
	case err != nil:
		return err
	default:
		return Assign(i, t)
	}
}

func Decode[T any](enc Encoder, v any, t *T) errorFunc {
	return func() error {
		switch i, err := enc.Decode(v); {
		case err != nil:
			return err
		default:
			return Assign(i, t)
		}
	}
}

func DecodeString[T any](enc Encoder, b []byte, t *T, size int) errorFunc {
	return func() error {
		switch i, err := enc.DecodeString(string(b), size); {
		case err != nil:
			return err
		default:
			return Assign(i, t)
		}
	}
}

func DecodeSlice[T any](enc Encoder, v any, t *[]T) errorFunc {
	return func() error {
		if v == nil {
			return nil
		}

		var vi []any
		switch i, ok := v.([]any); {
		case !ok:
			return errors.Errorf("expected []any, but %T", v)
		case len(i) < 1:
			return nil
		default:
			vi = i
		}

		ti := make([]T, len(vi))

		for i, j := range slices.All(vi) {
			if err := Decode(enc, j, &ti[i])(); err != nil {
				return err
			}
		}

		*t = ti

		return nil
	}
}
