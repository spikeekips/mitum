package util

import (
	"encoding/base64"
	"slices"

	"github.com/pkg/errors"
	"github.com/spikeekips/mitum/example/test364_5/encoder"
	"github.com/spikeekips/mitum/util"
	"github.com/spikeekips/mitum/util/valuehash"
)

func AssignJSONFunc[T any](v any, t *T, f func(any) (T, error)) ErrorFunc {
	return func() error {
		if v == nil {
			return nil
		}

		switch i, err := f(v); {
		case err != nil:
			return err
		default:
			return encoder.Assign(i, t)
		}
	}
}

func AssignJSON[T any](v any, t *T) ErrorFunc {
	return func() error {
		if v == nil {
			return nil
		}

		i, ok := v.(T)
		if !ok {
			return errors.Errorf("expected %T, but %T", *t, v)
		}

		*t = i

		return nil
	}
}

func AssignJSONHash(v any, h *util.Hash) ErrorFunc {
	return AssignJSONFunc(v, h, func(v any) (util.Hash, error) {
		s, ok := v.(string)
		if !ok {
			return nil, errors.Errorf("expected string, but %T", v)
		}

		return valuehash.NewBytesFromString(s)
	})
}

func AssignJSONBytes(v any, b *[]byte) ErrorFunc {
	return AssignJSONFunc(v, b, func(v any) ([]byte, error) {
		s, ok := v.(string)
		if !ok {
			return nil, errors.Errorf("expected string, but %T", v)
		}

		return base64.StdEncoding.DecodeString(s)
	})
}

func AssignJSONInts(v, k any) ErrorFunc {
	return func() error {
		if v == nil {
			return errors.Errorf("nil value")
		}

		x, ok := v.(float64)
		if !ok {
			return errors.Errorf("unsupported type, %T", v)
		}

		switch t := k.(type) {
		case *int:
			*t = int(x)
		case *int8:
			*t = int8(x)
		case *int16:
			*t = int16(x)
		case *int32:
			*t = int32(x)
		case *int64:
			*t = int64(x)
		case *uint:
			*t = uint(x)
		case *uint8:
			*t = uint8(x)
		case *uint16:
			*t = uint16(x)
		case *uint32:
			*t = uint32(x)
		case *uint64:
			*t = uint64(x)
		default:
			return errors.Errorf("unsupported target type, %T", k)
		}

		return nil
	}
}

func AssignJSONSlice[T any](v any, t *[]T, f func(any) (T, error)) ErrorFunc {
	return func() error {
		if v == nil {
			return nil
		}

		l, ok := v.([]any)
		switch {
		case !ok:
			return errors.Errorf("expected []any, but %v", v)
		case len(l) < 1:
			return nil
		}

		j := make([]T, len(l))
		for i, k := range slices.All(l) {
			m, err := f(k)
			if err != nil {
				return err
			}

			j[i] = m
		}

		*t = j

		return nil
	}
}
