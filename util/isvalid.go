package util

import "github.com/pkg/errors"

var ErrInvalid = NewError("invalid")

type IsValider interface {
	IsValid([]byte) error
}

func CheckIsValiders(b []byte, allowNil bool, vs ...IsValider) error { // revive:disable-line:flag-parameter
	for i, v := range vs {
		if err := checkIsValider(b, allowNil, i, v); err != nil {
			return err
		}
	}

	return nil
}

func CheckIsValiderSlice[T IsValider](b []byte, allowNil bool, vs []T) error { // revive:disable-line:flag-parameter
	for i, v := range vs {
		if err := checkIsValider(b, allowNil, i, v); err != nil {
			return err
		}
	}

	return nil
}

func checkIsValider(b []byte, allowNil bool, i int, v IsValider) error { // revive:disable-line:flag-parameter
	if v == nil {
		if allowNil {
			return nil
		}

		return ErrInvalid.Errorf("%dth: nil found", i)
	}

	if err := v.IsValid(b); err != nil {
		if !errors.Is(err, ErrInvalid) {
			return ErrInvalid.Wrap(err)
		}

		return err
	}

	return nil
}

type DummyIsValider func([]byte) error

func (iv DummyIsValider) IsValid(b []byte) error {
	return iv(b)
}
