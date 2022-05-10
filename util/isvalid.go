package util

var ErrInvalid = NewError("invalid")

type IsValider interface {
	IsValid([]byte) error
}

func CheckIsValid(b []byte, allowNil bool, vs ...IsValider) error { // revive:disable-line:flag-parameter
	for i, v := range vs {
		if v == nil {
			if allowNil {
				return nil
			}

			return ErrInvalid.Errorf("%dth: nil found", i)
		}

		if err := v.IsValid(b); err != nil {
			return ErrInvalid.Wrap(err)
		}
	}

	return nil
}

type DummyIsValider func([]byte) error

func (iv DummyIsValider) IsValid(b []byte) error {
	return iv(b)
}
