package util

var InvalidError = NewError("invalid")

type IsValider interface {
	IsValid([]byte) error
}

func CheckIsValid(b []byte, allowNil bool, vs ...IsValider) error {
	for i, v := range vs {
		if v == nil {
			if allowNil {
				return nil
			}

			return InvalidError.Errorf("%dth: nil can not be checked", i)
		}
		if err := v.IsValid(b); err != nil {
			return InvalidError.Wrap(err)
		}
	}

	return nil
}
