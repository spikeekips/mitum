package util

import "context"

type (
	InFunc[T, Y any]             func(T) Y
	In2Func[T, Y, X any]         func(T, Y) X
	InErrorFunc[T any]           func(T) error
	In2ErrorFunc[T, Y any]       func(T, Y) error
	In3ErrorFunc[T, Y, X any]    func(T, Y, X) error
	In4ErrorFunc[T, Y, X, Z any] func(T, Y, X, Z) error
	OutFunc[T any]               func() T
	ErrorFunc                    = OutFunc[error]
	OutErrorFunc[T any]          func() (T, error)
	InContextErrorFunc[T any]    func(context.Context, T) error
)

func DummyErrorInFunc(err error) error {
	return err
}

func NewIfErrErrorInFunc(f InErrorFunc[error]) InErrorFunc[error] {
	return func(err error) error {
		if err != nil {
			return f(err)
		}

		return nil
	}
}
