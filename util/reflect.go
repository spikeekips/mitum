package util

import (
	"reflect"

	"github.com/pkg/errors"
)

func ReflectSetInterfaceValue(v, target interface{}) error {
	switch {
	case v == nil:
		return nil
	case target == nil:
		return errors.Errorf("target should be not nil")
	}

	value := reflect.ValueOf(target)
	if value.Type().Kind() != reflect.Ptr {
		return errors.Errorf("target should be pointer")
	}

	elem := value.Elem()

	switch t := elem.Type(); t.Kind() {
	case reflect.Interface:
		if !reflect.TypeOf(v).Implements(t) {
			return errors.Errorf("expected %v, but %T", value.Type().String(), v)
		}
	default:
		if elem.Type() != reflect.TypeOf(v) {
			return errors.Errorf("expected %T, but %T", elem.Interface(), v)
		} else if !elem.CanSet() {
			return errors.Errorf("target can not be set")
		}
	}

	elem.Set(reflect.ValueOf(v))

	return nil
}

func ReflectPtrSetInterfaceValue[T any](v interface{}, t *T) error {
	if v == nil {
		return nil
	}

	ptr := reflect.New(reflect.ValueOf(v).Type()).Interface()
	if err := ReflectSetInterfaceValue(v, ptr); err != nil {
		return err
	}

	return SetInterfaceValue(ptr, t)
}

func AssertInterfaceValue[T any](v interface{}) (t T, _ error) {
	if v == nil {
		return t, errors.Errorf("nil")
	}

	i, ok := v.(T)
	if !ok {
		return t, errors.Errorf("expected %v, but %T", reflect.TypeOf(&t).Elem(), v) //nolint:gocritic //...
	}

	return i, nil
}

func SetInterfaceValue[T any](v interface{}, t *T) error {
	if v == nil {
		return nil
	}

	switch i, err := AssertInterfaceValue[T](v); {
	case err != nil:
		return err
	default:
		*t = i

		return nil
	}
}
