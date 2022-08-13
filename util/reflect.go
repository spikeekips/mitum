package util

import (
	"reflect"

	"github.com/pkg/errors"
)

func InterfaceSetValue(v, target interface{}) error {
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
