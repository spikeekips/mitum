package util

import (
	"reflect"

	"github.com/pkg/errors"
)

// BLOCK use encoder.Decode

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
			return errors.Errorf("expoected %T, but %T", elem.Interface(), v)
		}
	default:
		if elem.Type() != reflect.TypeOf(v) {
			return errors.Errorf("expoected %T, but %T", elem.Interface(), v)
		} else if !elem.CanSet() {
			return errors.Errorf("target can not be set")
		}
	}

	elem.Set(reflect.ValueOf(v))

	return nil
}
