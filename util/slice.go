package util

import (
	"reflect"
)

func InStringSlice(n string, s []string) bool {
	for _, i := range s {
		if n == i {
			return true
		}
	}

	return false
}

func CheckSliceDuplicated(s interface{}, key func(interface{}) string) bool {
	if s == nil {
		return false
	}

	v := reflect.ValueOf(s)
	var sl []interface{}
	switch v.Kind() {
	case reflect.Invalid:
		return false
	case reflect.Slice, reflect.Array:
		sl = make([]interface{}, v.Len())
		for i := 0; i < v.Len(); i++ {
			sl[i] = v.Index(i).Interface()
		}
	default:
		return false
	}

	m := map[string]struct{}{}
	for i := range sl {
		var k string
		if sl[i] == nil {
			k = ""
		} else {
			k = key(sl[i])
		}

		if _, found := m[k]; found {
			return true
		}

		m[k] = struct{}{}
	}

	return false
}
