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

func CheckSliceDuplicated(s interface{}, key func(interface{}) string) (map[string]interface{}, bool) {
	if s == nil {
		return nil, false
	}

	sl := makeInterfaceSlice(s)
	if sl == nil {
		return nil, false
	}

	m := map[string]interface{}{}

	for i := range sl {
		var k string
		if sl[i] == nil {
			k = ""
		} else {
			k = key(sl[i])
		}

		if _, found := m[k]; found {
			return nil, true
		}

		m[k] = sl[i]
	}

	return m, false
}

func FilterSlices(a interface{}, f func(interface{}) bool) []interface{} {
	as := makeInterfaceSlice(a)
	if as == nil {
		return nil
	}

	ns := make([]interface{}, len(as))
	var index int

	for i := range as {
		if !f(as[i]) {
			continue
		}

		ns[index] = as[i]
		index++
	}

	return ns[:index]
}

func Filter2Slices(a, b interface{}, f func(interface{}, interface{}) bool) []interface{} {
	as := makeInterfaceSlice(a)
	if as == nil {
		return nil
	}

	bs := makeInterfaceSlice(b)
	if bs == nil {
		return nil
	}

	nb := make([]interface{}, len(as))

	var n int

	for i := range as {
		var found bool

		for j := range bs {
			if f(as[i], bs[j]) {
				found = true

				break
			}
		}

		if found {
			continue
		}

		nb[n] = as[i]
		n++
	}

	return nb[:n]
}

func makeInterfaceSlice(s interface{}) []interface{} {
	v := reflect.ValueOf(s)

	switch v.Kind() {
	case reflect.Slice, reflect.Array:
		l := make([]interface{}, v.Len())
		for i := 0; i < v.Len(); i++ {
			l[i] = v.Index(i).Interface()
		}

		return l
	default:
		return nil
	}
}
