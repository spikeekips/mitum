package util

import (
	"fmt"
	"reflect"

	"github.com/pkg/errors"
	"gopkg.in/yaml.v3"
)

type YAMLOrderedMap struct {
	m    map[string]interface{}
	keys []string
}

func NewYAMLOrderedMap() *YAMLOrderedMap {
	return &YAMLOrderedMap{m: map[string]interface{}{}}
}

func (m *YAMLOrderedMap) Get(k string) (_ interface{}, found bool) {
	i, found := m.m[k]

	return i, found
}

func (m *YAMLOrderedMap) Set(k string, v interface{}) (isnew bool) {
	_, found := m.m[k]

	m.m[k] = v

	if !found {
		m.keys = append(m.keys, k)
	}

	return !found
}

func (m *YAMLOrderedMap) Delete(k string) (removed bool) {
	_, found := m.m[k]

	m.keys = FilterSlice(m.keys, func(i string) bool {
		return i == k
	})

	delete(m.m, k)

	return found
}

func (m *YAMLOrderedMap) Traverse(f func(k string, v interface{}) bool) {
	for i := range m.keys {
		if k := m.keys[i]; !f(k, m.m[k]) {
			return
		}
	}
}

func (m *YAMLOrderedMap) Len() int {
	return len(m.keys)
}

func (m *YAMLOrderedMap) Clear() {
	m.keys = nil
	m.m = map[string]interface{}{}
}

func (m *YAMLOrderedMap) MarshalYAML() (interface{}, error) {
	rfs := make([]reflect.StructField, len(m.keys))

	for i := range m.keys {
		k := m.keys[i]
		v := m.m[k]

		var t reflect.Type

		switch {
		case v == nil:
			var s *int

			t = reflect.TypeOf(s)
		default:
			t = reflect.TypeOf(v)
		}

		rfs[i] = reflect.StructField{
			Name: fmt.Sprintf("A%d", i),
			Type: t,
			Tag:  reflect.StructTag(fmt.Sprintf("yaml:%q", k)),
		}
	}

	rt := reflect.StructOf(rfs)
	rv := reflect.New(rt)

	for i := range m.keys {
		rf := rv.Elem().FieldByName(fmt.Sprintf("A%d", i))

		if v := m.m[m.keys[i]]; v != nil {
			rf.Set(reflect.ValueOf(v))
		}
	}

	return rv.Interface(), nil
}

func (m *YAMLOrderedMap) UnmarshalYAML(y *yaml.Node) error {
	if k := y.Kind; k != yaml.MappingNode {
		return errors.Errorf("not map type, %d", k)
	}

	if len(y.Content) < 1 {
		m.Clear()

		return nil
	}

	m.keys = make([]string, len(y.Content)/2)
	m.m = map[string]interface{}{}

	for i := range y.Content {
		v := y.Content[i]

		switch {
		case i%2 == 0:
			m.keys[i/2] = v.Value
		default:
			u, err := UnmarshalYAMLNodeWithYAMLOrderedMap(v)
			if err != nil {
				return errors.WithStack(err)
			}

			m.m[m.keys[(i-1)/2]] = u
		}
	}

	return nil
}

func UnmarshalWithYAMLOrderedMap(b []byte) (interface{}, error) {
	var u interface{}
	if err := yaml.Unmarshal(b, &u); err != nil {
		return nil, errors.WithStack(err)
	}

	switch u.(type) {
	case map[string]interface{}:
		var m *YAMLOrderedMap
		if err := yaml.Unmarshal(b, &m); err != nil {
			return nil, errors.WithStack(err)
		}

		return m, nil
	default:
		return u, nil
	}
}

func UnmarshalYAMLNodeWithYAMLOrderedMap(y *yaml.Node) (interface{}, error) {
	if len(y.Value) < 1 && len(y.Content) < 1 {
		return nil, nil
	}

	switch y.Kind {
	case yaml.MappingNode:
		var u *YAMLOrderedMap
		if err := y.Decode(&u); err != nil {
			return nil, errors.WithStack(err)
		}

		return u, nil
	case yaml.SequenceNode:
		u := make([]interface{}, len(y.Content))

		for i := range y.Content {
			j, err := UnmarshalYAMLNodeWithYAMLOrderedMap(y.Content[i])
			if err != nil {
				return nil, errors.WithStack(err)
			}

			u[i] = j
		}

		return u, nil
	default:
		var u interface{}
		if err := y.Decode(&u); err != nil {
			return nil, errors.WithStack(err)
		}

		return u, nil
	}
}
