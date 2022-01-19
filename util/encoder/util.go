package encoder

import (
	"reflect"

	"github.com/pkg/errors"
	"github.com/spikeekips/mitum/util/hint"
)

func Ptr(i interface{}) (reflect.Value /* ptr */, reflect.Value /* elem */) {
	elem := reflect.ValueOf(i)
	if elem.Type().Kind() == reflect.Ptr {
		return elem, elem.Elem()
	}

	if elem.CanAddr() {
		return elem.Addr(), elem
	}

	ptr := reflect.New(elem.Type())
	ptr.Elem().Set(elem)

	return ptr, elem
}

func AnalyzeSetHinter(d DecodeDetail, v interface{}) DecodeDetail {
	if _, ok := v.(hint.SetHinter); !ok {
		return d
	}

	oht := v.(hint.Hinter).Hint()

	// NOTE hint.BaseHinter
	var found bool
	if i, j := reflect.TypeOf(v).FieldByName("BaseHinter"); j && i.Type == reflect.TypeOf(hint.BaseHinter{}) {
		found = true
	}

	if !found {
		p := d.Decode
		d.Decode = func(b []byte, ht hint.Hint) (interface{}, error) {
			i, err := p(b, ht)
			if err != nil {
				return i, errors.Wrap(err, "failed to decode")
			}

			if ht.IsEmpty() {
				ht = oht
			}

			return i.(hint.SetHinter).SetHint(ht), nil
		}

		return d
	}

	p := d.Decode
	d.Decode = func(b []byte, ht hint.Hint) (interface{}, error) {
		i, err := p(b, ht)
		if err != nil {
			return i, errors.Wrap(err, "failed to decode")
		}

		n := reflect.New(reflect.TypeOf(i))
		n.Elem().Set(reflect.ValueOf(i))

		v := n.Elem().FieldByName("BaseHinter")
		if !v.IsValid() || !v.CanAddr() {
			return i, nil
		}

		if ht.IsEmpty() {
			ht = oht
		}

		v.Set(reflect.ValueOf(hint.NewBaseHinter(ht)))

		return n.Elem().Interface(), nil
	}

	return d
}
