package encoder

import (
	"io"
	"reflect"

	"github.com/pkg/errors"
	"github.com/spikeekips/mitum/util"
	"github.com/spikeekips/mitum/util/hint"
)

func Ptr(i interface{}) (ptr reflect.Value, elem reflect.Value) {
	switch j, ok := i.(reflect.Value); {
	case ok:
		elem = j
	default:
		elem = reflect.ValueOf(i)
	}

	if elem.Type().Kind() == reflect.Ptr {
		return elem, elem.Elem()
	}

	if elem.CanAddr() {
		return elem.Addr(), elem
	}

	ptr = reflect.New(elem.Type())
	ptr.Elem().Set(elem)

	return ptr, elem
}

func AnalyzeSetHinter(d DecodeDetail, v interface{}) DecodeDetail {
	if _, ok := v.(hint.SetHinter); !ok {
		return d
	}

	orig := reflect.ValueOf(v)
	_, elem := Ptr(orig)
	isptr := orig.Type().Kind() == reflect.Ptr

	p := d.Decode
	oht := v.(hint.Hinter).Hint() //nolint:forcetypeassert //...

	// NOTE hint.BaseHinter
	if i, j := elem.Type().FieldByName("BaseHinter"); j && i.Type == reflect.TypeOf(hint.BaseHinter{}) {
		d.Decode = func(b []byte, ht hint.Hint) (interface{}, error) {
			i, err := p(b, ht)
			if err != nil {
				return i, errors.WithMessage(err, "decode")
			}

			n := reflect.New(elem.Type())

			switch {
			case isptr:
				n.Elem().Set(reflect.ValueOf(i).Elem())
			default:
				n.Elem().Set(reflect.ValueOf(i))
			}

			x := n.Elem().FieldByName("BaseHinter")
			if !x.IsValid() || !x.CanAddr() {
				return i, nil
			}

			if ht.IsEmpty() {
				ht = oht
			}

			x.Set(reflect.ValueOf(hint.NewBaseHinter(ht)))

			if isptr {
				return n.Interface(), nil
			}

			return n.Elem().Interface(), nil
		}

		return d
	}

	d.Decode = func(b []byte, ht hint.Hint) (interface{}, error) {
		i, err := p(b, ht)
		if err != nil {
			return i, errors.WithMessage(err, "decode")
		}

		if ht.IsEmpty() {
			ht = oht
		}

		return i.(hint.SetHinter).SetHint(ht), nil //nolint:forcetypeassert //...
	}

	return d
}

func Decode(enc Encoder, b []byte, v interface{}) error {
	e := util.StringError("decode")

	hinter, err := enc.Decode(b)
	if err != nil {
		return e.Wrap(err)
	}

	if err := util.InterfaceSetValue(hinter, v); err != nil {
		return e.Wrap(err)
	}

	return nil
}

func DecodeReader(enc Encoder, r io.Reader, v interface{}) error {
	e := util.StringError("DecodeReader")

	b, err := io.ReadAll(r)
	if err != nil {
		return e.WithMessage(err, "reader")
	}

	if err := Decode(enc, b, v); err != nil {
		return e.WithMessage(err, "decode")
	}

	return nil
}
