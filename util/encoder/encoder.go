package encoder

import (
	"reflect"

	"github.com/pkg/errors"
	"github.com/spikeekips/mitum/util"
	"github.com/spikeekips/mitum/util/hint"
)

type Encoder interface {
	Add(DecodeDetail) error
	AddHinter(hint.Hinter) error
	Marshal(interface{}) ([]byte, error)                       // NOTE native marshaler func
	Unmarshal([]byte, interface{}) error                       // NOTE native unmarshaler func
	Decode([]byte) (interface{}, error)                        // NOTE decode by hint inside []byte
	DecodeWithHint([]byte, hint.Hint) (interface{}, error)     // NOTE decode []byte by given hint
	DecodeWithHintType([]byte, hint.Type) (interface{}, error) // NOTE decode []byte by given type
	DecodeSlice([]byte) ([]interface{}, error)                 // NOTE decode sliced data
}

type DecodeFunc func([]byte, hint.Hint) (interface{}, error)

type DecodeDetail struct {
	Hint     hint.Hint
	Decode   DecodeFunc
	Desc     string
	Instance interface{}
}

func (d DecodeDetail) IsValid([]byte) error {
	if err := d.Hint.IsValid(nil); err != nil {
		return util.InvalidError.Wrapf(err, "invalid hint in DecodeDetail")
	}

	if d.Decode == nil && d.Instance == nil {
		return util.InvalidError.Errorf("instance and decode func are empty in DecodeDetail")
	}

	return nil
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
