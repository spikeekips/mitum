package jsonenc

import (
	"encoding"
	"encoding/json"
	"reflect"

	"github.com/pkg/errors"
	"github.com/spikeekips/mitum/util"
	"github.com/spikeekips/mitum/util/encoder"
	"github.com/spikeekips/mitum/util/hint"
)

var JSONEncoderHint = hint.MustNewHint("json-encoder-v0.0.1")

type Encoder struct {
	decoders *hint.CompatibleSet
}

func NewEncoder() *Encoder {
	return &Encoder{
		decoders: hint.NewCompatibleSet(),
	}
}

func (*Encoder) Hint() hint.Hint {
	return JSONEncoderHint
}

func (enc *Encoder) Add(d encoder.DecodeDetail) error {
	if err := d.IsValid(nil); err != nil {
		return util.InvalidError.Wrapf(err, "failed to add in json encoder")
	}

	return enc.addDecodeDetail(d)
}

func (enc *Encoder) AddHinter(hr hint.Hinter) error {
	if err := hr.Hint().IsValid(nil); err != nil {
		return util.InvalidError.Wrapf(err, "failed to add in json encoder")
	}

	return enc.addDecodeDetail(enc.analyzeHinter(hr))
}

func (*Encoder) Marshal(v interface{}) ([]byte, error) {
	return util.MarshalJSON(v)
}

func (*Encoder) Unmarshal(b []byte, v interface{}) error {
	return util.UnmarshalJSON(b, v)
}

func (enc *Encoder) Decode(b []byte) (interface{}, error) {
	if isNil(b) {
		return nil, nil
	}

	ht, err := enc.guessHint(b)
	if err != nil {
		return nil, errors.Wrapf(err, "failed to guess hint in json decoders")
	}

	return enc.decode(b, ht)
}

func (enc *Encoder) DecodeWithHint(b []byte, ht hint.Hint) (interface{}, error) {
	if isNil(b) {
		return nil, nil
	}

	return enc.decode(b, ht)
}

func (enc *Encoder) DecodeSlice(b []byte) ([]interface{}, error) {
	if isNil(b) {
		return nil, nil
	}

	var j []json.RawMessage
	if err := util.UnmarshalJSON(b, &j); err != nil {
		return nil, errors.Wrap(err, "failed to decode slice in json decoders")
	}

	s := make([]interface{}, len(j))
	for i := range j {
		k, err := enc.Decode(j[i])
		if err != nil {
			return nil, errors.Wrap(err, "failed to decode slice in json decoders")
		}

		s[i] = k
	}

	return s, nil
}

func (enc *Encoder) addDecodeDetail(d encoder.DecodeDetail) error {
	if err := enc.decoders.Add(d.Hint, d); err != nil {
		return util.InvalidError.Wrapf(err, "failed to add DecodeDetail in json encoder")
	}

	return nil
}

func (enc *Encoder) decode(b []byte, ht hint.Hint) (interface{}, error) {
	d, err := enc.findDecoder(ht)
	if err != nil {
		return nil, err
	}

	i, err := d.Decode(b, ht)
	if err != nil {
		return nil, errors.Wrapf(err, "failed to decode, %q in json decoders", ht)
	}

	return i, nil
}

func (enc *Encoder) findDecoder(ht hint.Hint) (encoder.DecodeDetail, error) {
	v := enc.decoders.Find(ht)
	if v == nil {
		return encoder.DecodeDetail{},
			util.NotFoundError.Errorf("failed to find decoder by hint, %q in json decoders", ht)
	}

	d, ok := v.(encoder.DecodeDetail)
	if !ok {
		return encoder.DecodeDetail{},
			errors.Errorf("failed to find decoder by hint in json decoders, %q; not DecodeDetail, %T", ht, v)
	}

	return d, nil
}

func (*Encoder) guessHint(b []byte) (hint.Hint, error) {
	var head hint.HintedJSONHead
	if err := util.UnmarshalJSON(b, &head); err != nil {
		return head.H, err
	}

	if err := head.H.IsValid(nil); err != nil {
		return head.H, err
	}

	return head.H, nil
}

func (enc *Encoder) analyzeHinter(hr hint.Hinter) encoder.DecodeDetail {
	d := encoder.DecodeDetail{Hint: hr.Hint()}

	ptr, elem := encoder.Ptr(hr)
	switch ptr.Interface().(type) {
	case Decodable:
		d.Desc = "JSONDecodable"
		d.Decode = func(b []byte, _ hint.Hint) (interface{}, error) {
			i := reflect.New(elem.Type()).Interface()

			if err := i.(Decodable).DecodeJSON(b, enc); err != nil {
				return nil, err
			}

			return reflect.ValueOf(i).Elem().Interface(), nil
		}
	case json.Unmarshaler:
		d.Desc = "JSONUnmarshaler"
		d.Decode = func(b []byte, _ hint.Hint) (interface{}, error) {
			i := reflect.New(elem.Type()).Interface()

			if err := i.(json.Unmarshaler).UnmarshalJSON(b); err != nil {
				return nil, err
			}

			return reflect.ValueOf(i).Elem().Interface(), nil
		}
	case encoding.TextUnmarshaler:
		d.Desc = "TextUnmarshaler"
		d.Decode = func(b []byte, _ hint.Hint) (interface{}, error) {
			i := reflect.New(elem.Type()).Interface()

			if err := i.(encoding.TextUnmarshaler).UnmarshalText(b); err != nil {
				return nil, err
			}

			return reflect.ValueOf(i).Elem().Interface(), nil
		}
	default:
		d.Desc = "native"
		d.Decode = func(b []byte, _ hint.Hint) (interface{}, error) {
			i := reflect.New(elem.Type()).Interface()

			if err := util.UnmarshalJSON(b, i); err != nil {
				return nil, err
			}

			return reflect.ValueOf(i).Elem().Interface(), nil
		}
	}

	return encoder.AnalyzeSetHinter(elem.Interface(), d)
}
