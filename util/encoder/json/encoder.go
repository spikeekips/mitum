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

var JSONEncoderHint = hint.MustNewHint("json-encoder-v2.0.0")

type Encoder struct {
	decoders *hint.CompatibleSet
	pool     util.ObjectPool
}

func NewEncoder() *Encoder {
	return &Encoder{
		decoders: hint.NewCompatibleSet(),
	}
}

func (*Encoder) Hint() hint.Hint {
	return JSONEncoderHint
}

func (enc *Encoder) SetPool(pool util.ObjectPool) *Encoder {
	enc.pool = pool

	return nil
}

func (enc *Encoder) Add(d encoder.DecodeDetail) error {
	if err := d.IsValid(nil); err != nil {
		return util.ErrInvalid.Wrapf(err, "failed to add in json encoder")
	}

	x := d
	if x.Decode == nil {
		x = enc.analyze(d, d.Instance)
	}

	return enc.addDecodeDetail(x)
}

func (enc *Encoder) AddHinter(hr hint.Hinter) error {
	if err := hr.Hint().IsValid(nil); err != nil {
		return util.ErrInvalid.Wrapf(err, "failed to add in json encoder")
	}

	return enc.addDecodeDetail(enc.analyze(encoder.DecodeDetail{Hint: hr.Hint()}, hr))
}

func (*Encoder) Marshal(v interface{}) ([]byte, error) {
	return util.MarshalJSON(v)
}

func (*Encoder) Unmarshal(b []byte, v interface{}) error {
	return util.UnmarshalJSON(b, v)
}

func (enc *Encoder) Decode(b []byte) (interface{}, error) {
	if util.IsNilJSON(b) {
		return nil, nil
	}

	ht, err := enc.guessHint(b)
	if err != nil {
		return nil, errors.WithMessage(err, "failed to guess hint in json decoders")
	}

	return enc.decodeWithHint(b, ht)
}

func (enc *Encoder) DecodeWithHint(b []byte, ht hint.Hint) (interface{}, error) {
	if util.IsNilJSON(b) {
		return nil, nil
	}

	return enc.decodeWithHint(b, ht)
}

func (enc *Encoder) DecodeWithHintType(b []byte, t hint.Type) (interface{}, error) {
	if util.IsNilJSON(b) {
		return nil, nil
	}

	ht, v := enc.decoders.FindBytType(t)
	if v == nil {
		return encoder.DecodeDetail{},
			errors.Errorf("failed to find decoder by type in json decoders, %q", t)
	}

	d, ok := v.(encoder.DecodeDetail)
	if !ok {
		return encoder.DecodeDetail{},
			errors.Errorf("failed to find decoder by type in json decoders, %q; not DecodeDetail, %T", ht, v)
	}

	i, err := d.Decode(b, ht)
	if err != nil {
		return nil, errors.WithMessagef(err, "failed to decode, %q in json decoders", ht)
	}

	return i, nil
}

func (enc *Encoder) DecodeWithFixedHintType(s string, size int) (interface{}, error) {
	if len(s) < 1 {
		return nil, nil
	}

	e := util.StringErrorFunc("failed to decode with fixed hint type")
	if size < 1 {
		return nil, e(nil, "size < 1")
	}

	i, found := enc.poolGet(s)
	if found {
		if i != nil {
			err, ok := i.(error)
			if ok {
				return nil, err
			}
		}

		return i, nil
	}

	i, err := enc.decodeWithFixedHintType(s, size)
	if err != nil {
		enc.poolSet(s, err)

		return nil, err
	}

	enc.poolSet(s, i)

	return i, nil
}

func (enc *Encoder) decodeWithFixedHintType(s string, size int) (interface{}, error) {
	e := util.StringErrorFunc("failed to decode with fixed hint type")

	body, t, err := hint.ParseFixedTypedString(s, size)
	if err != nil {
		return nil, e(err, "failed to parse fixed typed string")
	}

	i, err := enc.DecodeWithHintType([]byte(body), t)
	if err != nil {
		return nil, e(err, "failed to decode with hint type")
	}

	return i, nil
}

func (enc *Encoder) DecodeSlice(b []byte) ([]interface{}, error) {
	if util.IsNilJSON(b) {
		return nil, nil
	}

	var j []json.RawMessage
	if err := util.UnmarshalJSON(b, &j); err != nil {
		return nil, errors.WithMessage(err, "failed to decode slice in json decoders")
	}

	s := make([]interface{}, len(j))

	for i := range j {
		k, err := enc.Decode(j[i])
		if err != nil {
			return nil, errors.WithMessage(err, "failed to decode slice in json decoders")
		}

		s[i] = k
	}

	return s, nil
}

func (enc *Encoder) addDecodeDetail(d encoder.DecodeDetail) error {
	if err := enc.decoders.Add(d.Hint, d); err != nil {
		return util.ErrInvalid.Wrapf(err, "failed to add DecodeDetail in json encoder")
	}

	return nil
}

func (enc *Encoder) decodeWithHint(b []byte, ht hint.Hint) (interface{}, error) {
	v := enc.decoders.Find(ht)
	if v == nil {
		return nil,
			util.ErrNotFound.Errorf("failed to find decoder by hint, %q in json decoders", ht)
	}

	d, ok := v.(encoder.DecodeDetail)
	if !ok {
		return nil,
			errors.Errorf("failed to find decoder by hint in json decoders, %q; not DecodeDetail, %T", ht, v)
	}

	i, err := d.Decode(b, ht)
	if err != nil {
		return nil, errors.WithMessagef(err, "failed to decode, %q in json decoders", ht)
	}

	return i, nil
}

func (*Encoder) guessHint(b []byte) (hint.Hint, error) {
	e := util.StringErrorFunc("failed to guess hint")

	var head hint.HintedJSONHead
	if err := util.UnmarshalJSON(b, &head); err != nil {
		return head.H, e(err, "hint not found in head")
	}

	if err := head.H.IsValid(nil); err != nil {
		return head.H, e(err, "invalid hint")
	}

	return head.H, nil
}

func (enc *Encoder) analyze(d encoder.DecodeDetail, v interface{}) encoder.DecodeDetail {
	e := util.StringErrorFunc("failed to analyze in json encoder")

	ptr, elem := encoder.Ptr(v)

	switch ptr.Interface().(type) {
	case Decodable:
		d.Desc = "JSONDecodable"
		d.Decode = func(b []byte, _ hint.Hint) (interface{}, error) {
			i := reflect.New(elem.Type()).Interface()

			if err := i.(Decodable).DecodeJSON(b, enc); err != nil { //nolint:forcetypeassert //...
				return nil, e(err, "failed to DecodeJSON")
			}

			return reflect.ValueOf(i).Elem().Interface(), nil
		}
	case json.Unmarshaler:
		d.Desc = "JSONUnmarshaler"
		d.Decode = func(b []byte, _ hint.Hint) (interface{}, error) {
			i := reflect.New(elem.Type()).Interface()

			if err := i.(json.Unmarshaler).UnmarshalJSON(b); err != nil { //nolint:forcetypeassert //...
				return nil, e(err, "failed to UnmarshalJSON")
			}

			return reflect.ValueOf(i).Elem().Interface(), nil
		}
	case encoding.TextUnmarshaler:
		d.Desc = "TextUnmarshaler"
		d.Decode = func(b []byte, _ hint.Hint) (interface{}, error) {
			i := reflect.New(elem.Type()).Interface()

			if err := i.(encoding.TextUnmarshaler).UnmarshalText(b); err != nil { //nolint:forcetypeassert //...
				return nil, e(err, "failed to UnmarshalText")
			}

			return reflect.ValueOf(i).Elem().Interface(), nil
		}
	default:
		d.Desc = "native"
		d.Decode = func(b []byte, _ hint.Hint) (interface{}, error) {
			i := reflect.New(elem.Type()).Interface()

			if err := util.UnmarshalJSON(b, i); err != nil {
				return nil, e(err, "failed to native UnmarshalJSON")
			}

			return reflect.ValueOf(i).Elem().Interface(), nil
		}
	}

	return encoder.AnalyzeSetHinter(d, elem.Interface())
}

func (enc *Encoder) poolGet(s string) (interface{}, bool) {
	if enc.pool == nil {
		return nil, false
	}

	return enc.pool.Get(s)
}

func (enc *Encoder) poolSet(s string, v interface{}) {
	if enc.pool == nil {
		return
	}

	enc.pool.Set(s, v)
}
