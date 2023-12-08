package jsonenc

import (
	"encoding"
	"encoding/json"
	"io"
	"reflect"

	"github.com/pkg/errors"
	"github.com/spikeekips/mitum/util"
	"github.com/spikeekips/mitum/util/encoder"
	"github.com/spikeekips/mitum/util/hint"
)

var JSONEncoderHint = hint.MustNewHint("json-encoder-v0.0.1")

type Encoder struct {
	decoders *hint.CompatibleSet[encoder.DecodeDetail]
	pool     util.ObjectPool
}

func NewEncoder() *Encoder {
	return &Encoder{
		decoders: hint.NewCompatibleSet[encoder.DecodeDetail](1 << 10), //nolint:gomnd // big enough
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
		return util.ErrInvalid.Wrap(err)
	}

	x := d
	if x.Decode == nil {
		x = enc.analyze(d, d.Instance)
	}

	return enc.addDecodeDetail(x)
}

func (enc *Encoder) AddHinter(hr hint.Hinter) error {
	if err := hr.Hint().IsValid(nil); err != nil {
		return util.ErrInvalid.Wrap(err)
	}

	return enc.addDecodeDetail(enc.analyze(encoder.DecodeDetail{Hint: hr.Hint()}, hr))
}

func (*Encoder) Marshal(v interface{}) ([]byte, error) {
	return util.MarshalJSON(v)
}

func (*Encoder) Unmarshal(b []byte, v interface{}) error {
	return util.UnmarshalJSON(b, v)
}

func (*Encoder) StreamEncoder(w io.Writer) util.StreamEncoder {
	return util.NewJSONStreamEncoder(w)
}

func (*Encoder) StreamDecoder(r io.Reader) util.StreamDecoder {
	return util.NewJSONStreamDecoder(r)
}

func (enc *Encoder) Decode(b []byte) (interface{}, error) {
	if util.IsNilJSON(b) {
		return nil, nil
	}

	ht, err := enc.guessHint(b)
	if err != nil {
		return nil, err
	}

	return enc.decodeWithHint(b, ht)
}

func (enc *Encoder) DecodeWithHint(b []byte, ht hint.Hint) (interface{}, error) {
	if util.IsNilJSON(b) {
		return nil, nil
	}

	return enc.decodeWithHint(b, ht.String())
}

func (enc *Encoder) DecodeWithHintType(b []byte, t hint.Type) (interface{}, error) {
	if util.IsNilJSON(b) {
		return nil, nil
	}

	ht, d, found := enc.decoders.FindBytType(t)
	if !found {
		return nil, errors.Errorf("find decoder by type, %q", t)
	}

	i, err := d.Decode(b, ht)
	if err != nil {
		return nil, errors.WithMessagef(err, "decode by hint, %q", ht)
	}

	return i, nil
}

func (enc *Encoder) DecodeWithFixedHintType(s string, size int) (interface{}, error) {
	if len(s) < 1 {
		return nil, nil
	}

	e := util.StringError("decode with fixed hint type")

	if size < 1 {
		return nil, e.Errorf("size < 1")
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
	body, t, err := hint.ParseFixedTypedString(s, size)
	if err != nil {
		return nil, err
	}

	i, err := enc.DecodeWithHintType([]byte(body), t)
	if err != nil {
		return nil, err
	}

	return i, nil
}

func (enc *Encoder) DecodeSlice(b []byte) ([]interface{}, error) {
	if util.IsNilJSON(b) {
		return nil, nil
	}

	var j []json.RawMessage
	if err := util.UnmarshalJSON(b, &j); err != nil {
		return nil, errors.WithMessage(err, "decode slice")
	}

	s := make([]interface{}, len(j))

	for i := range j {
		k, err := enc.Decode(j[i])
		if err != nil {
			return nil, errors.WithMessage(err, "decode slice")
		}

		s[i] = k
	}

	return s, nil
}

func (enc *Encoder) addDecodeDetail(d encoder.DecodeDetail) error {
	if err := enc.decoders.Add(d.Hint, d); err != nil {
		return util.ErrInvalid.Wrap(err)
	}

	return nil
}

func (enc *Encoder) decodeWithHint(b []byte, s string) (interface{}, error) {
	ht, d, found, err := enc.decoders.FindByString(s)
	if err != nil {
		return nil, util.ErrNotFound.WithMessage(err, "find decoder by hint, %q", s)
	}

	if !found {
		return nil, util.ErrNotFound.Errorf("find decoder by hint, %q", s)
	}

	i, err := d.Decode(b, ht)
	if err != nil {
		return nil, errors.WithMessagef(err, "decode by hint, %q", ht)
	}

	return i, nil
}

func (*Encoder) guessHint(b []byte) (string, error) {
	var head hint.HintedJSONHead
	if err := util.UnmarshalJSON(b, &head); err != nil {
		return head.H, errors.WithMessagef(err, "hint not found in head")
	}

	return head.H, nil
}

func (enc *Encoder) analyze(d encoder.DecodeDetail, v interface{}) encoder.DecodeDetail {
	e := util.StringError("analyze")

	orig := reflect.ValueOf(v)
	ptr, elem := encoder.Ptr(orig)

	tointerface := func(i interface{}) interface{} {
		return reflect.ValueOf(i).Elem().Interface()
	}

	if orig.Type().Kind() == reflect.Ptr {
		tointerface = func(i interface{}) interface{} {
			return i
		}
	}

	switch ptr.Interface().(type) {
	case Decodable:
		d.Desc = "JSONDecodable"
		d.Decode = func(b []byte, _ hint.Hint) (interface{}, error) {
			i := reflect.New(elem.Type()).Interface()

			if err := i.(Decodable).DecodeJSON(b, enc); err != nil { //nolint:forcetypeassert //...
				return nil, e.WithMessage(err, "DecodeJSON")
			}

			return tointerface(i), nil
		}
	case json.Unmarshaler:
		d.Desc = "JSONUnmarshaler"
		d.Decode = func(b []byte, _ hint.Hint) (interface{}, error) {
			i := reflect.New(elem.Type()).Interface()

			if err := i.(json.Unmarshaler).UnmarshalJSON(b); err != nil { //nolint:forcetypeassert //...
				return nil, e.WithMessage(err, "UnmarshalJSON")
			}

			return tointerface(i), nil
		}
	case encoding.TextUnmarshaler:
		d.Desc = "TextUnmarshaler"
		d.Decode = func(b []byte, _ hint.Hint) (interface{}, error) {
			i := reflect.New(elem.Type()).Interface()

			if err := i.(encoding.TextUnmarshaler).UnmarshalText(b); err != nil { //nolint:forcetypeassert //...
				return nil, e.WithMessage(err, "UnmarshalText")
			}

			return tointerface(i), nil
		}
	default:
		d.Desc = "native"
		d.Decode = func(b []byte, _ hint.Hint) (interface{}, error) {
			i := reflect.New(elem.Type()).Interface()

			if err := util.UnmarshalJSON(b, i); err != nil {
				return nil, e.WithMessage(err, "native UnmarshalJSON")
			}

			return tointerface(i), nil
		}
	}

	return enc.analyzeExtensible(
		encoder.AnalyzeSetHinter(d, orig.Interface()),
		ptr,
	)
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

	enc.pool.Set(s, v, nil)
}

func (*Encoder) analyzeExtensible(d encoder.DecodeDetail, ptr reflect.Value) encoder.DecodeDetail {
	p := d.Decode

	if i, j := reflect.TypeOf(ptr.Elem()).FieldByName("DefaultExtensibleJSON"); j &&
		i.Type == reflect.TypeOf(util.DefaultExtensibleJSON{}) {
		d.Decode = func(b []byte, ht hint.Hint) (interface{}, error) {
			i, err := p(b, ht)
			if err != nil {
				return i, err
			}

			if i == nil {
				return i, nil
			}

			n := reflect.New(reflect.TypeOf(i))
			n.Elem().Set(reflect.ValueOf(i))

			x := n.Elem().FieldByName("DefaultExtensibleJSON")
			if !x.IsValid() || !x.CanAddr() {
				return i, nil
			}

			de := util.DefaultExtensibleJSON{}
			de.SetMarshaledJSON(b)

			x.Set(reflect.ValueOf(de))

			return n.Elem().Interface(), nil
		}

		return d
	}

	if _, ok := ptr.Interface().(util.ExtensibleJSONSetter); !ok {
		return d
	}

	d.Decode = func(b []byte, ht hint.Hint) (interface{}, error) {
		i, err := p(b, ht)
		if err != nil {
			return i, err
		}

		if i == nil {
			return i, nil
		}

		uptr, _ := encoder.Ptr(i)

		switch j, ok := uptr.Interface().(util.ExtensibleJSONSetter); {
		case !ok:
			return i, nil
		default:
			j.SetMarshaledJSON(b) //nolint:forcetypeassert //...

			return uptr.Elem().Interface(), nil
		}
	}

	return d
}
