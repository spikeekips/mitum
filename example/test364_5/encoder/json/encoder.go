package jsonenc

import (
	"reflect"
	"strconv"

	"github.com/pkg/errors"
	"github.com/spikeekips/mitum/example/test364_5/encoder"
	"github.com/spikeekips/mitum/util"
	"github.com/spikeekips/mitum/util/hint"
)

type (
	decodeMapFunc    = func(*Encoder, map[string]any) (any, error)
	decodeStringFunc = func(*Encoder, string) (any, error)
	decodeJSONMap    interface {
		DecodeJSON(*Encoder, map[string]any) (any, error)
	}
	decodeJSONString interface {
		DecodeJSON(*Encoder, string) (any, error)
	}
)

type Encoder struct {
	set       map[string]any
	cache     util.GCache[string, [2]any /* {decode func, error} */]
	hintCache util.GCache[string, [2]any /* {*hint.Hint, error} }*/]
}

func NewEncoder(cacheSize int) *Encoder {
	return &Encoder{
		set:       map[string]any{},
		cache:     util.NewLRUGCache[string, [2]any](cacheSize),
		hintCache: util.NewLRUGCache[string, [2]any](cacheSize),
	}
}

func AddDecode[T any](enc *Encoder, ht hint.Hint, sample T, decode any) error {
	if decode == nil {
		return errors.Errorf("nil decode")
	}

	if err := ht.IsValid(nil); err != nil {
		return err
	}

	if reflect.TypeOf(sample) == nil {
		return errors.Errorf("nil sample")
	}

	return enc.addDecode(ht, sample, hinterDecodeFunc(ht, sample, decode))
}

func (enc *Encoder) addDecode(ht hint.Hint, sample any, decode any) error {
	decodeKey := enc.decodeKeyWithHint(ht, decode)

	if _, found := enc.set[decodeKey]; found {
		return errors.Errorf("already added, %q(%q)", ht, decodeKey)
	}

	enc.set[decodeKey] = decode

	return nil
}

func (enc *Encoder) Marshal(v any) ([]byte, error) {
	return util.MarshalJSON(v)
}

func (enc *Encoder) Unmarshal(b []byte) (any, error) {
	var inf any
	if err := util.UnmarshalJSON(b, &inf); err != nil {
		return nil, err
	}

	return enc.Decode(inf)
}

func (enc *Encoder) Decode(v any) (any, error) {
	if v == nil {
		return nil, nil
	}

	switch t := v.(type) {
	case map[string]any:
		return enc.decodeMap(t)
	default:
		return nil, errors.Errorf("Decode: unsupported data type, %T", v)
	}
}

func (enc *Encoder) DecodeString(s string, size int) (any, error) {
	var decode decodeStringFunc

	switch b, t, err := hint.ParseFixedTypedString(s, size); {
	case err != nil:
		return nil, err
	default:
		switch i, err := enc.getDecodeFunc(t, 0); {
		case err != nil:
			return nil, err
		default:
			if err := encoder.Assign(i, &decode); err != nil {
				return nil, err
			}
		}

		return decode(enc, b)
	}
}

func (enc *Encoder) decodeMap(m map[string]any) (any, error) {
	var decode decodeMapFunc

	var ht hint.Hint
	switch i, found := m["_hint"]; {
	case !found:
		return nil, errors.Errorf("_hint not found")
	default:
		s, ok := i.(string)
		if !ok {
			return nil, errors.Errorf("expected hint string, but %T", i)
		}

		switch h, err := enc.getHint(s); {
		case err != nil:
			return nil, err
		default:
			ht = h
		}
	}

	switch i, err := enc.getDecodeFunc(ht.Type(), ht.Version().Major()); {
	case err != nil:
		return nil, err
	default:
		if err := encoder.Assign(i, &decode); err != nil {
			return nil, err
		}
	}

	return decode(enc, m)
}

func (enc *Encoder) getHint(s string) (hint.Hint, error) {
	if i, found := enc.hintCache.Get(s); found {
		if i[1] != nil {
			return hint.Hint{}, i[1].(error)
		}

		return *(i[0].(*hint.Hint)), nil
	}

	ht, err := hint.ParseHint(s)
	enc.hintCache.Set(s, [2]any{&ht, err}, 0)

	return ht, err
}

func (enc *Encoder) getDecodeFunc(t hint.Type, v uint64) (any, error) {
	decodeKey := enc.decodeKey(t, v)
	if i, found := enc.cache.Get(decodeKey); found {
		if i[1] != nil {
			return nil, i[1].(error)
		}

		return i[0], nil
	}

	var f any
	var err error

	switch i, found := enc.set[decodeKey]; {
	case !found:
		err = errors.Errorf("decode func not found, %q", t)
	default:
		f = i
	}

	enc.cache.Set(decodeKey, [2]any{f, err}, 0)

	return f, err
}

func (*Encoder) decodeKey(t hint.Type, v uint64) string {
	return t.String() + "-" + strconv.FormatUint(v, 10)
}

func (enc *Encoder) decodeKeyWithHint(ht hint.Hint, decode any) string {
	major := ht.Version().Major()
	if _, ok := decode.(decodeStringFunc); ok {
		major = 0
	}

	return enc.decodeKey(ht.Type(), major)
}

func hinterDecodeFunc[T any](ht hint.Hint, sample T, decode any) any {
	d, ok := decode.(decodeMapFunc)
	if !ok {
		return decode
	}

	var indirect bool

	if _, ok := any(sample).(hint.SetHinter); !ok {
		if _, ok := any(new(T)).(hint.SetHinter); !ok {
			return decode
		}

		indirect = true
	}

	var sethint func(T) (any, error)

	switch {
	case indirect:
		sethint = func(t T) (any, error) {
			n := new(T)
			*n = t

			_ = any(n).(hint.SetHinter).SetHint(ht)

			return *n, nil
		}
	default:
		sethint = func(t T) (any, error) {
			n := any(t).(hint.SetHinter).SetHint(ht)
			if _, ok := n.(T); !ok {
				return nil, errors.Errorf("SetHint; expected %T, but %T", sample, n)
			}

			return n, nil
		}
	}

	return func(enc *Encoder, m map[string]any) (any, error) {
		switch i, err := d(enc, m); {
		case err != nil, i == nil:
			return i, err
		default:
			k, ok := i.(T)
			if !ok {
				return nil, errors.Errorf("decoded; expected %T, but %T", sample, i)
			}

			return sethint(k)
		}
	}
}
