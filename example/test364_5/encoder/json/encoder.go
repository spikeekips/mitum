package jsonenc

import (
	"fmt"
	"reflect"

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
	set       map[hint.Type]map[uint64]any
	cache     util.GCache[string, [2]any /* {decode func, error} */]
	hintCache util.GCache[string, [2]any /* {hint.Hint, error} }*/]
}

func NewEncoder(cacheSize int) *Encoder {
	return &Encoder{
		set:       map[hint.Type]map[uint64]any{},
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

	decode = hinterDecodeFunc(ht, sample, decode)

	return enc.AddDecode(ht, sample, decode)
}

func (enc *Encoder) AddDecode(ht hint.Hint, sample any, decode any) error {
	major := ht.Version().Major()
	if _, ok := decode.(decodeStringFunc); ok {
		major = 0
	}

	var m map[uint64]any

	switch i, found := enc.set[ht.Type()]; {
	case !found:
		m = map[uint64]any{}
		enc.set[ht.Type()] = m
	default:
		if _, found := i[major]; found {
			return errors.Errorf("already added, %q(major=%d)", ht, major)
		}

		m = i
	}

	m[major] = decode

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

		return i[0].(hint.Hint), nil
	}

	ht, err := hint.ParseHint(s)
	enc.hintCache.Set(s, [2]any{ht, err}, 0)

	return ht, err
}

func (enc *Encoder) getDecodeFunc(t hint.Type, v uint64) (any, error) {
	cachekey := fmt.Sprintf("%s-%d", t, v)
	if i, found := enc.cache.Get(cachekey); found {
		if i[1] != nil {
			return nil, i[1].(error)
		}

		return i[0], nil
	}

	var f any
	var err error

	switch m, found := enc.set[t]; {
	case !found:
		err = errors.Errorf("decode func not found, %q", t)
	default:
		i, found := m[v]
		if !found {
			err = errors.Errorf("no compatible version, %d", v)

			break
		}

		f = i
	}

	enc.cache.Set(cachekey, [2]any{f, err}, 0)

	return f, err
}

func hinterDecodeFunc[T any](ht hint.Hint, sample T, decode any) any {
	d, ok := decode.(decodeMapFunc)
	if !ok {
		return decode
	}

	isptr := reflect.TypeOf(sample).Kind() == reflect.Ptr
	var indirect bool

	if _, ok := any(sample).(hint.SetHinter); !ok {
		if isptr {
			return decode
		}

		if _, ok := any(new(T)).(hint.SetHinter); !ok {
			return decode
		}

		indirect = true
	}

	return func(enc *Encoder, m map[string]any) (any, error) {
		var t T

		switch i, err := d(enc, m); {
		case err != nil, i == nil:
			return i, err
		default:
			k, ok := i.(T)
			if !ok {
				return nil, errors.Errorf("decoded; expected %T, but %T", sample, i)
			}

			t = k
		}

		switch {
		case indirect:
			n := new(T)
			*n = t

			_ = any(n).(hint.SetHinter).SetHint(ht)

			return *n, nil
		case isptr:
			_ = any(t).(hint.SetHinter).SetHint(ht)

			return t, nil
		default:
			n := any(t).(hint.SetHinter).SetHint(ht)
			if _, ok := n.(T); !ok {
				return nil, errors.Errorf("SetHint; expected %T, but %T", sample, n)
			}

			return n, nil
		}
	}
}
