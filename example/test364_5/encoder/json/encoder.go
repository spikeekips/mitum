package jsonenc

import (
	"context"
	"reflect"

	"github.com/pkg/errors"
	"github.com/spikeekips/mitum/example/test364_5/encoder"
	dutil "github.com/spikeekips/mitum/example/test364_5/util"
	"github.com/spikeekips/mitum/util"
	"github.com/spikeekips/mitum/util/hint"
)

type (
	decodeMapFunc    = func(*Encoder, map[string]any) (any, error)
	decodeStringFunc = func(*Encoder, string) (any, error)
)

var defaultEncoderBufSize uint64 = 1 << 9

type Encoder struct {
	hintsetSet   func(hint.Type, uint64, any) bool
	hintsetGet   func(hint.Type, uint64) (any, bool)
	hintsetClose func()
	cacheGet     func(string) (hint.Hint, error)
	cacheClose   func()
}

func NewEncoder(ctx context.Context, cacheSize int) *Encoder {
	enc := new(Encoder)

	enc.hintsetSet, enc.hintsetGet, enc.hintsetClose = dutil.NewHintset[any]()
	enc.cacheGet, enc.cacheClose = dutil.NewHintCache(
		ctx, cacheSize, defaultEncoderBufSize)

	return enc
}

func AddDecode[T any](enc *Encoder, ht hint.Hint, sample T, decode any) error {
	if decode == nil {
		return errors.New("nil decode")
	}

	if err := ht.IsValid(nil); err != nil {
		return err
	}

	if reflect.TypeOf(sample) == nil {
		return errors.New("nil sample")
	}

	return enc.addDecode(ht, hinterDecodeFunc(ht, sample, decode))
}

func (enc *Encoder) Close() {
	enc.hintsetClose()
	enc.cacheClose()
}

func (enc *Encoder) addDecode(ht hint.Hint, decode any) error {
	t, v := enc.decodeKey(ht, decode)

	if !enc.hintsetSet(t, v, decode) {
		return errors.Errorf("already added, %q", ht)
	}

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

func (enc *Encoder) UnmarshalString(b []byte, size int) (any, error) {
	var s string
	if err := util.UnmarshalJSON(b, &s); err != nil {
		return nil, err
	}

	return enc.DecodeString(s, size)
}

func (enc *Encoder) Decode(v any) (any, error) {
	if v == nil {
		return nil, nil //nolint:nilnil
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

	switch _, t, err := hint.ParseFixedTypedString(s, size); {
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

		return decode(enc, s)
	}
}

func (enc *Encoder) decodeMap(m map[string]any) (any, error) {
	var decode decodeMapFunc

	var ht hint.Hint
	switch i, found := m["_hint"]; {
	case !found:
		return nil, errors.New("_hint not found")
	default:
		s, ok := i.(string)
		if !ok {
			return nil, errors.Errorf("expected hint string, but %T", i)
		}

		switch h, err := enc.cacheGet(s); {
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

func (enc *Encoder) getDecodeFunc(t hint.Type, v uint64) (any, error) {
	switch f, found := enc.hintsetGet(t, v); {
	case !found:
		return nil, errors.Errorf("decode func not found, %q", t)
	default:
		return f, nil
	}
}

func (enc *Encoder) decodeKey(ht hint.Hint, decode any) (hint.Type, uint64) {
	major := ht.Version().Major()
	if _, ok := decode.(decodeStringFunc); ok {
		major = 0
	}

	return ht.Type(), major
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
		sethint = func(t T) (any, error) { //nolint:unparam
			n := new(T)
			*n = t

			_ = any(n).(hint.SetHinter).SetHint(ht) //nolint:forcetypeassert

			return *n, nil
		}
	default:
		sethint = func(t T) (any, error) {
			n := any(t).(hint.SetHinter).SetHint(ht) //nolint:forcetypeassert
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

			if h, ok := i.(hint.Hinter); ok && !h.Hint().IsEmpty() {
				return k, nil
			}

			return sethint(k)
		}
	}
}
