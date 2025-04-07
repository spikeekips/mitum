package jsonenc

import (
	"context"
	"reflect"
	"strconv"

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

type Encoder struct {
	set            map[string]any
	hintCacheGet   func(string) (hint.Hint, bool, error)
	hintCacheSet   func(string, *hint.Hint, error)
	hintCacheClose func()
}

var defaultEncoderBufSize uint64 = 1 << 9

func NewEncoder(ctx context.Context, cacheSize int) *Encoder {
	enc := &Encoder{
		set: map[string]any{},
	}

	enc.hintCacheGet, enc.hintCacheSet, enc.hintCacheClose = newFuncHintCache(ctx, cacheSize, defaultEncoderBufSize)

	return enc
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

	return enc.addDecode(ht, hinterDecodeFunc(ht, sample, decode))
}

func (enc *Encoder) Close() {
	enc.hintCacheClose()
}

func (enc *Encoder) addDecode(ht hint.Hint, decode any) error {
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
	if i, found, err := enc.hintCacheGet(s); found {
		return i, err
	}

	ht, err := hint.ParseHint(s)

	var h *hint.Hint
	if err == nil {
		h = &ht
	}

	enc.hintCacheSet(s, h, err)

	return ht, err
}

func (enc *Encoder) getDecodeFunc(t hint.Type, v uint64) (any, error) {
	decodeKey := enc.decodeKey(t, v)

	switch f, found := enc.set[decodeKey]; {
	case !found:
		return nil, errors.Errorf("decode func not found, %q", t)
	default:
		return f, nil
	}
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

func newFuncHintCache(ctx context.Context, cacheSize int, bufSize uint64) (
	func(string) (hint.Hint, bool, error),
	func(string, *hint.Hint, error),
	func(),
) {
	get, _, lazySet, closef := dutil.NewLastFirstLazyCache[string, [2]any /* {*hint.Hint, error} */](
		ctx, cacheSize, bufSize, bufSize)

	return func(s string) (hint.Hint, bool, error) {
			if i, found := get(s); found {
				if i[1] != nil {
					return hint.Hint{}, false, i[1].(error) //nolint:forcetypeassert
				}

				return *(i[0].(*hint.Hint)), true, nil //nolint:forcetypeassert
			}

			return hint.Hint{}, false, nil
		},
		func(s string, ht *hint.Hint, err error) {
			lazySet(s, [2]any{ht, err}, 0)
		},
		closef
}
