package dop5

import (
	"unique"

	"github.com/pkg/errors"
	"github.com/spikeekips/mitum/base"
	jsonenc "github.com/spikeekips/mitum/example/test364_5/encoder/json"
	dutil "github.com/spikeekips/mitum/example/test364_5/util"
	"github.com/spikeekips/mitum/util"
)

type LazyMPublickey unique.Handle[string]

func (k LazyMPublickey) value() string  { return unique.Handle[string](k).Value() }
func (k LazyMPublickey) String() string { return k.value() }
func (k LazyMPublickey) Bytes() []byte  { return []byte(k.value()) }
func (k LazyMPublickey) IsValid([]byte) error {
	if k.value() == "" {
		return util.ErrInvalid.Errorf("empty")
	}

	return nil
}

func (k LazyMPublickey) Equal(b base.PKKey) bool {
	return k.value() == b.String()
}

func (k LazyMPublickey) Verify(b []byte, sig base.Signature) error {
	s := k.value()

	switch i, err := base.LoadMPublickey(s[:len(s)-base.PKKeyTypeSize]); {
	case err != nil:
		return err
	default:
		return i.Verify(b, sig)
	}
}

func (k LazyMPublickey) MarshalText() ([]byte, error) {
	return []byte(k.value()), nil
}

func (k *LazyMPublickey) UnmarshalText(b []byte) error {
	*k = LazyMPublickey(unique.Make(string(b)))

	return nil
}

var _ = any(LazyMPublickey{}).(base.Publickey)

func NewLazyMPublickey(s string) LazyMPublickey {
	return LazyMPublickey(unique.Make(s))
}

func DecodeJSONMPublickey(_ *jsonenc.Encoder, s string) (any, error) {
	return NewLazyMPublickey(s), nil
}

func AssignJSONPublickey(v any, k *base.Publickey, enc *jsonenc.Encoder) dutil.ErrorFunc {
	return dutil.AssignJSONFunc(v, k, func(v any) (base.Publickey, error) {
		s, ok := v.(string)
		if !ok {
			return nil, errors.Errorf("expected string, but %T", v)
		}

		switch i, err := enc.DecodeString(s, base.PKKeyTypeSize); {
		case err != nil:
			return nil, err
		default:
			return i.(base.Publickey), nil
		}
	})
}
