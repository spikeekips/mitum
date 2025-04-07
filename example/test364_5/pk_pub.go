package dop5

import (
	"github.com/pkg/errors"
	"github.com/spikeekips/mitum/base"
	jsonenc "github.com/spikeekips/mitum/example/test364_5/encoder/json"
	dutil "github.com/spikeekips/mitum/example/test364_5/util"
	"github.com/spikeekips/mitum/util"
)

type LazyMPublickey string

func (k LazyMPublickey) String() string { return string(k) }
func (k LazyMPublickey) Bytes() []byte  { return []byte(k) }
func (k LazyMPublickey) IsValid([]byte) error {
	if k == "" {
		return util.ErrInvalid.Errorf("empty")
	}

	return nil
}

func (k LazyMPublickey) Equal(b base.PKKey) bool {
	return string(k) == b.String()
}

func (k LazyMPublickey) Verify(b []byte, sig base.Signature) error {
	s := k.String()

	switch i, err := base.LoadMPublickey(s[:len(s)-base.PKKeyTypeSize]); {
	case err != nil:
		return err
	default:
		return i.Verify(b, sig)
	}
}

func (k LazyMPublickey) MarshalText() ([]byte, error) {
	return []byte(k), nil
}

func (k *LazyMPublickey) UnmarshalText(b []byte) error {
	*k = LazyMPublickey(string(b))

	return nil
}

var _ = any(LazyMPublickey("")).(base.Publickey) //nolint:forcetypeassert

func NewLazyMPublickey(s string) LazyMPublickey {
	return LazyMPublickey(s)
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
			p, ok := i.(base.Publickey)
			if !ok {
				return nil, errors.Errorf("expected base.Publickey, but %T", i)
			}

			return p, nil
		}
	})
}
