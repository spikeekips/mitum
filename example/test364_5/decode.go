package dop5

import (
	"github.com/pkg/errors"
	"github.com/spikeekips/mitum/base"
	jsonenc "github.com/spikeekips/mitum/example/test364_5/encoder/json"
	dutil "github.com/spikeekips/mitum/example/test364_5/util"
)

func DecodeJSONMPublickey(enc *jsonenc.Encoder, s string) (any, error) {
	return base.LoadMPublickey(s)
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
