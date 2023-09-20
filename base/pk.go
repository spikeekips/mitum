package base

import (
	"bytes"
	"fmt"

	"github.com/btcsuite/btcutil/base58"
	"github.com/pkg/errors"
	"github.com/spikeekips/mitum/util"
	"github.com/spikeekips/mitum/util/encoder"
)

const PKKeyTypeSize = 3

var ErrSignatureVerification = util.NewIDError("signature verification failed")

type PKKey interface {
	fmt.Stringer
	util.Byter
	util.IsValider
	Equal(PKKey) bool
}

type Privatekey interface {
	PKKey
	Publickey() Publickey
	Sign([]byte) (Signature, error)
}

type Publickey interface {
	PKKey
	Verify([]byte, Signature) error
}

type Signature []byte

func (sg Signature) Bytes() []byte {
	return sg
}

func (sg Signature) String() string {
	return base58.Encode(sg)
}

func (sg Signature) IsValid([]byte) error {
	if len(sg) < 1 {
		return util.ErrInvalid.Errorf("empty signature")
	}

	return nil
}

func (sg Signature) Equal(b Signature) bool {
	if b == nil {
		return false
	}

	return bytes.Equal(sg, b)
}

func (sg Signature) MarshalText() ([]byte, error) {
	return []byte(sg.String()), nil
}

func (sg *Signature) UnmarshalText(b []byte) error {
	*sg = Signature(base58.Decode(string(b)))

	return nil
}

func decodePKKeyFromString(s string, enc encoder.Encoder) (PKKey, error) {
	e := util.StringError("decode pk key")

	i, err := enc.DecodeWithFixedHintType(s, PKKeyTypeSize)

	switch {
	case err != nil:
		return nil, e.Wrap(err)
	case i == nil:
		return nil, e.Errorf("nil")
	}

	switch k, ok := i.(PKKey); {
	case !ok:
		return nil, e.Errorf("not PKKey, %T", i)
	case k.String() != s:
		return nil, e.Errorf("unknown key format")
	default:
		return k, nil
	}
}

func DecodePrivatekeyFromString(s string, enc encoder.Encoder) (Privatekey, error) {
	e := util.StringError("decode privatekey")

	i, err := decodePKKeyFromString(s, enc)
	if err != nil {
		return nil, e.Wrap(err)
	}

	k, ok := i.(Privatekey)
	if !ok {
		return nil, e.Errorf("not Privatekey, %T", i)
	}

	return k, nil
}

func DecodePublickeyFromString(s string, enc encoder.Encoder) (Publickey, error) {
	if len(s) < 1 {
		return nil, errors.Errorf("decode publickey; empty")
	}

	switch i, found := objcache.Get(s); {
	case !found:
	case i == nil:
	default:
		if err, ok := i.(error); ok {
			return nil, err
		}

		return i.(Publickey), nil //nolint:forcetypeassert //...
	}

	pub, err := decodePublickeyFromString(s, enc)
	if err != nil {
		objcache.Set(s, err, nil)

		return nil, err
	}

	objcache.Set(s, pub, nil)

	return pub, nil
}

func decodePublickeyFromString(s string, enc encoder.Encoder) (Publickey, error) {
	e := util.StringError("decode publickey")

	i, err := decodePKKeyFromString(s, enc)
	if err != nil {
		return nil, e.Wrap(err)
	}

	k, ok := i.(Publickey)
	if !ok {
		return nil, e.Errorf("not Publickey, %T", i)
	}

	return k, nil
}
