package base

import (
	"bytes"
	"fmt"

	"github.com/btcsuite/btcutil/base58"
	"github.com/spikeekips/mitum/util"
	"github.com/spikeekips/mitum/util/encoder"
)

const PKKeyTypeSize = 3

var ErrSignatureVerification = util.NewError("signature verification failed")

type Signer interface {
	Sign(Privatekey, NetworkID /* additional info */) error
}

type HashSigner interface {
	HashSign(Privatekey, NetworkID) error
}

type NodeSigner interface {
	Sign(Privatekey, NetworkID, Address) error
}

type NodeHashSigner interface {
	HashSign(Privatekey, NetworkID, Address) error
}

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
	e := util.StringErrorFunc("failed to parse pk key")

	i, err := enc.DecodeWithFixedHintType(s, PKKeyTypeSize)

	switch {
	case err != nil:
		return nil, e(err, "failed to decode pk key")
	case i == nil:
		return nil, nil
	}

	k, ok := i.(PKKey)
	if !ok {
		return nil, e(nil, "failed to decode pk key; not PKKey, %T", i)
	}

	return k, nil
}

func DecodePrivatekeyFromString(s string, enc encoder.Encoder) (Privatekey, error) {
	if len(s) < 1 {
		return nil, nil
	}

	e := util.StringErrorFunc("failed to parse privatekey")

	i, err := decodePKKeyFromString(s, enc)

	switch {
	case err != nil:
		return nil, e(err, "")
	case i == nil:
		return nil, nil
	}

	k, ok := i.(Privatekey)
	if !ok {
		return nil, e(nil, "failed to decode privatekey; not Privatekey, %T", i)
	}

	return k, nil
}

func DecodePublickeyFromString(s string, enc encoder.Encoder) (Publickey, error) {
	if len(s) < 1 {
		return nil, nil
	}

	switch i, found := objcache.Get(s); {
	case !found:
	case i == nil:
		return nil, nil
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
	e := util.StringErrorFunc("failed to parse publickey")

	i, err := decodePKKeyFromString(s, enc)

	switch {
	case err != nil:
		return nil, e(err, "")
	case i == nil:
		return nil, nil
	default:
		k, ok := i.(Publickey)
		if !ok {
			return nil, e(nil, "failed to decode publickey; not Publickey, %T", i)
		}

		return k, nil
	}
}
