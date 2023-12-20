package base

import (
	"bytes"
	"encoding/hex"
	"fmt"

	"github.com/pkg/errors"
	"github.com/spikeekips/mitum/util"
	"github.com/spikeekips/mitum/util/encoder"
)

const PKKeyTypeSize = 3

var ErrSignatureVerification = util.NewIDError("signature verification")

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
	return hex.EncodeToString(sg)
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
	switch i, err := hex.DecodeString(string(b)); {
	case err != nil:
		return err
	default:
		*sg = Signature(i)

		return nil
	}
}

func decodePKKeyFromString(s string, enc encoder.Encoder) (PKKey, error) {
	i, err := enc.DecodeWithFixedHintType(s, PKKeyTypeSize)

	switch {
	case err != nil:
		return nil, err
	case i == nil:
		return nil, errors.Errorf("nil")
	}

	switch k, err := util.AssertInterfaceValue[PKKey](i); {
	case err != nil:
		return nil, err
	case k.String() != s:
		return nil, errors.Errorf("unknown key format")
	default:
		return k, nil
	}
}

func DecodePrivatekeyFromString(s string, enc encoder.Encoder) (k Privatekey, _ error) {
	e := util.StringError("decode privatekey")

	i, err := decodePKKeyFromString(s, enc)
	if err != nil {
		return nil, e.Wrap(err)
	}

	if err := util.SetInterfaceValue(i, &k); err != nil {
		return nil, e.Wrap(err)
	}

	return k, nil
}

func DecodePublickeyFromString(s string, enc encoder.Encoder) (Publickey, error) {
	e := util.StringError("decode publickey")

	if len(s) < 1 {
		return nil, e.Errorf("empty")
	}

	switch i, found := objcache.Get(s); {
	case !found:
	case i == nil:
	default:
		if err, ok := i.(error); ok {
			return nil, e.Wrap(err)
		}

		return util.AssertInterfaceValue[Publickey](i)
	}

	pub, err := decodePublickeyFromString(s, enc)
	if err != nil {
		objcache.Set(s, err, nil)

		return nil, e.Wrap(err)
	}

	objcache.Set(s, pub, nil)

	return pub, nil
}

func decodePublickeyFromString(s string, enc encoder.Encoder) (k Publickey, _ error) {
	i, err := decodePKKeyFromString(s, enc)
	if err != nil {
		return nil, err
	}

	if err := util.SetInterfaceValue(i, &k); err != nil {
		return nil, err
	}

	return k, nil
}
