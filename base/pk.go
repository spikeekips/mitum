package base

import (
	"bytes"
	"fmt"

	"github.com/btcsuite/btcutil/base58"
	"github.com/spikeekips/mitum/util"
	"github.com/spikeekips/mitum/util/hint"
)

var SignatureVerificationError = util.NewError("signature verification failed")

type PKKey interface {
	fmt.Stringer
	hint.Hinter
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

type Signer interface {
	Sign(Privatekey, []byte /* additional info */) error
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
		return util.InvalidError.Errorf("empty signature")
	}

	return nil
}

func (sg Signature) Equal(b Signature) bool {
	return bytes.Equal(sg, b)
}

func (sg Signature) MarshalText() ([]byte, error) {
	return []byte(sg.String()), nil
}

func (sg *Signature) UnmarshalText(b []byte) error {
	*sg = Signature(base58.Decode(string(b)))

	return nil
}
