package base

import (
	"fmt"
	"strings"

	btcec "github.com/btcsuite/btcd/btcec/v2"
	"github.com/btcsuite/btcd/btcec/v2/ecdsa"
	"github.com/btcsuite/btcd/btcutil/base58"
	"github.com/btcsuite/btcd/chaincfg/chainhash"
	"github.com/pkg/errors"
	"github.com/spikeekips/mitum/util"
	"github.com/spikeekips/mitum/util/hint"
)

// MPublickey is the default publickey of mitum, it is based on BTC Privatekey.
type MPublickey struct {
	k *btcec.PublicKey
	s string
	b []byte
	hint.BaseHinter
}

func NewMPublickey(k *btcec.PublicKey) MPublickey {
	pub := MPublickey{
		BaseHinter: hint.NewBaseHinter(MPublickeyHint),
		k:          k,
	}

	return pub.ensure()
}

func ParseMPublickey(s string) (MPublickey, error) {
	t := MPublickeyHint.Type().String()

	switch {
	case !strings.HasSuffix(s, t):
		return MPublickey{}, util.ErrInvalid.Errorf("unknown publickey string")
	case len(s) <= len(t):
		return MPublickey{}, util.ErrInvalid.Errorf("invalid publickey string; too short")
	}

	return LoadMPublickey(s[:len(s)-len(t)])
}

func LoadMPublickey(s string) (MPublickey, error) {
	k, err := btcec.ParsePubKey(base58.Decode(s))
	if err != nil {
		return MPublickey{}, util.ErrInvalid.WithMessage(err, "load publickey")
	}

	return NewMPublickey(k), nil
}

func (k MPublickey) String() string {
	return k.s
}

func (k MPublickey) Bytes() []byte {
	return k.b
}

func (k MPublickey) IsValid([]byte) error {
	if err := k.BaseHinter.IsValid(MPublickeyHint.Type().Bytes()); err != nil {
		return util.ErrInvalid.WithMessage(err, "wrong hint in publickey")
	}

	switch {
	case k.k == nil:
		return util.ErrInvalid.Errorf("empty btc publickey in publickey")
	case len(k.s) < 1:
		return util.ErrInvalid.Errorf("empty publickey string")
	case len(k.b) < 1:
		return util.ErrInvalid.Errorf("empty publickey []byte")
	}

	return nil
}

func (k MPublickey) Equal(b PKKey) bool {
	switch {
	case b == nil:
		return false
	default:
		return k.s == b.String()
	}
}

func (k MPublickey) Verify(input []byte, sig Signature) error {
	bsig, err := ecdsa.ParseSignature(sig)
	if err != nil {
		return errors.Wrap(err, "verify signature by publickey")
	}

	if !bsig.Verify(chainhash.DoubleHashB(input), k.k) {
		return ErrSignatureVerification.WithStack()
	}

	return nil
}

func (k MPublickey) MarshalText() ([]byte, error) {
	return []byte(k.s), nil
}

func (k *MPublickey) UnmarshalText(b []byte) error {
	u, err := LoadMPublickey(string(b))
	if err != nil {
		return errors.Wrap(err, "unmarshal publickey")
	}

	*k = u.ensure()

	return nil
}

func (k *MPublickey) ensure() MPublickey {
	if k.k == nil {
		return *k
	}

	k.s = fmt.Sprintf("%s%s", base58.Encode(k.k.SerializeCompressed()), k.Hint().Type().String())
	k.b = []byte(k.s)

	return *k
}
