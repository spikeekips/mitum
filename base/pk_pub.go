package base

import (
	"fmt"
	"strings"

	"github.com/btcsuite/btcd/btcec"
	"github.com/btcsuite/btcd/chaincfg/chainhash"
	"github.com/btcsuite/btcutil/base58"
	"github.com/pkg/errors"
	"github.com/spikeekips/mitum/util"
	"github.com/spikeekips/mitum/util/hint"
)

// MPublickey is the default publickey of mitum, it is based on BTC Privatekey.
type MPublickey struct {
	hint.BaseHinter
	k *btcec.PublicKey
	s string
	b []byte
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
		return MPublickey{}, util.InvalidError.Errorf("unknown publickey string")
	case len(s) <= len(t):
		return MPublickey{}, util.InvalidError.Errorf("invalid publickey string; too short")
	}

	return LoadMPublickey(s[:len(s)-len(t)])
}

func LoadMPublickey(s string) (MPublickey, error) {
	k, err := btcec.ParsePubKey(base58.Decode(s), btcec.S256())
	if err != nil {
		return MPublickey{}, util.InvalidError.Wrapf(err, "failed to load publickey")
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
		return util.InvalidError.Wrapf(err, "wrong hint in publickey")
	}

	switch {
	case k.k == nil:
		return util.InvalidError.Errorf("empty btc publickey in publickey")
	case len(k.s) < 1:
		return util.InvalidError.Errorf("empty publickey string")
	case len(k.b) < 1:
		return util.InvalidError.Errorf("empty publickey []byte")
	}

	return nil
}

func (k MPublickey) Equal(b PKKey) bool {
	switch {
	case b == nil:
		return false
	case k.Hint().Type() != b.Hint().Type():
		return false
	default:
		return k.s == b.String()
	}
}

func (k MPublickey) Verify(input []byte, sig Signature) error {
	bsig, err := btcec.ParseSignature(sig, btcec.S256())
	if err != nil {
		return errors.Wrap(err, "failed to verify signature by publickey")
	}

	if !bsig.Verify(chainhash.DoubleHashB(input), k.k) {
		return SignatureVerificationError.Call()
	}

	return nil
}

func (k MPublickey) MarshalText() ([]byte, error) {
	return []byte(k.s), nil
}

func (k *MPublickey) UnmarshalText(b []byte) error {
	u, err := LoadMPublickey(string(b))
	if err != nil {
		return errors.Wrap(err, "failed to UnmarshalText for publickey")
	}

	*k = u.ensure()

	return nil
}

func (k MPublickey) ensure() MPublickey {
	if k.k == nil {
		return k
	}

	if len(k.s) < 1 {
		k.s = fmt.Sprintf("%s%s", base58.Encode(k.k.SerializeCompressed()), MPublickeyHint.Type().String())
	}

	if len(k.b) < 1 {
		k.b = []byte(k.s)
	}

	return k
}
