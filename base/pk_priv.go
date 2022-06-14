package base

import (
	"bytes"
	"crypto/ecdsa"
	"fmt"
	"strings"

	btcec "github.com/btcsuite/btcd/btcec/v2"
	btcec_ecdsa "github.com/btcsuite/btcd/btcec/v2/ecdsa"
	"github.com/btcsuite/btcd/chaincfg/chainhash"
	"github.com/btcsuite/btcutil/base58"
	"github.com/pkg/errors"
	"github.com/spikeekips/mitum/util"
	"github.com/spikeekips/mitum/util/hint"
	"github.com/spikeekips/mitum/util/valuehash"
)

var (
	MPrivatekeyHint = hint.MustNewHint("mpr-v0.0.1")
	MPublickeyHint  = hint.MustNewHint("mpu-v0.0.1")
)

const PrivatekeyMinSeedSize = 36

// MPrivatekey is the default privatekey of mitum, it is based on BTC Privatekey.
type MPrivatekey struct {
	priv *btcec.PrivateKey
	s    string
	pub  MPublickey
	b    []byte
	hint.BaseHinter
}

func NewMPrivatekey() MPrivatekey {
	priv, _ := btcec.NewPrivateKey()

	return newMPrivatekeyFromPrivateKey(priv)
}

func NewMPrivatekeyFromSeed(s string) (MPrivatekey, error) {
	if l := len([]byte(s)); l < PrivatekeyMinSeedSize {
		return MPrivatekey{}, util.ErrInvalid.Errorf(
			"wrong seed for privatekey; too short, %d < %d", l, PrivatekeyMinSeedSize)
	}

	k, err := ecdsa.GenerateKey(
		btcec.S256(),
		bytes.NewReader([]byte(valuehash.NewSHA256([]byte(s)).String())),
	)
	if err != nil {
		return MPrivatekey{}, errors.Wrap(err, "")
	}

	priv, _ := btcec.PrivKeyFromBytes(k.D.Bytes())

	return newMPrivatekeyFromPrivateKey(priv), nil
}

func ParseMPrivatekey(s string) (MPrivatekey, error) {
	t := MPrivatekeyHint.Type().String()

	switch {
	case !strings.HasSuffix(s, t):
		return MPrivatekey{}, util.ErrInvalid.Errorf("unknown privatekey string")
	case len(s) <= len(t):
		return MPrivatekey{}, util.ErrInvalid.Errorf("invalid privatekey string; too short")
	}

	return LoadMPrivatekey(s[:len(s)-len(t)])
}

func LoadMPrivatekey(s string) (MPrivatekey, error) {
	b := base58.Decode(s)

	if len(b) < 1 {
		return MPrivatekey{}, util.ErrInvalid.Errorf("malformed private key")
	}

	priv, _ := btcec.PrivKeyFromBytes(b)

	return newMPrivatekeyFromPrivateKey(priv), nil
}

func newMPrivatekeyFromPrivateKey(priv *btcec.PrivateKey) MPrivatekey {
	k := MPrivatekey{
		BaseHinter: hint.NewBaseHinter(MPrivatekeyHint),
		priv:       priv,
	}

	return k.ensure()
}

func (k MPrivatekey) String() string {
	return k.s
}

func (k MPrivatekey) Bytes() []byte {
	return k.b
}

func (k MPrivatekey) IsValid([]byte) error {
	if err := k.BaseHinter.IsValid(MPrivatekeyHint.Type().Bytes()); err != nil {
		return util.ErrInvalid.Wrapf(err, "wrong hint in privatekey")
	}

	switch {
	case k.priv == nil:
		return util.ErrInvalid.Errorf("empty btc privatekey")
	case len(k.s) < 1:
		return util.ErrInvalid.Errorf("empty privatekey string")
	case len(k.b) < 1:
		return util.ErrInvalid.Errorf("empty privatekey []byte")
	}

	return nil
}

func (k MPrivatekey) Publickey() Publickey {
	return k.pub
}

func (k MPrivatekey) Equal(b PKKey) bool {
	switch {
	case b == nil:
		return false
	default:
		return k.s == b.String()
	}
}

func (k MPrivatekey) Sign(b []byte) (Signature, error) {
	sig := btcec_ecdsa.Sign(k.priv, chainhash.DoubleHashB(b))

	return Signature(sig.Serialize()), nil
}

func (k MPrivatekey) MarshalText() ([]byte, error) {
	return []byte(k.s), nil
}

func (k *MPrivatekey) UnmarshalText(b []byte) error {
	u, err := LoadMPrivatekey(string(b))
	if err != nil {
		return errors.Wrap(err, "")
	}

	*k = u.ensure()

	return nil
}

func (k *MPrivatekey) ensure() MPrivatekey {
	if k.priv == nil {
		return *k
	}

	k.pub = NewMPublickey(k.priv.PubKey())
	k.s = fmt.Sprintf("%s%s", base58.Encode(k.priv.Serialize()), k.Hint().Type().String())
	k.b = []byte(k.s)

	return *k
}
