package base

import (
	"encoding/hex"
	"strings"

	btcec "github.com/btcsuite/btcd/btcec/v2"
	btcec_ecdsa "github.com/btcsuite/btcd/btcec/v2/ecdsa"
	"github.com/btcsuite/btcd/chaincfg/chainhash"
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
	hint.BaseHinter
}

func NewMPrivatekey() *MPrivatekey {
	priv, _ := btcec.NewPrivateKey()

	return newMPrivatekeyFromPrivateKey(priv)
}

func NewMPrivatekeyFromSeed(s string) (*MPrivatekey, error) {
	if l := len([]byte(s)); l < PrivatekeyMinSeedSize {
		return nil, util.ErrInvalid.Errorf(
			"wrong seed for privatekey; too short, %d < %d", l, PrivatekeyMinSeedSize)
	}

	priv, _ := btcec.PrivKeyFromBytes(valuehash.NewSHA256([]byte(s)).Bytes())

	return newMPrivatekeyFromPrivateKey(priv), nil
}

func ParseMPrivatekey(s string) (*MPrivatekey, error) {
	t := MPrivatekeyHint.Type().String()

	switch {
	case !strings.HasSuffix(s, t):
		return nil, util.ErrInvalid.Errorf("unknown privatekey string")
	case len(s) <= len(t):
		return nil, util.ErrInvalid.Errorf("invalid privatekey string; too short")
	}

	return LoadMPrivatekey(s[:len(s)-len(t)])
}

func LoadMPrivatekey(s string) (*MPrivatekey, error) {
	switch b, err := hex.DecodeString(s); {
	case err != nil:
		return nil, util.ErrInvalid.WithMessage(err, "private key")
	case len(b) < 1:
		return nil, util.ErrInvalid.Errorf("empty private key")
	default:
		priv, _ := btcec.PrivKeyFromBytes(b)

		return newMPrivatekeyFromPrivateKey(priv), nil
	}
}

func newMPrivatekeyFromPrivateKey(priv *btcec.PrivateKey) *MPrivatekey {
	return &MPrivatekey{
		BaseHinter: hint.NewBaseHinter(MPrivatekeyHint),
		priv:       priv,
	}
}

func (k *MPrivatekey) String() string {
	var s strings.Builder

	_, _ = s.WriteString(hex.EncodeToString(k.priv.Serialize()))
	_, _ = s.WriteString(k.Hint().Type().String())

	return s.String()
}

func (k *MPrivatekey) Bytes() []byte {
	return []byte(k.String())
}

func (k *MPrivatekey) IsValid([]byte) error {
	if err := k.BaseHinter.IsValid(MPrivatekeyHint.Type().Bytes()); err != nil {
		return util.ErrInvalid.WithMessage(err, "wrong hint in privatekey")
	}

	if k.priv == nil {
		return util.ErrInvalid.Errorf("empty btc privatekey")
	}

	return nil
}

func (k *MPrivatekey) Publickey() Publickey {
	return NewMPublickey(k.priv.PubKey())
}

func (k *MPrivatekey) Equal(b PKKey) bool {
	return IsEqualPKKey(k, b)
}

func (k *MPrivatekey) Sign(b []byte) (Signature, error) {
	sig := btcec_ecdsa.Sign(k.priv, chainhash.DoubleHashB(b))

	return Signature(sig.Serialize()), nil
}

func (k *MPrivatekey) MarshalText() ([]byte, error) {
	return k.Bytes(), nil
}

func (k *MPrivatekey) UnmarshalText(b []byte) error {
	u, err := LoadMPrivatekey(string(b))
	if err != nil {
		return err
	}

	*k = *u

	return nil
}
