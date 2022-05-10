package base

import (
	"bytes"
	"crypto/ecdsa"
	"fmt"
	"strings"

	"github.com/btcsuite/btcd/btcec"
	"github.com/btcsuite/btcd/chaincfg"
	"github.com/btcsuite/btcd/chaincfg/chainhash"
	"github.com/btcsuite/btcutil"
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
	wif *btcutil.WIF
	s   string
	pub MPublickey
	b   []byte
	hint.BaseHinter
}

func NewMPrivatekey() MPrivatekey {
	secret, _ := btcec.NewPrivateKey(btcec.S256()) //nolint:errcheck // ...

	wif, _ := btcutil.NewWIF(secret, &chaincfg.MainNetParams, true) //nolint:errcheck // ...

	return newMPrivatekeyFromWIF(wif)
}

func NewMPrivatekeyFromSeed(s string) (MPrivatekey, error) {
	if l := len(s); l < PrivatekeyMinSeedSize {
		return MPrivatekey{}, util.ErrInvalid.Errorf(
			"wrong seed for privatekey; too short, %d < %d", l, PrivatekeyMinSeedSize)
	}

	k, err := ecdsa.GenerateKey(
		btcec.S256(),
		bytes.NewReader([]byte(valuehash.NewSHA256([]byte(s)).String())),
	)
	if err != nil {
		return MPrivatekey{}, errors.Wrap(err, "failed NewPrivatekeyFromSeed")
	}

	wif, err := btcutil.NewWIF((*btcec.PrivateKey)(k), &chaincfg.MainNetParams, true)
	if err != nil {
		return MPrivatekey{}, errors.Wrap(err, "failed NewPrivatekeyFromSeed")
	}

	return newMPrivatekeyFromWIF(wif), nil
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
	wif, err := btcutil.DecodeWIF(s)
	if err != nil {
		return MPrivatekey{}, util.ErrInvalid.Wrapf(err, "failed to load privatekey")
	}

	return newMPrivatekeyFromWIF(wif), nil
}

func newMPrivatekeyFromWIF(wif *btcutil.WIF) MPrivatekey {
	k := MPrivatekey{
		BaseHinter: hint.NewBaseHinter(MPrivatekeyHint),
		wif:        wif,
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
	case k.wif == nil:
		return util.ErrInvalid.Errorf("empty btc wif of privatekey")
	case k.wif.PrivKey == nil:
		return util.ErrInvalid.Errorf("empty btc wif.PrivKey of privatekey")
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
	sig, err := k.wif.PrivKey.Sign(chainhash.DoubleHashB(b))
	if err != nil {
		return nil, errors.Wrap(err, "failed to sign")
	}

	return Signature(sig.Serialize()), nil
}

func (k MPrivatekey) MarshalText() ([]byte, error) {
	return []byte(k.s), nil
}

func (k *MPrivatekey) UnmarshalText(b []byte) error {
	u, err := LoadMPrivatekey(string(b))
	if err != nil {
		return errors.Wrap(err, "failed to UnmarshalText for privatekey")
	}

	*k = u.ensure()

	return nil
}

func (k *MPrivatekey) ensure() MPrivatekey {
	switch {
	case k.wif == nil:
		return *k
	case k.wif.PrivKey == nil:
		return *k
	}

	k.pub = NewMPublickey(k.wif.PrivKey.PubKey())
	k.s = fmt.Sprintf("%s%s", k.wif.String(), k.Hint().Type().String())
	k.b = []byte(k.s)

	return *k
}
