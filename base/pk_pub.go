package base

import (
	"encoding/hex"
	"strings"

	btcec "github.com/btcsuite/btcd/btcec/v2"
	"github.com/btcsuite/btcd/btcec/v2/ecdsa"
	"github.com/btcsuite/btcd/chaincfg/chainhash"
	"github.com/pkg/errors"
	"github.com/spikeekips/mitum/util"
	"github.com/spikeekips/mitum/util/hint"
)

// MPublickey is the default publickey of mitum, it is based on BTC Privatekey.
type MPublickey struct {
	k *btcec.PublicKey
	hint.BaseHinter
}

func NewMPublickey(k *btcec.PublicKey) *MPublickey {
	return &MPublickey{
		BaseHinter: hint.NewBaseHinter(MPublickeyHint),
		k:          k,
	}
}

func ParseMPublickey(s string) (*MPublickey, error) {
	t := MPublickeyHint.Type().String()

	switch {
	case !strings.HasSuffix(s, t):
		return nil, util.ErrInvalid.Errorf("unknown publickey string")
	case len(s) <= len(t):
		return nil, util.ErrInvalid.Errorf("invalid publickey string; too short")
	}

	return LoadMPublickey(s[:len(s)-len(t)])
}

func LoadMPublickey(s string) (*MPublickey, error) {
	switch i, err := hex.DecodeString(s); {
	case err != nil:
		return nil, err
	default:
		k, err := btcec.ParsePubKey(i)
		if err != nil {
			return nil, util.ErrInvalid.WithMessage(err, "load publickey")
		}

		return NewMPublickey(k), nil
	}
}

func (k *MPublickey) String() string {
	var s strings.Builder

	_, _ = s.WriteString(hex.EncodeToString(k.k.SerializeCompressed()))
	_, _ = s.WriteString(k.Hint().Type().String())

	return s.String()
}

func (k *MPublickey) Bytes() []byte {
	return []byte(k.String())
}

func (k *MPublickey) IsValid([]byte) error {
	if err := k.BaseHinter.IsValid(MPublickeyHint.Type().Bytes()); err != nil {
		return util.ErrInvalid.WithMessage(err, "wrong hint in publickey")
	}

	if k.k == nil {
		return util.ErrInvalid.Errorf("empty btc publickey in publickey")
	}

	return nil
}

func (k *MPublickey) Equal(b PKKey) bool {
	return IsEqualPKKey(k, b)
}

func (k *MPublickey) Verify(input []byte, sig Signature) error {
	bsig, err := ecdsa.ParseSignature(sig)
	if err != nil {
		return errors.Wrap(err, "verify signature by publickey")
	}

	if !bsig.Verify(chainhash.DoubleHashB(input), k.k) {
		return ErrSignatureVerification.WithStack()
	}

	return nil
}

func (k *MPublickey) MarshalText() ([]byte, error) {
	return k.Bytes(), nil
}

func (k *MPublickey) UnmarshalText(b []byte) error {
	u, err := LoadMPublickey(string(b))
	if err != nil {
		return errors.Wrap(err, "unmarshal publickey")
	}

	*k = *u

	return nil
}
