package base

import (
	"slices"

	"github.com/pkg/errors"
	"github.com/spikeekips/mitum/util"
	"github.com/spikeekips/mitum/util/hint"
)

var (
	DummyPrivatekeyHint = hint.MustNewHint("dpk-v0.0.1")
	DummyPublickeyHint  = hint.MustNewHint("dpu-v0.0.1")
)

type dummyKey struct {
	s  string
	ht hint.Hint
}

func newDummyKey(s string) dummyKey {
	return dummyKey{s: s}
}

func (key dummyKey) Hint() hint.Hint {
	return key.ht
}

func (key dummyKey) Bytes() []byte {
	return []byte(key.s)
}

func (key dummyKey) IsValid([]byte) error {
	if len(key.s) < 1 {
		return util.ErrInvalid.Errorf("empty")
	}

	return nil
}

func (key dummyKey) Equal(b dummyKey) bool {
	return key.s == b.s
}

func (key *dummyKey) unmarshalText(b []byte) error {
	if len(b) < 1 {
		return errors.Errorf("empty")
	}

	key.s = string(b)

	return nil
}

type DummyPrivatekey struct {
	dummyKey
}

func NewDummyPrivatekey(s string) DummyPrivatekey {
	return DummyPrivatekey{dummyKey: dummyKey{s: s, ht: DummyPrivatekeyHint}}
}

func (key DummyPrivatekey) String() string {
	return key.s + key.ht.Type().String()
}

func (key DummyPrivatekey) Equal(b PKKey) bool {
	switch i, ok := b.(DummyPrivatekey); {
	case !ok, !key.dummyKey.Equal(i.dummyKey):
		return false
	default:
		return true
	}
}

func (key DummyPrivatekey) Publickey() Publickey {
	return NewDummyPublickey(key.s)
}

func (key DummyPrivatekey) Sign(b []byte) (Signature, error) {
	return Signature(util.ConcatBytesSlice([]byte(key.s), b)), nil
}

func (key DummyPrivatekey) MarshalText() ([]byte, error) {
	return []byte(key.String()), nil
}

func (key *DummyPrivatekey) UnmarshalText(b []byte) error {
	key.dummyKey = dummyKey{}

	if err := key.dummyKey.unmarshalText(b); err != nil {
		return err
	}

	key.dummyKey.ht = DummyPrivatekeyHint

	return nil
}

type DummyPublickey struct {
	dummyKey
}

func NewDummyPublickey(s string) DummyPublickey {
	return DummyPublickey{dummyKey: dummyKey{s: s, ht: DummyPublickeyHint}}
}

func (key DummyPublickey) String() string {
	return key.s + key.ht.Type().String()
}

func (key DummyPublickey) Equal(b PKKey) bool {
	switch i, ok := b.(DummyPublickey); {
	case !ok, !key.dummyKey.Equal(i.dummyKey):
		return false
	default:
		return true
	}
}

func (key DummyPublickey) Verify(b []byte, s Signature) error {
	if !slices.Equal(util.ConcatBytesSlice([]byte(key.s), b), s.Bytes()) {
		return ErrSignatureVerification
	}

	return nil
}

func (key DummyPublickey) MarshalText() ([]byte, error) {
	return []byte(key.String()), nil
}

func (key *DummyPublickey) UnmarshalText(b []byte) error {
	key.dummyKey = dummyKey{}

	if err := key.dummyKey.unmarshalText(b); err != nil {
		return err
	}

	key.dummyKey.ht = DummyPublickeyHint

	return nil
}
