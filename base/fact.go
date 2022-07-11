package base

import (
	"math"

	"github.com/spikeekips/mitum/util"
)

const MaxTokenSize = math.MaxUint16

type Fact interface {
	util.IsValider
	util.Hasher
	Token() Token
}

type SignedFact interface {
	util.HashByter
	Fact() Fact
	Signed() []Signed
}

type NodeSignedFact interface {
	SignedFact
	NodeSigned() []NodeSigned
}

type Token []byte

func (t Token) IsValid([]byte) error {
	e := util.StringErrorFunc("invalid Token")

	switch l := len(t); {
	case l < 1:
		return e(util.ErrInvalid.Errorf("empty"), "")
	case l > MaxTokenSize:
		return e(util.ErrInvalid.Errorf("too long; %d > %d", l, MaxTokenSize), "")
	}

	return nil
}

func IsValidFact(fact Fact, b []byte) error {
	if err := util.CheckIsValid(b, false,
		fact.Hash(),
		fact.Token(),
	); err != nil {
		return util.ErrInvalid.Wrapf(err, "invalid Fact")
	}

	return nil
}

func IsValidSignedFact(sf SignedFact, networkID []byte) error {
	e := util.ErrInvalid.Errorf("invalid SignedFact")

	sfs := sf.Signed()
	if len(sfs) < 1 {
		return e.Errorf("empty SignedFact")
	}

	bs := make([]util.IsValider, len(sf.Signed())+1)
	bs[0] = sf.Fact()

	for i := range sfs {
		bs[i+1] = sfs[i]
	}

	if err := util.CheckIsValid(networkID, false, bs...); err != nil {
		return e.Wrapf(err, "invalid SignedFact")
	}

	// NOTE caller should check the duplication of Signeds

	for i := range sfs {
		if err := sfs[i].Verify(networkID, sf.Fact().Hash().Bytes()); err != nil {
			return e.Wrapf(err, "failed to verify signed")
		}
	}

	return nil
}
