package base

import (
	"math"

	"github.com/spikeekips/mitum/util"
	"github.com/spikeekips/mitum/util/hint"
)

const MaxTokenSize = math.MaxUint16

type Fact interface {
	hint.Hinter
	util.IsValider
	util.Hasher
	Token() Token
}

type SignedFact interface {
	util.HashByter
	Fact() Fact
	Signed() []Signed
}

type Token []byte

func (t Token) IsValid([]byte) error {
	e := util.StringErrorFunc("invalid Token")
	switch l := len(t); {
	case l < 1:
		return e(util.InvalidError.Errorf("empty"), "")
	case l > MaxTokenSize:
		return e(util.InvalidError.Errorf("too long; %d > %d", l, MaxTokenSize), "")
	}

	return nil
}

// BLOCK add Token.Bytes()

func IsValidFact(fact Fact, b []byte) error {
	if err := util.CheckIsValid(b, false,
		fact.Hint(),
		fact.Hash(),
		fact.Token(),
	); err != nil {
		return util.InvalidError.Wrapf(err, "invalid Fact")
	}

	return nil
}

func IsValidSignedFact(sf SignedFact, networkID []byte) error {
	e := util.StringErrorFunc("invalid SignedFact")

	bs := make([]util.IsValider, len(sf.Signed())+1)
	bs[0] = sf.Fact()

	sfs := sf.Signed()
	if len(sfs) < 1 {
		return e(util.InvalidError.Errorf("empty SignedFact"), "")
	}

	for i := range sfs {
		bs[i+1] = sfs[i]
	}

	if err := util.CheckIsValid(networkID, false, bs...); err != nil {
		return e(util.InvalidError.Wrapf(err, "invalid SignedFact"), "")
	}

	for i := range sfs {
		if err := sfs[i].Verify(networkID, sf.Fact().Hash().Bytes()); err != nil {
			return e(util.InvalidError.Wrapf(err, "failed to verify signed"), "")
		}
	}

	return nil
}
