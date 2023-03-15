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

type Facter interface {
	Fact() Fact
}

type SignFact interface {
	util.HashByter
	util.IsValider
	Fact() Fact
	Signs() []Sign
}

type NodeSignFact interface {
	SignFact
	NodeSigns() []NodeSign
}

type Token []byte

type Tokener interface {
	Token() Token
}

type TokenSetter interface {
	SetToken(Token) error
}

func (t Token) IsValid([]byte) error {
	e := util.ErrInvalid.Errorf("invalid Token")

	switch l := len(t); {
	case l < 1:
		return e.Errorf("empty")
	case l > MaxTokenSize:
		return e.Errorf("too long; %d > %d", l, MaxTokenSize)
	}

	return nil
}

func IsValidFact(fact Fact, b []byte) error {
	if err := util.CheckIsValiders(b, false,
		fact.Hash(),
		fact.Token(),
	); err != nil {
		return util.ErrInvalid.Wrapf(err, "invalid Fact")
	}

	return nil
}

func IsValidSignFact(sf SignFact, networkID []byte) error {
	e := util.ErrInvalid.Errorf("invalid SignFact")

	sfs := sf.Signs()
	if len(sfs) < 1 {
		return e.Errorf("empty signs")
	}

	bs := make([]util.IsValider, len(sf.Signs())+1)
	bs[0] = sf.Fact()

	for i := range sfs {
		bs[i+1] = sfs[i]
	}

	if err := util.CheckIsValiders(networkID, false, bs...); err != nil {
		return e.Wrapf(err, "invalid SignFact")
	}

	// NOTE caller should check the duplication of Signs

	for i := range sfs {
		if err := sfs[i].Verify(networkID, sf.Fact().Hash().Bytes()); err != nil {
			return e.Wrapf(err, "verify sign")
		}
	}

	return nil
}
