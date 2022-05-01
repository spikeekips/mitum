package base

import (
	"github.com/spikeekips/mitum/util"
	"github.com/spikeekips/mitum/util/hint"
	"github.com/spikeekips/mitum/util/valuehash"
)

type BaseFact struct {
	hint.BaseHinter
	h util.Hash
	t Token
}

func NewBaseFact(ht hint.Hint, t Token) BaseFact {
	return BaseFact{
		BaseHinter: hint.NewBaseHinter(ht),
		t:          t,
	}
}

func (fact BaseFact) Hash() util.Hash {
	return fact.h
}

func (fact *BaseFact) SetHash(h util.Hash) {
	fact.h = h
}

func (fact BaseFact) Token() Token {
	return fact.t
}

func (fact BaseFact) IsValid([]byte) error {
	e := util.StringErrorFunc("invalid BaseFact")

	if err := fact.BaseHinter.IsValid(fact.Hint().Type().Bytes()); err != nil {
		return e(err, "")
	}

	if err := IsValidFact(fact, nil); err != nil {
		return e(err, "")
	}

	return nil
}

func (fact BaseFact) JSONMarshaler() BaseFactJSONMarshaler {
	return BaseFactJSONMarshaler{
		BaseHinter: fact.BaseHinter,
		H:          fact.h,
		T:          fact.t,
	}
}

func (fact *BaseFact) SetJSONUnmarshaler(u BaseFactJSONUnmarshaler) {
	fact.h = u.H.Hash()
	fact.t = u.T
}

type BaseFactJSONMarshaler struct {
	hint.BaseHinter
	H util.Hash `json:"hash"`
	T Token     `json:"token"`
}

type BaseFactJSONUnmarshaler struct {
	H valuehash.HashDecoder `json:"hash"`
	T Token                 `json:"token"`
}
