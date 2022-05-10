package base

import (
	"github.com/spikeekips/mitum/util"
	"github.com/spikeekips/mitum/util/hint"
	"github.com/spikeekips/mitum/util/valuehash"
)

type BaseFact struct {
	h util.Hash
	t Token
	hint.BaseHinter
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
		Hash:       fact.h,
		Token:      fact.t,
	}
}

func (fact *BaseFact) SetJSONUnmarshaler(u BaseFactJSONUnmarshaler) {
	fact.h = u.Hash.Hash()
	fact.t = u.Token
}

type BaseFactJSONMarshaler struct {
	Hash  util.Hash `json:"hash"`
	Token Token     `json:"token"`
	hint.BaseHinter
}

type BaseFactJSONUnmarshaler struct {
	Hash  valuehash.HashDecoder `json:"hash"`
	Token Token                 `json:"token"`
}
