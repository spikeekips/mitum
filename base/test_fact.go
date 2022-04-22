//go:build test
// +build test

package base

import (
	"github.com/spikeekips/mitum/util"
	"github.com/spikeekips/mitum/util/hint"
	"github.com/spikeekips/mitum/util/valuehash"
	"github.com/stretchr/testify/assert"
)

func (fact *BaseFact) SetToken(t Token) {
	fact.t = t
}

var DummyFactHint = hint.MustNewHint("dummyfact-v1.2.3")

type DummyFact struct {
	h     util.Hash
	token Token
	v     string
}

func NewDummyFact(token Token, v string) DummyFact {
	fact := DummyFact{
		token: token,
		v:     v,
	}
	fact.h = fact.hash()

	return fact
}

func (fact DummyFact) Hint() hint.Hint {
	return DummyFactHint
}

func (fact DummyFact) IsValid([]byte) error {
	if err := util.CheckIsValid(nil, false, fact.h, fact.token); err != nil {
		return util.InvalidError.Wrapf(err, "invalid DummyFact")
	}

	if !fact.h.Equal(fact.hash()) {
		return util.InvalidError.Errorf("DummyFact hash does not match")
	}

	return nil
}

func (fact DummyFact) Hash() util.Hash {
	return fact.h
}

func (fact DummyFact) Token() Token {
	return fact.token
}

func (fact DummyFact) hash() util.Hash {
	return valuehash.NewSHA256(util.ConcatBytesSlice([]byte(fact.v), fact.token))
}

func (fact DummyFact) MarshalJSON() ([]byte, error) {
	return util.MarshalJSON(struct {
		hint.HintedJSONHead
		H     util.Hash
		Token Token
		V     string
	}{
		HintedJSONHead: hint.NewHintedJSONHead(fact.Hint()),
		H:              fact.h,
		Token:          fact.token,
		V:              fact.v,
	})
}

func (fact *DummyFact) UnmarshalJSON(b []byte) error {
	var u struct {
		H     valuehash.HashDecoder
		Token Token
		V     string
	}

	if err := util.UnmarshalJSON(b, &u); err != nil {
		return err
	}

	fact.h = u.H.Hash()
	fact.token = u.Token
	fact.v = u.V

	return nil
}

func EqualFact(t *assert.Assertions, a, b Fact) {
	t.True(a.Hint().Equal(b.Hint()))
	t.True(a.Hash().Equal(b.Hash()))
	t.Equal(a.Token(), b.Token())
}

func EqualSignedFact(t *assert.Assertions, a, b SignedFact) {
	t.Equal(a.HashBytes(), b.HashBytes())

	EqualFact(t, a.Fact(), b.Fact())
	EqualSigneds(t, a.Signed(), b.Signed())
}
