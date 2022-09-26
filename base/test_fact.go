//go:build test
// +build test

package base

import (
	"github.com/spikeekips/mitum/util"
	"github.com/spikeekips/mitum/util/hint"
	"github.com/spikeekips/mitum/util/valuehash"
	"github.com/stretchr/testify/assert"
)

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
	fact.h = fact.generateHash()

	return fact
}

func (fact DummyFact) Hint() hint.Hint {
	return DummyFactHint
}

func (fact DummyFact) IsValid([]byte) error {
	if err := util.CheckIsValiders(nil, false, fact.h, fact.token); err != nil {
		return util.ErrInvalid.Wrapf(err, "invalid DummyFact")
	}

	if !fact.h.Equal(fact.generateHash()) {
		return util.ErrInvalid.Errorf("DummyFact hash does not match")
	}

	return nil
}

func (fact DummyFact) Hash() util.Hash {
	return fact.h
}

func (fact DummyFact) Token() Token {
	return fact.token
}

func (fact DummyFact) generateHash() util.Hash {
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
	aht := a.(hint.Hinter).Hint()
	bht := b.(hint.Hinter).Hint()
	t.True(aht.Equal(bht), "Hint does not match; %q != %q", aht, bht)

	t.True(a.Hash().Equal(b.Hash()))
	t.Equal(a.Token(), b.Token())
}

func EqualSignFact(t *assert.Assertions, a, b SignFact) {
	t.Equal(a.HashBytes(), b.HashBytes())

	EqualFact(t, a.Fact(), b.Fact())
	EqualSigns(t, a.Signs(), b.Signs())
}
