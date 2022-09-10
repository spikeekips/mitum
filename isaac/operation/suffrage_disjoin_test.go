package isaacoperation

import (
	"errors"
	"strings"
	"testing"

	"github.com/spikeekips/mitum/base"
	"github.com/spikeekips/mitum/util"
	"github.com/spikeekips/mitum/util/encoder"
	jsonenc "github.com/spikeekips/mitum/util/encoder/json"
	"github.com/spikeekips/mitum/util/valuehash"
	"github.com/stretchr/testify/suite"
)

type testSuffrageDisjoinFact struct {
	suite.Suite
}

func (t *testSuffrageDisjoinFact) TestNew() {
	fact := NewSuffrageDisjoinFact(util.UUID().Bytes(), base.RandomAddress(""), base.Height(33))
	t.NoError(fact.IsValid(nil))
}

func (t *testSuffrageDisjoinFact) TestIsValid() {
	t.Run("invalid BaseFact", func() {
		fact := NewSuffrageDisjoinFact(util.UUID().Bytes(), base.RandomAddress(""), base.Height(33))
		fact.SetToken(nil)

		err := fact.IsValid(nil)
		t.Error(err)
		t.True(errors.Is(err, util.ErrInvalid))
		t.ErrorContains(err, "invalid BaseFact")
	})

	t.Run("empty node", func() {
		fact := NewSuffrageDisjoinFact(util.UUID().Bytes(), nil, base.Height(33))
		err := fact.IsValid(nil)
		t.Error(err)
		t.True(errors.Is(err, util.ErrInvalid))
		t.ErrorContains(err, "invalid SuffrageDisjoin")
	})

	t.Run("bad node", func() {
		addr := base.NewStringAddress(strings.Repeat("a", base.MinAddressSize-base.AddressTypeSize-1))
		fact := NewSuffrageDisjoinFact(util.UUID().Bytes(), addr, base.Height(33))
		err := fact.IsValid(nil)
		t.Error(err)
		t.True(errors.Is(err, util.ErrInvalid))
		t.ErrorContains(err, "invalid SuffrageDisjoin")
	})

	t.Run("empty height", func() {
		fact := NewSuffrageDisjoinFact(util.UUID().Bytes(), base.RandomAddress(""), base.NilHeight)
		err := fact.IsValid(nil)
		t.Error(err)
		t.True(errors.Is(err, util.ErrInvalid))
		t.ErrorContains(err, "invalid SuffrageDisjoin")
	})

	t.Run("wrong hash", func() {
		fact := NewSuffrageDisjoinFact(util.UUID().Bytes(), base.RandomAddress(""), base.Height(33))
		fact.SetHash(valuehash.NewBytes(util.UUID().Bytes()))

		err := fact.IsValid(nil)
		t.Error(err)
		t.True(errors.Is(err, util.ErrInvalid))
		t.ErrorContains(err, "hash does not match")
	})
}

func TestSuffrageDisjoinFact(t *testing.T) {
	suite.Run(t, new(testSuffrageDisjoinFact))
}

func TestSuffrageDisjoinFactEncode(tt *testing.T) {
	t := new(encoder.BaseTestEncode)

	enc := jsonenc.NewEncoder()

	t.Encode = func() (interface{}, []byte) {
		fact := NewSuffrageDisjoinFact(util.UUID().Bytes(), base.RandomAddress(""), base.Height(33))

		b, err := enc.Marshal(fact)
		t.NoError(err)

		t.T().Log("marshaled:", string(b))

		return fact, b
	}
	t.Decode = func(b []byte) interface{} {
		t.NoError(enc.Add(encoder.DecodeDetail{Hint: base.StringAddressHint, Instance: base.StringAddress{}}))
		t.NoError(enc.Add(encoder.DecodeDetail{Hint: SuffrageDisjoinFactHint, Instance: SuffrageDisjoinFact{}}))

		i, err := enc.Decode(b)
		t.NoError(err)

		_, ok := i.(SuffrageDisjoinFact)
		t.True(ok)

		return i
	}
	t.Compare = func(a, b interface{}) {
		af, ok := a.(SuffrageDisjoinFact)
		t.True(ok)
		bf, ok := b.(SuffrageDisjoinFact)
		t.True(ok)

		t.NoError(bf.IsValid(nil))

		base.EqualFact(t.Assert(), af, bf)
	}

	suite.Run(tt, t)
}

type testSuffrageDisjoin struct {
	suite.Suite
}

func (t *testSuffrageDisjoin) TestIsValid() {
	priv := base.NewMPrivatekey()
	networkID := util.UUID().Bytes()

	t.Run("ok", func() {
		fact := NewSuffrageDisjoinFact(util.UUID().Bytes(), base.RandomAddress(""), base.Height(33))
		op := NewSuffrageDisjoin(fact)
		t.NoError(op.Sign(priv, networkID, fact.Node()))

		t.NoError(op.IsValid(networkID))
	})

	t.Run("different network id", func() {
		fact := NewSuffrageDisjoinFact(util.UUID().Bytes(), base.RandomAddress(""), base.Height(33))
		op := NewSuffrageDisjoin(fact)
		t.NoError(op.Sign(priv, networkID, fact.Node()))

		err := op.IsValid(util.UUID().Bytes())
		t.Error(err)
		t.True(errors.Is(err, base.ErrSignatureVerification))
	})

	t.Run("different node", func() {
		fact := NewSuffrageDisjoinFact(util.UUID().Bytes(), base.RandomAddress(""), base.Height(33))
		op := NewSuffrageDisjoin(fact)
		t.NoError(op.Sign(priv, networkID, base.RandomAddress("")))

		err := op.IsValid(networkID)
		t.Error(err)
		t.ErrorContains(err, "not signed by node")
	})

	t.Run("multiple signed", func() {
		fact := NewSuffrageDisjoinFact(util.UUID().Bytes(), base.RandomAddress(""), base.Height(33))
		op := NewSuffrageDisjoin(fact)
		t.NoError(op.Sign(priv, networkID, fact.Node()))

		t.NoError(op.Sign(base.NewMPrivatekey(), networkID, base.RandomAddress("")))

		err := op.IsValid(networkID)
		t.Error(err)
		t.True(errors.Is(err, util.ErrInvalid))
		t.ErrorContains(err, "multiple signed found")
	})
}

func TestSuffrageDisjoin(t *testing.T) {
	suite.Run(t, new(testSuffrageDisjoin))
}

func TestSuffrageDisjoinEncode(tt *testing.T) {
	t := new(encoder.BaseTestEncode)

	enc := jsonenc.NewEncoder()
	networkID := util.UUID().Bytes()

	t.Encode = func() (interface{}, []byte) {
		fact := NewSuffrageDisjoinFact(util.UUID().Bytes(), base.RandomAddress(""), base.Height(33))
		op := NewSuffrageDisjoin(fact)
		t.NoError(op.Sign(base.NewMPrivatekey(), networkID, fact.Node()))

		t.NoError(op.IsValid(networkID))

		b, err := enc.Marshal(op)
		t.NoError(err)

		t.T().Log("marshaled:", string(b))

		return op, b
	}
	t.Decode = func(b []byte) interface{} {
		t.NoError(enc.Add(encoder.DecodeDetail{Hint: base.StringAddressHint, Instance: base.StringAddress{}}))
		t.NoError(enc.Add(encoder.DecodeDetail{Hint: base.MPublickeyHint, Instance: base.MPublickey{}}))
		t.NoError(enc.Add(encoder.DecodeDetail{Hint: SuffrageDisjoinFactHint, Instance: SuffrageDisjoinFact{}}))
		t.NoError(enc.Add(encoder.DecodeDetail{Hint: SuffrageDisjoinHint, Instance: SuffrageDisjoin{}}))

		i, err := enc.Decode(b)
		t.NoError(err)

		op, ok := i.(SuffrageDisjoin)
		t.True(ok)

		t.NoError(op.IsValid(networkID))

		return i
	}
	t.Compare = func(a, b interface{}) {
		af, ok := a.(SuffrageDisjoin)
		t.True(ok)
		bf, ok := b.(SuffrageDisjoin)
		t.True(ok)

		t.NoError(bf.IsValid(networkID))

		base.EqualOperation(t.Assert(), af, bf)
	}

	suite.Run(tt, t)
}
