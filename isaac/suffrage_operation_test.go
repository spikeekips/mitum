package isaac

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

type testSuffrageWithdrawFact struct {
	suite.Suite
}

func (t *testSuffrageWithdrawFact) TestNew() {
	fact := NewSuffrageWithdrawFact(util.UUID().Bytes(), base.RandomAddress(""), base.Height(33))
	t.NoError(fact.IsValid(nil))
}

func (t *testSuffrageWithdrawFact) TestIsValid() {
	t.Run("invalid BaseFact", func() {
		fact := NewSuffrageWithdrawFact(util.UUID().Bytes(), base.RandomAddress(""), base.Height(33))
		fact.SetToken(nil)

		err := fact.IsValid(nil)
		t.Error(err)
		t.True(errors.Is(err, util.ErrInvalid))
		t.ErrorContains(err, "invalid BaseFact")
	})

	t.Run("empty node", func() {
		fact := NewSuffrageWithdrawFact(util.UUID().Bytes(), nil, base.Height(33))
		err := fact.IsValid(nil)
		t.Error(err)
		t.True(errors.Is(err, util.ErrInvalid))
		t.ErrorContains(err, "invalid SuffrageWithdraw")
	})

	t.Run("bad node", func() {
		addr := base.NewStringAddress(strings.Repeat("a", base.MinAddressSize-base.AddressTypeSize-1))
		fact := NewSuffrageWithdrawFact(util.UUID().Bytes(), addr, base.Height(33))
		err := fact.IsValid(nil)
		t.Error(err)
		t.True(errors.Is(err, util.ErrInvalid))
		t.ErrorContains(err, "invalid SuffrageWithdraw")
	})

	t.Run("empty height", func() {
		fact := NewSuffrageWithdrawFact(util.UUID().Bytes(), base.RandomAddress(""), base.NilHeight)
		err := fact.IsValid(nil)
		t.Error(err)
		t.True(errors.Is(err, util.ErrInvalid))
		t.ErrorContains(err, "invalid SuffrageWithdraw")
	})

	t.Run("wrong hash", func() {
		fact := NewSuffrageWithdrawFact(util.UUID().Bytes(), base.RandomAddress(""), base.Height(33))
		fact.SetHash(valuehash.NewBytes(util.UUID().Bytes()))

		err := fact.IsValid(nil)
		t.Error(err)
		t.True(errors.Is(err, util.ErrInvalid))
		t.ErrorContains(err, "hash does not match")
	})
}

func TestSuffrageWithdrawFact(t *testing.T) {
	suite.Run(t, new(testSuffrageWithdrawFact))
}

func TestSuffrageWithdrawFactEncode(tt *testing.T) {
	t := new(encoder.BaseTestEncode)

	enc := jsonenc.NewEncoder()

	t.Encode = func() (interface{}, []byte) {
		fact := NewSuffrageWithdrawFact(util.UUID().Bytes(), base.RandomAddress(""), base.Height(33))

		b, err := enc.Marshal(fact)
		t.NoError(err)

		t.T().Log("marshaled:", string(b))

		return fact, b
	}
	t.Decode = func(b []byte) interface{} {
		t.NoError(enc.Add(encoder.DecodeDetail{Hint: base.StringAddressHint, Instance: base.StringAddress{}}))
		t.NoError(enc.Add(encoder.DecodeDetail{Hint: SuffrageWithdrawFactHint, Instance: SuffrageWithdrawFact{}}))

		i, err := enc.Decode(b)
		t.NoError(err)

		_, ok := i.(SuffrageWithdrawFact)
		t.True(ok)

		return i
	}
	t.Compare = func(a, b interface{}) {
		af, ok := a.(SuffrageWithdrawFact)
		t.True(ok)
		bf, ok := b.(SuffrageWithdrawFact)
		t.True(ok)

		t.NoError(bf.IsValid(nil))

		base.EqualFact(t.Assert(), af, bf)
	}

	suite.Run(tt, t)
}

type testSuffrageWithdraw struct {
	suite.Suite
}

func (t *testSuffrageWithdraw) TestIsValid() {
	priv := base.NewMPrivatekey()
	networkID := util.UUID().Bytes()

	t.Run("ok", func() {
		fact := NewSuffrageWithdrawFact(util.UUID().Bytes(), base.RandomAddress(""), base.Height(33))
		op := NewSuffrageWithdraw(fact)
		t.NoError(op.NodeSign(priv, networkID, base.RandomAddress("")))

		t.NoError(op.IsValid(networkID))

		signs := op.NodeSigns()
		t.Equal(1, len(signs))
	})

	t.Run("target node signed", func() {
		fact := NewSuffrageWithdrawFact(util.UUID().Bytes(), base.RandomAddress(""), base.Height(33))
		op := NewSuffrageWithdraw(fact)
		t.NoError(op.NodeSign(priv, networkID, fact.Node()))

		err := op.IsValid(networkID)
		t.Error(err)
		t.True(errors.Is(err, util.ErrInvalid))
		t.ErrorContains(err, "withdraw target node sign found")

		signs := op.NodeSigns()
		t.Equal(0, len(signs))
	})

	t.Run("different network id", func() {
		fact := NewSuffrageWithdrawFact(util.UUID().Bytes(), base.RandomAddress(""), base.Height(33))
		op := NewSuffrageWithdraw(fact)
		t.NoError(op.NodeSign(priv, networkID, base.RandomAddress("")))

		err := op.IsValid(util.UUID().Bytes())
		t.Error(err)
		t.True(errors.Is(err, base.ErrSignatureVerification))
	})
}

func TestSuffrageWithdraw(t *testing.T) {
	suite.Run(t, new(testSuffrageWithdraw))
}

func TestSuffrageWithdrawEncode(tt *testing.T) {
	t := new(encoder.BaseTestEncode)

	enc := jsonenc.NewEncoder()
	networkID := util.UUID().Bytes()

	t.Encode = func() (interface{}, []byte) {
		fact := NewSuffrageWithdrawFact(util.UUID().Bytes(), base.RandomAddress(""), base.Height(33))
		op := NewSuffrageWithdraw(fact)
		t.NoError(op.NodeSign(base.NewMPrivatekey(), networkID, base.RandomAddress("")))
		t.NoError(op.NodeSign(base.NewMPrivatekey(), networkID, base.RandomAddress("")))

		t.NoError(op.IsValid(networkID))

		b, err := enc.Marshal(op)
		t.NoError(err)

		t.T().Log("marshaled:", string(b))

		return op, b
	}
	t.Decode = func(b []byte) interface{} {
		t.NoError(enc.Add(encoder.DecodeDetail{Hint: base.StringAddressHint, Instance: base.StringAddress{}}))
		t.NoError(enc.Add(encoder.DecodeDetail{Hint: base.MPublickeyHint, Instance: base.MPublickey{}}))
		t.NoError(enc.Add(encoder.DecodeDetail{Hint: SuffrageWithdrawFactHint, Instance: SuffrageWithdrawFact{}}))
		t.NoError(enc.Add(encoder.DecodeDetail{Hint: SuffrageWithdrawHint, Instance: SuffrageWithdraw{}}))

		i, err := enc.Decode(b)
		t.NoError(err)

		op, ok := i.(SuffrageWithdraw)
		t.True(ok)

		t.NoError(op.IsValid(networkID))

		return i
	}
	t.Compare = func(a, b interface{}) {
		af, ok := a.(SuffrageWithdraw)
		t.True(ok)
		bf, ok := b.(SuffrageWithdraw)
		t.True(ok)

		t.NoError(bf.IsValid(networkID))

		base.EqualOperation(t.Assert(), af, bf)
	}

	suite.Run(tt, t)
}
