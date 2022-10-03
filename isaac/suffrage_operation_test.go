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
	fact := NewSuffrageWithdrawFact(base.RandomAddress(""), base.Height(33))
	t.NoError(fact.IsValid(nil))

	_ = (interface{})(fact).(base.SuffrageWithdrawFact)
}

func (t *testSuffrageWithdrawFact) TestIsValid() {
	t.Run("invalid BaseFact", func() {
		fact := NewSuffrageWithdrawFact(base.RandomAddress(""), base.Height(33))
		fact.SetHash(nil)

		err := fact.IsValid(nil)
		t.Error(err)
		t.True(errors.Is(err, util.ErrInvalid))
		t.ErrorContains(err, "invalid BaseFact")
	})

	t.Run("invalid token", func() {
		fact := NewSuffrageWithdrawFact(base.RandomAddress(""), base.Height(33))
		t.NoError(fact.SetToken(util.UUID().Bytes()))

		err := fact.IsValid(nil)
		t.Error(err)
		t.True(errors.Is(err, util.ErrInvalid))
		t.ErrorContains(err, "invalid token")
	})

	t.Run("empty node", func() {
		fact := NewSuffrageWithdrawFact(nil, base.Height(33))
		err := fact.IsValid(nil)
		t.Error(err)
		t.True(errors.Is(err, util.ErrInvalid))
		t.ErrorContains(err, "invalid SuffrageWithdraw")
	})

	t.Run("bad node", func() {
		addr := base.NewStringAddress(strings.Repeat("a", base.MinAddressSize-base.AddressTypeSize-1))
		fact := NewSuffrageWithdrawFact(addr, base.Height(33))
		err := fact.IsValid(nil)
		t.Error(err)
		t.True(errors.Is(err, util.ErrInvalid))
		t.ErrorContains(err, "invalid SuffrageWithdraw")
	})

	t.Run("empty height", func() {
		fact := NewSuffrageWithdrawFact(base.RandomAddress(""), base.NilHeight)
		err := fact.IsValid(nil)
		t.Error(err)
		t.True(errors.Is(err, util.ErrInvalid))
		t.ErrorContains(err, "invalid SuffrageWithdraw")
	})

	t.Run("wrong hash", func() {
		fact := NewSuffrageWithdrawFact(base.RandomAddress(""), base.Height(33))
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
		fact := NewSuffrageWithdrawFact(base.RandomAddress(""), base.Height(33))

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

type testSuffrageWithdrawOperation struct {
	suite.Suite
}

func (t *testSuffrageWithdrawOperation) TestNew() {
	priv := base.NewMPrivatekey()
	networkID := util.UUID().Bytes()

	fact := NewSuffrageWithdrawFact(base.RandomAddress(""), base.Height(33))
	op := NewSuffrageWithdrawOperation(fact)
	t.NoError(op.NodeSign(priv, networkID, base.RandomAddress("")))

	_ = (interface{})(op).(base.SuffrageWithdrawOperation)
}

func (t *testSuffrageWithdrawOperation) TestIsValid() {
	priv := base.NewMPrivatekey()
	networkID := util.UUID().Bytes()

	t.Run("ok", func() {
		fact := NewSuffrageWithdrawFact(base.RandomAddress(""), base.Height(33))
		op := NewSuffrageWithdrawOperation(fact)
		t.NoError(op.NodeSign(priv, networkID, base.RandomAddress("")))

		t.NoError(op.IsValid(networkID))

		signs := op.NodeSigns()
		t.Equal(1, len(signs))
	})

	t.Run("target node signed", func() {
		fact := NewSuffrageWithdrawFact(base.RandomAddress(""), base.Height(33))
		op := NewSuffrageWithdrawOperation(fact)
		t.NoError(op.NodeSign(priv, networkID, fact.Node()))

		err := op.IsValid(networkID)
		t.Error(err)
		t.True(errors.Is(err, util.ErrInvalid))
		t.ErrorContains(err, "withdraw target node sign found")

		signs := op.NodeSigns()
		t.Equal(0, len(signs))
	})

	t.Run("different network id", func() {
		fact := NewSuffrageWithdrawFact(base.RandomAddress(""), base.Height(33))
		op := NewSuffrageWithdrawOperation(fact)
		t.NoError(op.NodeSign(priv, networkID, base.RandomAddress("")))

		err := op.IsValid(util.UUID().Bytes())
		t.Error(err)
		t.True(errors.Is(err, base.ErrSignatureVerification))
	})
}

func TestSuffrageWithdrawOperation(t *testing.T) {
	suite.Run(t, new(testSuffrageWithdrawOperation))
}

func TestSuffrageWithdrawOperationEncode(tt *testing.T) {
	t := new(encoder.BaseTestEncode)

	enc := jsonenc.NewEncoder()
	networkID := util.UUID().Bytes()

	t.Encode = func() (interface{}, []byte) {
		fact := NewSuffrageWithdrawFact(base.RandomAddress(""), base.Height(33))
		op := NewSuffrageWithdrawOperation(fact)
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
		t.NoError(enc.Add(encoder.DecodeDetail{Hint: SuffrageWithdrawHint, Instance: SuffrageWithdrawOperation{}}))

		i, err := enc.Decode(b)
		t.NoError(err)

		op, ok := i.(SuffrageWithdrawOperation)
		t.True(ok)

		t.NoError(op.IsValid(networkID))

		return i
	}
	t.Compare = func(a, b interface{}) {
		af, ok := a.(SuffrageWithdrawOperation)
		t.True(ok)
		bf, ok := b.(SuffrageWithdrawOperation)
		t.True(ok)

		t.NoError(bf.IsValid(networkID))

		base.EqualOperation(t.Assert(), af, bf)
	}

	suite.Run(tt, t)
}
