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

type testSuffrageExpelFact struct {
	suite.Suite
}

func (t *testSuffrageExpelFact) TestNew() {
	fact := NewSuffrageExpelFact(base.RandomAddress(""), base.Height(33), base.Height(44), util.UUID().String())
	t.NoError(fact.IsValid(nil))

	_ = (interface{})(fact).(base.SuffrageExpelFact)
}

func (t *testSuffrageExpelFact) TestIsValid() {
	t.Run("invalid BaseFact", func() {
		fact := NewSuffrageExpelFact(base.RandomAddress(""), base.Height(33), base.Height(44), util.UUID().String())
		fact.SetHash(nil)

		err := fact.IsValid(nil)
		t.Error(err)
		t.True(errors.Is(err, util.ErrInvalid))
		t.ErrorContains(err, "invalid BaseFact")
	})

	t.Run("invalid token", func() {
		fact := NewSuffrageExpelFact(base.RandomAddress(""), base.Height(33), base.Height(44), util.UUID().String())
		t.NoError(fact.SetToken(util.UUID().Bytes()))

		err := fact.IsValid(nil)
		t.Error(err)
		t.True(errors.Is(err, util.ErrInvalid))
		t.ErrorContains(err, "invalid token")
	})

	t.Run("empty node", func() {
		fact := NewSuffrageExpelFact(nil, base.Height(33), base.Height(44), util.UUID().String())
		err := fact.IsValid(nil)
		t.Error(err)
		t.True(errors.Is(err, util.ErrInvalid))
		t.ErrorContains(err, "invalid SuffrageExpel")
	})

	t.Run("bad node", func() {
		addr := base.NewStringAddress(strings.Repeat("a", base.MinAddressSize-base.AddressTypeSize-1))
		fact := NewSuffrageExpelFact(addr, base.Height(33), base.Height(44), util.UUID().String())
		err := fact.IsValid(nil)
		t.Error(err)
		t.True(errors.Is(err, util.ErrInvalid))
		t.ErrorContains(err, "invalid SuffrageExpel")
	})

	t.Run("empty start height", func() {
		fact := NewSuffrageExpelFact(base.RandomAddress(""), base.NilHeight, base.Height(44), util.UUID().String())
		err := fact.IsValid(nil)
		t.Error(err)
		t.True(errors.Is(err, util.ErrInvalid))
		t.ErrorContains(err, "invalid SuffrageExpel")
	})

	t.Run("empty end height", func() {
		fact := NewSuffrageExpelFact(base.RandomAddress(""), base.Height(33), base.NilHeight, util.UUID().String())
		err := fact.IsValid(nil)
		t.Error(err)
		t.True(errors.Is(err, util.ErrInvalid))
		t.ErrorContains(err, "invalid SuffrageExpel")
	})

	t.Run("wrong start height", func() {
		fact := NewSuffrageExpelFact(base.RandomAddress(""), base.Height(33), base.Height(22), util.UUID().String())
		err := fact.IsValid(nil)
		t.Error(err)
		t.True(errors.Is(err, util.ErrInvalid))
		t.ErrorContains(err, "invalid SuffrageExpel")
	})

	t.Run("start == end", func() {
		fact := NewSuffrageExpelFact(base.RandomAddress(""), base.Height(33), base.Height(33), util.UUID().String())
		t.NoError(fact.IsValid(nil))
	})

	t.Run("wrong hash", func() {
		fact := NewSuffrageExpelFact(base.RandomAddress(""), base.Height(33), base.Height(44), util.UUID().String())
		fact.SetHash(valuehash.NewBytes(util.UUID().Bytes()))

		err := fact.IsValid(nil)
		t.Error(err)
		t.True(errors.Is(err, util.ErrInvalid))
		t.ErrorContains(err, "hash does not match")
	})
}

func TestSuffrageExpelFact(t *testing.T) {
	suite.Run(t, new(testSuffrageExpelFact))
}

func TestSuffrageExpelFactEncode(tt *testing.T) {
	t := new(encoder.BaseTestEncode)

	enc := jsonenc.NewEncoder()

	t.Encode = func() (interface{}, []byte) {
		fact := NewSuffrageExpelFact(base.RandomAddress(""), base.Height(33), base.Height(44), util.UUID().String())

		b, err := enc.Marshal(fact)
		t.NoError(err)

		t.T().Log("marshaled:", string(b))

		return fact, b
	}
	t.Decode = func(b []byte) interface{} {
		t.NoError(enc.Add(encoder.DecodeDetail{Hint: base.StringAddressHint, Instance: base.StringAddress{}}))
		t.NoError(enc.Add(encoder.DecodeDetail{Hint: SuffrageExpelFactHint, Instance: SuffrageExpelFact{}}))

		i, err := enc.Decode(b)
		t.NoError(err)

		_, ok := i.(SuffrageExpelFact)
		t.True(ok)

		return i
	}
	t.Compare = func(a, b interface{}) {
		af, ok := a.(SuffrageExpelFact)
		t.True(ok)
		bf, ok := b.(SuffrageExpelFact)
		t.True(ok)

		t.NoError(bf.IsValid(nil))

		base.EqualFact(t.Assert(), af, bf)
		t.Equal(af.Reason(), bf.Reason())
	}

	suite.Run(tt, t)
}

type testSuffrageExpelOperation struct {
	suite.Suite
	priv      base.Privatekey
	networkID base.NetworkID
}

func (t *testSuffrageExpelOperation) SetupTest() {
	t.priv = base.NewMPrivatekey()
	t.networkID = util.UUID().Bytes()
}

func (t *testSuffrageExpelOperation) TestNew() {
	fact := NewSuffrageExpelFact(base.RandomAddress(""), base.Height(33), base.Height(44), util.UUID().String())
	op := NewSuffrageExpelOperation(fact)
	t.NoError(op.NodeSign(t.priv, t.networkID, base.RandomAddress("")))

	_ = (interface{})(op).(base.SuffrageExpelOperation)
}

func (t *testSuffrageExpelOperation) TestIsValid() {
	t.Run("ok", func() {
		fact := NewSuffrageExpelFact(base.RandomAddress(""), base.Height(33), base.Height(44), util.UUID().String())
		op := NewSuffrageExpelOperation(fact)
		t.NoError(op.NodeSign(t.priv, t.networkID, base.RandomAddress("")))

		t.NoError(op.IsValid(t.networkID))

		signs := op.NodeSigns()
		t.Equal(1, len(signs))
	})

	t.Run("target node signed", func() {
		fact := NewSuffrageExpelFact(base.RandomAddress(""), base.Height(33), base.Height(44), util.UUID().String())
		op := NewSuffrageExpelOperation(fact)
		t.NoError(op.NodeSign(t.priv, t.networkID, fact.Node()))

		err := op.IsValid(t.networkID)
		t.Error(err)
		t.True(errors.Is(err, util.ErrInvalid))
		t.ErrorContains(err, "valid node signs not found")

		signs := op.NodeSigns()
		t.Equal(0, len(signs))
	})

	t.Run("different network id", func() {
		fact := NewSuffrageExpelFact(base.RandomAddress(""), base.Height(33), base.Height(44), util.UUID().String())
		op := NewSuffrageExpelOperation(fact)
		t.NoError(op.NodeSign(t.priv, t.networkID, base.RandomAddress("")))

		err := op.IsValid(util.UUID().Bytes())
		t.Error(err)
		t.True(errors.Is(err, base.ErrSignatureVerification))
	})
}

func (t *testSuffrageExpelOperation) TestIsValidWithSuffrage() {
	t.Run("ok", func() {
		height := base.Height(33)
		local := base.RandomLocalNode()
		expelnode := base.RandomLocalNode()
		nodes := []base.Node{local, base.RandomLocalNode(), expelnode}

		fact := NewSuffrageExpelFact(expelnode.Address(), height, height+1, util.UUID().String())
		op := NewSuffrageExpelOperation(fact)
		t.NoError(op.NodeSign(local.Privatekey(), t.networkID, local.Address()))

		t.NoError(op.IsValid(t.networkID))

		suf, err := NewSuffrage(nodes)
		t.NoError(err)

		t.NoError(IsValidExpelWithSuffrageLifespan(height, op, suf, 1))
	})

	t.Run("expired", func() {
		height := base.Height(33)
		local := base.RandomLocalNode()
		expelnode := base.RandomLocalNode()
		nodes := []base.Node{local, base.RandomLocalNode(), expelnode}

		fact := NewSuffrageExpelFact(expelnode.Address(), height, height+1, util.UUID().String())
		op := NewSuffrageExpelOperation(fact)
		t.NoError(op.NodeSign(local.Privatekey(), t.networkID, local.Address()))

		t.NoError(op.IsValid(t.networkID))

		suf, err := NewSuffrage(nodes)
		t.NoError(err)

		err = IsValidExpelWithSuffrageLifespan(height+2, op, suf, 1)
		t.Error(err)
		t.ErrorContains(err, "expired")
	})

	t.Run("signed node not in suffrage", func() {
		height := base.Height(33)
		local := base.RandomLocalNode()
		expelnode := base.RandomLocalNode()
		nodes := []base.Node{local, expelnode}

		fact := NewSuffrageExpelFact(expelnode.Address(), height, height+1, util.UUID().String())
		op := NewSuffrageExpelOperation(fact)

		t.NoError(op.NodeSign(local.Privatekey(), t.networkID, local.Address()))
		t.NoError(op.NodeSign(base.NewMPrivatekey(), t.networkID, base.RandomAddress("")))

		t.NoError(op.IsValid(t.networkID))

		suf, err := NewSuffrage(nodes)
		t.NoError(err)

		err = IsValidExpelWithSuffrageLifespan(height, op, suf, 1)
		t.Error(err)
		t.ErrorContains(err, "unknown node signed")
	})

	t.Run("expel node not in suffrage", func() {
		height := base.Height(33)
		local := base.RandomLocalNode()
		expelnode := base.RandomLocalNode()
		nodes := []base.Node{local, base.RandomLocalNode()}

		fact := NewSuffrageExpelFact(expelnode.Address(), height, height+1, util.UUID().String())
		op := NewSuffrageExpelOperation(fact)
		t.NoError(op.NodeSign(local.Privatekey(), t.networkID, local.Address()))

		t.NoError(op.IsValid(t.networkID))

		suf, err := NewSuffrage(nodes)
		t.NoError(err)

		err = IsValidExpelWithSuffrageLifespan(height, op, suf, 1)
		t.Error(err)
		t.ErrorContains(err, "unknown expel node found")
	})
}

func TestSuffrageExpelOperation(t *testing.T) {
	suite.Run(t, new(testSuffrageExpelOperation))
}

func TestSuffrageExpelOperationEncode(tt *testing.T) {
	t := new(encoder.BaseTestEncode)

	enc := jsonenc.NewEncoder()
	networkID := util.UUID().Bytes()

	t.Encode = func() (interface{}, []byte) {
		fact := NewSuffrageExpelFact(base.RandomAddress(""), base.Height(33), base.Height(44), util.UUID().String())
		op := NewSuffrageExpelOperation(fact)
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
		t.NoError(enc.Add(encoder.DecodeDetail{Hint: base.MPublickeyHint, Instance: &base.MPublickey{}}))
		t.NoError(enc.Add(encoder.DecodeDetail{Hint: SuffrageExpelFactHint, Instance: SuffrageExpelFact{}}))
		t.NoError(enc.Add(encoder.DecodeDetail{Hint: SuffrageExpelOperationHint, Instance: SuffrageExpelOperation{}}))

		i, err := enc.Decode(b)
		t.NoError(err)

		op, ok := i.(SuffrageExpelOperation)
		t.True(ok)

		t.NoError(op.IsValid(networkID))

		return i
	}
	t.Compare = func(a, b interface{}) {
		af, ok := a.(SuffrageExpelOperation)
		t.True(ok)
		bf, ok := b.(SuffrageExpelOperation)
		t.True(ok)

		t.NoError(bf.IsValid(networkID))

		base.EqualOperation(t.Assert(), af, bf)
	}

	suite.Run(tt, t)
}
