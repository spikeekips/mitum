package isaacoperation

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/spikeekips/mitum/base"
	"github.com/spikeekips/mitum/isaac"
	"github.com/spikeekips/mitum/util"
	"github.com/spikeekips/mitum/util/encoder"
	jsonenc "github.com/spikeekips/mitum/util/encoder/json"
	"github.com/spikeekips/mitum/util/valuehash"
	"github.com/stretchr/testify/suite"
)

type testSuffrageJoinPermissionFact struct {
	suite.Suite
}

func (t *testSuffrageJoinPermissionFact) TestNew() {
	fact := NewSuffrageJoinPermissionFact(base.RandomAddress(""), valuehash.RandomSHA256())
	t.NoError(fact.IsValid(nil))
}

func (t *testSuffrageJoinPermissionFact) TestIsValid() {
	t.Run("invalid BaseFact", func() {
		fact := NewSuffrageJoinPermissionFact(base.RandomAddress(""), valuehash.RandomSHA256())
		fact.SetToken(nil)

		err := fact.IsValid(nil)
		t.Error(err)
		t.True(errors.Is(err, util.ErrInvalid))
		t.ErrorContains(err, "invalid BaseFact")
	})

	t.Run("empty candidate", func() {
		fact := NewSuffrageJoinPermissionFact(nil, valuehash.RandomSHA256())
		err := fact.IsValid(nil)
		t.Error(err)
		t.True(errors.Is(err, util.ErrInvalid))
		t.ErrorContains(err, "invalid SuffrageJoinPermission")
	})

	t.Run("bad candidate", func() {
		addr := base.NewStringAddress(strings.Repeat("a", base.MinAddressSize-base.AddressTypeSize-1))
		fact := NewSuffrageJoinPermissionFact(addr, valuehash.RandomSHA256())
		err := fact.IsValid(nil)
		t.Error(err)
		t.True(errors.Is(err, util.ErrInvalid))
		t.ErrorContains(err, "invalid SuffrageJoinPermission")
	})

	t.Run("empty state", func() {
		fact := NewSuffrageJoinPermissionFact(base.RandomAddress(""), nil)
		err := fact.IsValid(nil)
		t.Error(err)
		t.True(errors.Is(err, util.ErrInvalid))
		t.ErrorContains(err, "invalid SuffrageJoinPermission")
	})

	t.Run("token == state", func() {
		fact := NewSuffrageJoinPermissionFact(base.RandomAddress(""), valuehash.RandomSHA256())
		fact.SetToken(valuehash.RandomSHA256().Bytes())

		err := fact.IsValid(nil)
		t.Error(err)
		t.True(errors.Is(err, util.ErrInvalid))
		t.ErrorContains(err, "invalid SuffrageJoinPermission")
		t.ErrorContains(err, "wrong token")
	})

	t.Run("wrong hash", func() {
		fact := NewSuffrageJoinPermissionFact(base.RandomAddress(""), valuehash.RandomSHA256())
		fact.SetHash(valuehash.NewBytes(util.UUID().Bytes()))

		err := fact.IsValid(nil)
		t.Error(err)
		t.True(errors.Is(err, util.ErrInvalid))
		t.ErrorContains(err, "hash does not match")
	})
}

func TestSuffrageJoinPermissionFact(t *testing.T) {
	suite.Run(t, new(testSuffrageJoinPermissionFact))
}

func TestSuffrageJoinPermissionFactEncode(tt *testing.T) {
	t := new(encoder.BaseTestEncode)

	enc := jsonenc.NewEncoder()

	t.Encode = func() (interface{}, []byte) {
		fact := NewSuffrageJoinPermissionFact(base.RandomAddress(""), valuehash.RandomSHA256())

		b, err := enc.Marshal(fact)
		t.NoError(err)

		t.T().Log("marshaled:", string(b))

		return fact, b
	}
	t.Decode = func(b []byte) interface{} {
		t.NoError(enc.Add(encoder.DecodeDetail{Hint: base.StringAddressHint, Instance: base.StringAddress{}}))
		t.NoError(enc.Add(encoder.DecodeDetail{Hint: SuffrageJoinPermissionFactHint, Instance: SuffrageJoinPermissionFact{}}))

		i, err := enc.Decode(b)
		t.NoError(err)

		_, ok := i.(SuffrageJoinPermissionFact)
		t.True(ok)

		return i
	}
	t.Compare = func(a, b interface{}) {
		af, ok := a.(SuffrageJoinPermissionFact)
		t.True(ok)
		bf, ok := b.(SuffrageJoinPermissionFact)
		t.True(ok)

		t.NoError(bf.IsValid(nil))

		base.EqualFact(t.Assert(), af, bf)
	}

	suite.Run(tt, t)
}

type testSuffrageGenesisJoinPermissionFact struct {
	suite.Suite
	pub       base.Publickey
	networkID base.NetworkID
}

func (t *testSuffrageGenesisJoinPermissionFact) SetupSuite() {
	t.pub = base.NewMPrivatekey().Publickey()
	t.networkID = util.UUID().Bytes()
}

func (t *testSuffrageGenesisJoinPermissionFact) newFact() SuffrageGenesisJoinPermissionFact {
	node := isaac.NewNode(t.pub, base.RandomAddress(""))
	return NewSuffrageGenesisJoinPermissionFact([]base.Node{node}, t.networkID)
}

func (t *testSuffrageGenesisJoinPermissionFact) TestNew() {
	fact := t.newFact()
	t.NoError(fact.IsValid(t.networkID))
}

func (t *testSuffrageGenesisJoinPermissionFact) TestIsValid() {
	t.Run("invalid BaseFact", func() {
		fact := t.newFact()
		fact.SetToken(nil)

		err := fact.IsValid(t.networkID)
		t.Error(err)
		t.True(errors.Is(err, util.ErrInvalid))
		t.ErrorContains(err, "invalid BaseFact")
	})

	t.Run("empty node", func() {
		fact := NewSuffrageGenesisJoinPermissionFact(nil, t.networkID)
		err := fact.IsValid(t.networkID)
		t.Error(err)
		t.True(errors.Is(err, util.ErrInvalid))
		t.ErrorContains(err, "invalid SuffrageGenesisJoinPermission")
	})

	t.Run("bad node", func() {
		addr := base.NewStringAddress(strings.Repeat("a", base.MinAddressSize-base.AddressTypeSize-1))
		node := isaac.NewNode(t.pub, addr)

		fact := NewSuffrageGenesisJoinPermissionFact([]base.Node{node}, t.networkID)
		err := fact.IsValid(t.networkID)
		t.Error(err)
		t.True(errors.Is(err, util.ErrInvalid))
		t.ErrorContains(err, "invalid SuffrageGenesisJoinPermission")
	})

	t.Run("empty publickey", func() {
		node := isaac.NewNode(nil, base.RandomAddress(""))

		fact := NewSuffrageGenesisJoinPermissionFact([]base.Node{node}, t.networkID)
		err := fact.IsValid(t.networkID)
		t.Error(err)
		t.True(errors.Is(err, util.ErrInvalid))
		t.ErrorContains(err, "invalid SuffrageGenesisJoinPermission")
	})

	t.Run("token == networkID", func() {
		fact := t.newFact()
		fact.SetToken(valuehash.RandomSHA256().Bytes())

		err := fact.IsValid(t.networkID)
		t.Error(err)
		t.True(errors.Is(err, util.ErrInvalid))
		t.ErrorContains(err, "invalid SuffrageGenesisJoinPermission")
		t.ErrorContains(err, "wrong token")
	})

	t.Run("wrong hash", func() {
		fact := t.newFact()
		fact.SetHash(valuehash.NewBytes(util.UUID().Bytes()))

		err := fact.IsValid(t.networkID)
		t.Error(err)
		t.True(errors.Is(err, util.ErrInvalid))
		t.ErrorContains(err, "hash does not match")
	})
}

func TestSuffrageGenesisJoinPermissionFact(t *testing.T) {
	suite.Run(t, new(testSuffrageGenesisJoinPermissionFact))
}

func TestSuffrageGenesisJoinPermissionFactEncode(tt *testing.T) {
	t := new(encoder.BaseTestEncode)

	enc := jsonenc.NewEncoder()
	networkID := util.UUID().Bytes()

	t.Encode = func() (interface{}, []byte) {
		node := isaac.NewNode(base.NewMPrivatekey().Publickey(), base.RandomAddress(""))
		fact := NewSuffrageGenesisJoinPermissionFact([]base.Node{node}, networkID)

		b, err := enc.Marshal(fact)
		t.NoError(err)

		t.T().Log("marshaled:", string(b))

		return fact, b
	}
	t.Decode = func(b []byte) interface{} {
		t.NoError(enc.Add(encoder.DecodeDetail{Hint: base.StringAddressHint, Instance: base.StringAddress{}}))
		t.NoError(enc.Add(encoder.DecodeDetail{Hint: base.MPublickeyHint, Instance: base.MPublickey{}}))
		t.NoError(enc.Add(encoder.DecodeDetail{Hint: SuffrageGenesisJoinPermissionFactHint, Instance: SuffrageGenesisJoinPermissionFact{}}))
		t.NoError(enc.Add(encoder.DecodeDetail{Hint: isaac.NodeHint, Instance: base.BaseNode{}}))

		i, err := enc.Decode(b)
		t.NoError(err)

		_, ok := i.(SuffrageGenesisJoinPermissionFact)
		t.True(ok)

		return i
	}
	t.Compare = func(a, b interface{}) {
		af, ok := a.(SuffrageGenesisJoinPermissionFact)
		t.True(ok)
		bf, ok := b.(SuffrageGenesisJoinPermissionFact)
		t.True(ok)

		t.NoError(bf.IsValid(networkID))

		base.EqualFact(t.Assert(), af, bf)
	}

	suite.Run(tt, t)
}

type testSuffrageGenesisJoin struct {
	suite.Suite
	priv      base.Privatekey
	networkID base.NetworkID
}

func (t *testSuffrageGenesisJoin) SetupSuite() {
	t.priv = base.NewMPrivatekey()
	t.networkID = util.UUID().Bytes()
}

func (t *testSuffrageGenesisJoin) newFact() SuffrageGenesisJoinPermissionFact {
	node := isaac.NewNode(t.priv.Publickey(), base.RandomAddress(""))
	return NewSuffrageGenesisJoinPermissionFact([]base.Node{node}, t.networkID)
}

func (t *testSuffrageGenesisJoin) TestNew() {
	fact := t.newFact()
	op := NewSuffrageGenesisJoin(fact)
	t.NoError(op.Sign(t.priv, t.networkID))

	t.NoError(op.IsValid(t.networkID))

	_ = (interface{})(op).(base.Operation)
}

func (t *testSuffrageGenesisJoin) TestIsValid() {
	t.Run("invalid fact", func() {
		fact := t.newFact()
		fact.SetToken(nil)

		op := NewSuffrageGenesisJoin(fact)
		t.NoError(op.Sign(t.priv, t.networkID))

		err := op.IsValid(t.networkID)
		t.Error(err)
		t.True(errors.Is(err, util.ErrInvalid))
		t.ErrorContains(err, "invalid SuffrageGenesisJoin")
	})

	t.Run("multiple signed", func() {
		fact := t.newFact()
		op := NewSuffrageGenesisJoin(fact)
		t.NoError(op.Sign(t.priv, t.networkID))
		t.NoError(op.Sign(base.NewMPrivatekey(), t.networkID))

		t.Equal(2, len(op.Signed()))

		err := op.IsValid(t.networkID)
		t.Error(err)
		t.True(errors.Is(err, util.ErrInvalid))
		t.ErrorContains(err, "multiple signed found")
	})

	t.Run("signer does not match with node publickey", func() {
		fact := t.newFact()
		op := NewSuffrageGenesisJoin(fact)
		t.NoError(op.Sign(base.NewMPrivatekey(), t.networkID))

		t.NoError(op.IsValid(t.networkID))
	})
}

func (t *testSuffrageGenesisJoin) TestPreProcess() {
	fact := t.newFact()
	op := NewSuffrageGenesisJoin(fact)
	t.NoError(op.Sign(t.priv, t.networkID))

	reason, err := op.PreProcess(context.Background(), nil)
	t.NoError(err)
	t.Nil(reason)
}

func (t *testSuffrageGenesisJoin) TestProcess() {
	fact := t.newFact()
	op := NewSuffrageGenesisJoin(fact)
	t.NoError(op.Sign(t.priv, t.networkID))

	values, reason, err := op.Process(context.Background(), nil)
	t.NoError(err)
	t.Nil(reason)

	t.Equal(1, len(values))

	value := values[0]
	t.Equal(isaac.SuffrageStateKey, value.Key())

	expected := isaac.NewSuffrageStateValue(base.GenesisHeight, fact.Nodes())

	t.True(base.IsEqualStateValue(expected, value.Value()))
}

func TestSuffrageGenesisJoin(t *testing.T) {
	suite.Run(t, new(testSuffrageGenesisJoin))
}
