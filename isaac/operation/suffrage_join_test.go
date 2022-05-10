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
		t.True(errors.Is(err, util.InvalidError))
		t.ErrorContains(err, "invalid BaseFact")
	})

	t.Run("empty candidate", func() {
		fact := NewSuffrageJoinPermissionFact(nil, valuehash.RandomSHA256())
		err := fact.IsValid(nil)
		t.Error(err)
		t.True(errors.Is(err, util.InvalidError))
		t.ErrorContains(err, "invalid SuffrageJoinPermission")
	})

	t.Run("bad candidate", func() {
		addr := base.NewStringAddress(strings.Repeat("a", base.MinAddressSize-base.AddressTypeSize-1))
		fact := NewSuffrageJoinPermissionFact(addr, valuehash.RandomSHA256())
		err := fact.IsValid(nil)
		t.Error(err)
		t.True(errors.Is(err, util.InvalidError))
		t.ErrorContains(err, "invalid SuffrageJoinPermission")
	})

	t.Run("empty state", func() {
		fact := NewSuffrageJoinPermissionFact(base.RandomAddress(""), nil)
		err := fact.IsValid(nil)
		t.Error(err)
		t.True(errors.Is(err, util.InvalidError))
		t.ErrorContains(err, "invalid SuffrageJoinPermission")
	})

	t.Run("token == state", func() {
		fact := NewSuffrageJoinPermissionFact(base.RandomAddress(""), valuehash.RandomSHA256())
		fact.SetToken(valuehash.RandomSHA256().Bytes())

		err := fact.IsValid(nil)
		t.Error(err)
		t.True(errors.Is(err, util.InvalidError))
		t.ErrorContains(err, "invalid SuffrageJoinPermission")
		t.ErrorContains(err, "wrong token")
	})

	t.Run("wrong hash", func() {
		fact := NewSuffrageJoinPermissionFact(base.RandomAddress(""), valuehash.RandomSHA256())
		fact.SetHash(valuehash.NewBytes(util.UUID().Bytes()))

		err := fact.IsValid(nil)
		t.Error(err)
		t.True(errors.Is(err, util.InvalidError))
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

func (t *testSuffrageGenesisJoinPermissionFact) TestNew() {
	fact := NewSuffrageGenesisJoinPermissionFact(base.RandomAddress(""), t.pub, t.networkID)
	t.NoError(fact.IsValid(t.networkID))
}

func (t *testSuffrageGenesisJoinPermissionFact) TestIsValid() {
	t.Run("invalid BaseFact", func() {
		fact := NewSuffrageGenesisJoinPermissionFact(base.RandomAddress(""), t.pub, t.networkID)
		fact.SetToken(nil)

		err := fact.IsValid(t.networkID)
		t.Error(err)
		t.True(errors.Is(err, util.InvalidError))
		t.ErrorContains(err, "invalid BaseFact")
	})

	t.Run("empty node", func() {
		fact := NewSuffrageGenesisJoinPermissionFact(nil, t.pub, t.networkID)
		err := fact.IsValid(t.networkID)
		t.Error(err)
		t.True(errors.Is(err, util.InvalidError))
		t.ErrorContains(err, "invalid SuffrageGenesisJoinPermission")
	})

	t.Run("bad node", func() {
		addr := base.NewStringAddress(strings.Repeat("a", base.MinAddressSize-base.AddressTypeSize-1))
		fact := NewSuffrageGenesisJoinPermissionFact(addr, t.pub, t.networkID)
		err := fact.IsValid(t.networkID)
		t.Error(err)
		t.True(errors.Is(err, util.InvalidError))
		t.ErrorContains(err, "invalid SuffrageGenesisJoinPermission")
	})

	t.Run("empty publickey", func() {
		fact := NewSuffrageGenesisJoinPermissionFact(base.RandomAddress(""), nil, t.networkID)
		err := fact.IsValid(t.networkID)
		t.Error(err)
		t.True(errors.Is(err, util.InvalidError))
		t.ErrorContains(err, "invalid SuffrageGenesisJoinPermission")
	})

	t.Run("token == networkID", func() {
		fact := NewSuffrageGenesisJoinPermissionFact(base.RandomAddress(""), t.pub, t.networkID)
		fact.SetToken(valuehash.RandomSHA256().Bytes())

		err := fact.IsValid(t.networkID)
		t.Error(err)
		t.True(errors.Is(err, util.InvalidError))
		t.ErrorContains(err, "invalid SuffrageGenesisJoinPermission")
		t.ErrorContains(err, "wrong token")
	})

	t.Run("wrong hash", func() {
		fact := NewSuffrageGenesisJoinPermissionFact(base.RandomAddress(""), t.pub, t.networkID)
		fact.SetHash(valuehash.NewBytes(util.UUID().Bytes()))

		err := fact.IsValid(t.networkID)
		t.Error(err)
		t.True(errors.Is(err, util.InvalidError))
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
		fact := NewSuffrageGenesisJoinPermissionFact(base.RandomAddress(""), base.NewMPrivatekey().Publickey(), networkID)

		b, err := enc.Marshal(fact)
		t.NoError(err)

		t.T().Log("marshaled:", string(b))

		return fact, b
	}
	t.Decode = func(b []byte) interface{} {
		t.NoError(enc.Add(encoder.DecodeDetail{Hint: base.StringAddressHint, Instance: base.StringAddress{}}))
		t.NoError(enc.Add(encoder.DecodeDetail{Hint: base.MPublickeyHint, Instance: base.MPublickey{}}))
		t.NoError(enc.Add(encoder.DecodeDetail{Hint: SuffrageGenesisJoinPermissionFactHint, Instance: SuffrageGenesisJoinPermissionFact{}}))

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

func (t *testSuffrageGenesisJoin) TestNew() {
	fact := NewSuffrageGenesisJoinPermissionFact(base.RandomAddress(""), t.priv.Publickey(), t.networkID)
	op := NewSuffrageGenesisJoin(fact)
	t.NoError(op.Sign(t.priv, t.networkID))

	t.NoError(op.IsValid(t.networkID))

	_ = (interface{})(op).(base.Operation)
}

func (t *testSuffrageGenesisJoin) TestIsValid() {
	t.Run("invalid fact", func() {
		fact := NewSuffrageGenesisJoinPermissionFact(nil, t.priv.Publickey(), t.networkID)
		op := NewSuffrageGenesisJoin(fact)
		t.NoError(op.Sign(t.priv, t.networkID))

		err := op.IsValid(t.networkID)
		t.Error(err)
		t.True(errors.Is(err, util.InvalidError))
		t.ErrorContains(err, "invalid SuffrageGenesisJoin")
	})

	t.Run("multiple signed", func() {
		fact := NewSuffrageGenesisJoinPermissionFact(base.RandomAddress(""), t.priv.Publickey(), t.networkID)
		op := NewSuffrageGenesisJoin(fact)
		t.NoError(op.Sign(t.priv, t.networkID))
		t.NoError(op.Sign(base.NewMPrivatekey(), t.networkID))

		t.Equal(2, len(op.Signed()))

		err := op.IsValid(t.networkID)
		t.Error(err)
		t.True(errors.Is(err, util.InvalidError))
		t.ErrorContains(err, "multiple signed found")
	})

	t.Run("signer does not match with node publickey", func() {
		fact := NewSuffrageGenesisJoinPermissionFact(base.RandomAddress(""), t.priv.Publickey(), t.networkID)
		op := NewSuffrageGenesisJoin(fact)
		t.NoError(op.Sign(base.NewMPrivatekey(), t.networkID))

		err := op.IsValid(t.networkID)
		t.Error(err)
		t.True(errors.Is(err, util.InvalidError))
		t.ErrorContains(err, "signer does not match with publickey")
	})
}

func (t *testSuffrageGenesisJoin) TestPreProcess() {
	fact := NewSuffrageGenesisJoinPermissionFact(base.RandomAddress(""), t.priv.Publickey(), t.networkID)
	op := NewSuffrageGenesisJoin(fact)
	t.NoError(op.Sign(t.priv, t.networkID))

	reason, err := op.PreProcess(context.Background(), nil)
	t.NoError(err)
	t.Nil(reason)
}

func (t *testSuffrageGenesisJoin) TestProcess() {
	fact := NewSuffrageGenesisJoinPermissionFact(base.RandomAddress(""), t.priv.Publickey(), t.networkID)
	op := NewSuffrageGenesisJoin(fact)
	t.NoError(op.Sign(t.priv, t.networkID))

	values, reason, err := op.Process(context.Background(), nil)
	t.NoError(err)
	t.Nil(reason)

	t.Equal(1, len(values))

	value := values[0]
	t.Equal(isaac.SuffrageStateKey, value.Key())

	node := isaac.NewNode(fact.Publickey(), fact.Node())
	expected := isaac.NewSuffrageStateValue(base.GenesisHeight, []base.Node{node})

	t.True(base.IsEqualStateValue(expected, value.Value()))
}

func TestSuffrageGenesisJoin(t *testing.T) {
	suite.Run(t, new(testSuffrageGenesisJoin))
}
