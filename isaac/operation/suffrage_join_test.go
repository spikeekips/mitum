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

type testSuffrageJoinFact struct {
	suite.Suite
}

func (t *testSuffrageJoinFact) TestNew() {
	fact := NewSuffrageJoinFact(util.UUID().Bytes(), base.RandomAddress(""), base.Height(33))
	t.NoError(fact.IsValid(nil))
}

func (t *testSuffrageJoinFact) TestIsValid() {
	t.Run("invalid BaseFact", func() {
		fact := NewSuffrageJoinFact(util.UUID().Bytes(), base.RandomAddress(""), base.Height(33))
		fact.SetToken(nil)

		err := fact.IsValid(nil)
		t.Error(err)
		t.True(errors.Is(err, util.ErrInvalid))
		t.ErrorContains(err, "invalid BaseFact")
	})

	t.Run("empty candidate", func() {
		fact := NewSuffrageJoinFact(util.UUID().Bytes(), nil, base.Height(33))
		err := fact.IsValid(nil)
		t.Error(err)
		t.True(errors.Is(err, util.ErrInvalid))
		t.ErrorContains(err, "invalid SuffrageJoin")
	})

	t.Run("bad candidate", func() {
		addr := base.NewStringAddress(strings.Repeat("a", base.MinAddressSize-base.AddressTypeSize-1))
		fact := NewSuffrageJoinFact(util.UUID().Bytes(), addr, base.Height(33))
		err := fact.IsValid(nil)
		t.Error(err)
		t.True(errors.Is(err, util.ErrInvalid))
		t.ErrorContains(err, "invalid SuffrageJoin")
	})

	t.Run("empty height", func() {
		fact := NewSuffrageJoinFact(util.UUID().Bytes(), base.RandomAddress(""), base.NilHeight)
		err := fact.IsValid(nil)
		t.Error(err)
		t.True(errors.Is(err, util.ErrInvalid))
		t.ErrorContains(err, "invalid SuffrageJoin")
	})

	t.Run("wrong hash", func() {
		fact := NewSuffrageJoinFact(util.UUID().Bytes(), base.RandomAddress(""), base.Height(33))
		fact.SetHash(valuehash.NewBytes(util.UUID().Bytes()))

		err := fact.IsValid(nil)
		t.Error(err)
		t.True(errors.Is(err, util.ErrInvalid))
		t.ErrorContains(err, "hash does not match")
	})
}

func TestSuffrageJoinFact(t *testing.T) {
	suite.Run(t, new(testSuffrageJoinFact))
}

func TestSuffrageJoinFactEncode(tt *testing.T) {
	t := new(encoder.BaseTestEncode)

	enc := jsonenc.NewEncoder()

	t.Encode = func() (interface{}, []byte) {
		fact := NewSuffrageJoinFact(util.UUID().Bytes(), base.RandomAddress(""), base.Height(33))

		b, err := enc.Marshal(fact)
		t.NoError(err)

		t.T().Log("marshaled:", string(b))

		return fact, b
	}
	t.Decode = func(b []byte) interface{} {
		t.NoError(enc.Add(encoder.DecodeDetail{Hint: base.StringAddressHint, Instance: base.StringAddress{}}))
		t.NoError(enc.Add(encoder.DecodeDetail{Hint: SuffrageJoinFactHint, Instance: SuffrageJoinFact{}}))

		i, err := enc.Decode(b)
		t.NoError(err)

		_, ok := i.(SuffrageJoinFact)
		t.True(ok)

		return i
	}
	t.Compare = func(a, b interface{}) {
		af, ok := a.(SuffrageJoinFact)
		t.True(ok)
		bf, ok := b.(SuffrageJoinFact)
		t.True(ok)

		t.NoError(bf.IsValid(nil))

		base.EqualFact(t.Assert(), af, bf)
	}

	suite.Run(tt, t)
}

type testSuffrageJoin struct {
	suite.Suite
}

func (t *testSuffrageJoin) TestIsValid() {
	priv := base.NewMPrivatekey()
	networkID := util.UUID().Bytes()

	t.Run("ok", func() {
		fact := NewSuffrageJoinFact(util.UUID().Bytes(), base.RandomAddress(""), base.Height(33))
		op := NewSuffrageJoin(fact)
		t.NoError(op.Sign(priv, networkID, fact.Candidate()))

		t.NoError(op.IsValid(networkID))
	})

	t.Run("different network id", func() {
		fact := NewSuffrageJoinFact(util.UUID().Bytes(), base.RandomAddress(""), base.Height(33))
		op := NewSuffrageJoin(fact)
		t.NoError(op.Sign(priv, networkID, fact.Candidate()))

		err := op.IsValid(util.UUID().Bytes())
		t.Error(err)
		t.True(errors.Is(err, base.SignatureVerificationError))
	})

	t.Run("different node", func() {
		fact := NewSuffrageJoinFact(util.UUID().Bytes(), base.RandomAddress(""), base.Height(33))
		op := NewSuffrageJoin(fact)
		t.NoError(op.Sign(priv, networkID, base.RandomAddress("")))

		err := op.IsValid(networkID)
		t.Error(err)
		t.ErrorContains(err, "not signed by candidate")
	})
}

func TestSuffrageJoin(t *testing.T) {
	suite.Run(t, new(testSuffrageJoin))
}

func TestSuffrageJoinEncode(tt *testing.T) {
	t := new(encoder.BaseTestEncode)

	enc := jsonenc.NewEncoder()
	networkID := util.UUID().Bytes()

	t.Encode = func() (interface{}, []byte) {
		fact := NewSuffrageJoinFact(util.UUID().Bytes(), base.RandomAddress(""), base.Height(33))
		op := NewSuffrageJoin(fact)
		t.NoError(op.Sign(base.NewMPrivatekey(), networkID, fact.Candidate()))

		t.NoError(op.IsValid(networkID))

		b, err := enc.Marshal(op)
		t.NoError(err)

		t.T().Log("marshaled:", string(b))

		return op, b
	}
	t.Decode = func(b []byte) interface{} {
		t.NoError(enc.Add(encoder.DecodeDetail{Hint: base.StringAddressHint, Instance: base.StringAddress{}}))
		t.NoError(enc.Add(encoder.DecodeDetail{Hint: base.MPublickeyHint, Instance: base.MPublickey{}}))
		t.NoError(enc.Add(encoder.DecodeDetail{Hint: SuffrageJoinFactHint, Instance: SuffrageJoinFact{}}))
		t.NoError(enc.Add(encoder.DecodeDetail{Hint: SuffrageJoinHint, Instance: SuffrageJoin{}}))

		i, err := enc.Decode(b)
		t.NoError(err)

		op, ok := i.(SuffrageJoin)
		t.True(ok)

		t.NoError(op.IsValid(networkID))

		return i
	}
	t.Compare = func(a, b interface{}) {
		af, ok := a.(SuffrageJoin)
		t.True(ok)
		bf, ok := b.(SuffrageJoin)
		t.True(ok)

		t.NoError(bf.IsValid(networkID))

		base.EqualOperation(t.Assert(), af, bf)
	}

	suite.Run(tt, t)
}

type testSuffrageGenesisJoinFact struct {
	suite.Suite
	pub       base.Publickey
	networkID base.NetworkID
}

func (t *testSuffrageGenesisJoinFact) SetupSuite() {
	t.pub = base.NewMPrivatekey().Publickey()
	t.networkID = util.UUID().Bytes()
}

func (t *testSuffrageGenesisJoinFact) newFact() SuffrageGenesisJoinFact {
	node := isaac.NewNode(t.pub, base.RandomAddress(""))
	return NewSuffrageGenesisJoinFact([]base.Node{node}, t.networkID)
}

func (t *testSuffrageGenesisJoinFact) TestNew() {
	fact := t.newFact()
	t.NoError(fact.IsValid(t.networkID))
}

func (t *testSuffrageGenesisJoinFact) TestIsValid() {
	t.Run("invalid BaseFact", func() {
		fact := t.newFact()
		fact.SetToken(nil)

		err := fact.IsValid(t.networkID)
		t.Error(err)
		t.True(errors.Is(err, util.ErrInvalid))
		t.ErrorContains(err, "invalid BaseFact")
	})

	t.Run("empty node", func() {
		fact := NewSuffrageGenesisJoinFact(nil, t.networkID)
		err := fact.IsValid(t.networkID)
		t.Error(err)
		t.True(errors.Is(err, util.ErrInvalid))
		t.ErrorContains(err, "invalid SuffrageGenesisJoin")
	})

	t.Run("bad node", func() {
		addr := base.NewStringAddress(strings.Repeat("a", base.MinAddressSize-base.AddressTypeSize-1))
		node := isaac.NewNode(t.pub, addr)

		fact := NewSuffrageGenesisJoinFact([]base.Node{node}, t.networkID)
		err := fact.IsValid(t.networkID)
		t.Error(err)
		t.True(errors.Is(err, util.ErrInvalid))
		t.ErrorContains(err, "invalid SuffrageGenesisJoin")
	})

	t.Run("empty publickey", func() {
		node := isaac.NewNode(nil, base.RandomAddress(""))

		fact := NewSuffrageGenesisJoinFact([]base.Node{node}, t.networkID)
		err := fact.IsValid(t.networkID)
		t.Error(err)
		t.True(errors.Is(err, util.ErrInvalid))
		t.ErrorContains(err, "invalid SuffrageGenesisJoin")
	})

	t.Run("token == networkID", func() {
		fact := t.newFact()
		fact.SetToken(valuehash.RandomSHA256().Bytes())

		err := fact.IsValid(t.networkID)
		t.Error(err)
		t.True(errors.Is(err, util.ErrInvalid))
		t.ErrorContains(err, "invalid SuffrageGenesisJoin")
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

func TestSuffrageGenesisJoinFact(t *testing.T) {
	suite.Run(t, new(testSuffrageGenesisJoinFact))
}

func TestSuffrageGenesisJoinFactEncode(tt *testing.T) {
	t := new(encoder.BaseTestEncode)

	enc := jsonenc.NewEncoder()
	networkID := util.UUID().Bytes()

	t.Encode = func() (interface{}, []byte) {
		node := isaac.NewNode(base.NewMPrivatekey().Publickey(), base.RandomAddress(""))
		fact := NewSuffrageGenesisJoinFact([]base.Node{node}, networkID)

		b, err := enc.Marshal(fact)
		t.NoError(err)

		t.T().Log("marshaled:", string(b))

		return fact, b
	}
	t.Decode = func(b []byte) interface{} {
		t.NoError(enc.Add(encoder.DecodeDetail{Hint: base.StringAddressHint, Instance: base.StringAddress{}}))
		t.NoError(enc.Add(encoder.DecodeDetail{Hint: base.MPublickeyHint, Instance: base.MPublickey{}}))
		t.NoError(enc.Add(encoder.DecodeDetail{Hint: SuffrageGenesisJoinFactHint, Instance: SuffrageGenesisJoinFact{}}))
		t.NoError(enc.Add(encoder.DecodeDetail{Hint: isaac.NodeHint, Instance: base.BaseNode{}}))

		i, err := enc.Decode(b)
		t.NoError(err)

		_, ok := i.(SuffrageGenesisJoinFact)
		t.True(ok)

		return i
	}
	t.Compare = func(a, b interface{}) {
		af, ok := a.(SuffrageGenesisJoinFact)
		t.True(ok)
		bf, ok := b.(SuffrageGenesisJoinFact)
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

func (t *testSuffrageGenesisJoin) newFact() SuffrageGenesisJoinFact {
	node := isaac.NewNode(t.priv.Publickey(), base.RandomAddress(""))
	return NewSuffrageGenesisJoinFact([]base.Node{node}, t.networkID)
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

	reason, err := op.PreProcess(context.Background(), base.NilGetState)
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

	fnodes := fact.Nodes()
	nodes := make([]base.SuffrageNodeStateValue, len(fnodes))
	for i := range fnodes {
		nodes[i] = isaac.NewSuffrageNodeStateValue(fnodes[i], base.GenesisHeight+1)
	}

	expected := isaac.NewSuffrageNodesStateValue(base.GenesisHeight, nodes)

	t.True(base.IsEqualStateValue(expected, value.Value()))
}

func TestSuffrageGenesisJoin(t *testing.T) {
	suite.Run(t, new(testSuffrageGenesisJoin))
}
