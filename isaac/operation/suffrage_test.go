package isaacoperation

import (
	"errors"
	"testing"

	"github.com/spikeekips/mitum/base"
	"github.com/spikeekips/mitum/util"
	"github.com/spikeekips/mitum/util/encoder"
	jsonenc "github.com/spikeekips/mitum/util/encoder/json"
	"github.com/spikeekips/mitum/util/valuehash"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/suite"
)

type testSuffrageUpdateFact struct {
	suite.Suite
}

func (t *testSuffrageUpdateFact) TestNew() {
	news := []base.Node{base.RandomNode()}
	outs := []base.Address{base.RandomAddress("")}

	fact := NewSuffrageUpdateFact(util.UUID().Bytes(), news, outs)
	t.NoError(fact.IsValid(nil))

	_ = (interface{})(fact).(base.Fact)

	t.Run("same hash", func() {
		newfact := NewSuffrageUpdateFact(fact.Token(), news, outs)

		t.True(fact.Hash().Equal(newfact.Hash()))
	})

	t.Run("different token", func() {
		newfact := NewSuffrageUpdateFact(util.UUID().Bytes(), news, outs)

		t.False(fact.Hash().Equal(newfact.Hash()))
	})
}

func (t *testSuffrageUpdateFact) TestIsValid() {
	t.Run("invalid base fact", func() {
		news := []base.Node{base.RandomNode()}
		outs := []base.Address{base.RandomAddress("")}

		fact := NewSuffrageUpdateFact(nil, news, outs)
		err := fact.IsValid(nil)
		t.Error(err)
		t.True(errors.Is(err, util.InvalidError))
		t.Contains(err.Error(), "invalid Token")
	})

	t.Run("hash mismatch", func() {
		news := []base.Node{base.RandomNode()}
		outs := []base.Address{base.RandomAddress("")}

		fact := NewSuffrageUpdateFact(util.UUID().Bytes(), news, outs)
		fact.SetHash(valuehash.RandomSHA256())

		err := fact.IsValid(nil)
		t.Error(err)
		t.True(errors.Is(err, util.InvalidError))
		t.Contains(err.Error(), "hash does not match")
	})

	t.Run("nil hash", func() {
		news := []base.Node{base.RandomNode()}
		outs := []base.Address{base.RandomAddress("")}

		fact := NewSuffrageUpdateFact(util.UUID().Bytes(), news, outs)
		fact.SetHash(nil)

		err := fact.IsValid(nil)
		t.Error(err)
		t.True(errors.Is(err, util.InvalidError))
	})

	t.Run("empty news and outs", func() {
		fact := NewSuffrageUpdateFact(util.UUID().Bytes(), nil, nil)

		err := fact.IsValid(nil)
		t.Error(err)
		t.True(errors.Is(err, util.InvalidError))
		t.Contains(err.Error(), "empty new members and out members")
	})

	t.Run("duplicated news", func() {
		dup := base.RandomNode()
		news := []base.Node{dup, dup}
		outs := []base.Address{base.RandomAddress("")}

		fact := NewSuffrageUpdateFact(util.UUID().Bytes(), news, outs)

		err := fact.IsValid(nil)
		t.Error(err)
		t.True(errors.Is(err, util.InvalidError))
		t.Contains(err.Error(), "duplciated member found")
	})

	t.Run("duplicated outs", func() {
		dup := base.RandomAddress("")
		news := []base.Node{base.RandomNode()}
		outs := []base.Address{dup, dup}

		fact := NewSuffrageUpdateFact(util.UUID().Bytes(), news, outs)

		err := fact.IsValid(nil)
		t.Error(err)
		t.True(errors.Is(err, util.InvalidError))
		t.Contains(err.Error(), "duplciated member found")
	})

	t.Run("duplicated news and outs", func() {
		dup := base.RandomNode()
		news := []base.Node{dup}
		outs := []base.Address{dup.Address()}

		fact := NewSuffrageUpdateFact(util.UUID().Bytes(), news, outs)

		err := fact.IsValid(nil)
		t.Error(err)
		t.True(errors.Is(err, util.InvalidError))
		t.Contains(err.Error(), "duplciated member found")
	})

	t.Run("nil news and outs", func() {
		news := []base.Node{base.RandomNode(), nil}
		outs := []base.Address{nil}

		fact := NewSuffrageUpdateFact(util.UUID().Bytes(), news, outs)

		err := fact.IsValid(nil)
		t.Error(err)
		t.True(errors.Is(err, util.InvalidError))
		t.Contains(err.Error(), "duplciated member found")
	})
}

func TestSuffrageUpdateFact(t *testing.T) {
	suite.Run(t, new(testSuffrageUpdateFact))
}

func TestSuffrageUpdateFactEncode(tt *testing.T) {
	t := new(encoder.BaseTestEncode)

	enc := jsonenc.NewEncoder()

	t.Encode = func() (interface{}, []byte) {
		news := []base.Node{base.RandomNode()}
		outs := []base.Address{base.RandomAddress("")}

		fact := NewSuffrageUpdateFact(util.UUID().Bytes(), news, outs)
		t.NoError(fact.IsValid(nil))

		b, err := enc.Marshal(fact)
		t.NoError(err)

		t.T().Log("marshaled:", string(b))

		return fact, b
	}
	t.Decode = func(b []byte) interface{} {
		t.NoError(enc.Add(encoder.DecodeDetail{Hint: base.StringAddressHint, Instance: base.StringAddress{}}))
		t.NoError(enc.Add(encoder.DecodeDetail{Hint: SuffrageUpdateFactHint, Instance: SuffrageUpdateFact{}}))
		t.NoError(enc.Add(encoder.DecodeDetail{Hint: base.MPublickeyHint, Instance: base.MPublickey{}}))
		t.NoError(enc.Add(encoder.DecodeDetail{Hint: base.DummyNodeHint, Instance: base.BaseNode{}}))

		i, err := enc.Decode(b)
		t.NoError(err)

		_, ok := i.(SuffrageUpdateFact)
		t.True(ok)

		return i
	}
	t.Compare = func(a, b interface{}) {
		af, ok := a.(SuffrageUpdateFact)
		t.True(ok)
		bf, ok := b.(SuffrageUpdateFact)
		t.True(ok)

		t.NoError(bf.IsValid(nil))

		EqualSuffrageUpdateFact(t.Assert(), af, bf)
	}

	suite.Run(tt, t)
}

func EqualSuffrageUpdateFact(t *assert.Assertions, a, b SuffrageUpdateFact) {
	base.EqualFact(t, a, b)

	t.Equal(len(a.news), len(b.news))
	t.Equal(len(a.outs), len(b.outs))

	for i := range a.news {
		aa := a.news[i]
		ba := a.news[i]

		t.True(base.IsEqualNode(aa, ba))
	}

	for i := range a.outs {
		aa := a.outs[i]
		ba := a.outs[i]

		t.True(aa.Equal(ba))
	}
}

type testSuffrageUpdate struct {
	suite.Suite
	priv      base.Privatekey
	networkID base.NetworkID
}

func (t *testSuffrageUpdate) SetupSuite() {
	t.priv = base.NewMPrivatekey()
	t.networkID = util.UUID().Bytes()
}

func (t *testSuffrageUpdate) TestNew() {
	news := []base.Node{base.RandomNode()}
	outs := []base.Address{base.RandomAddress("")}

	fact := NewSuffrageUpdateFact(util.UUID().Bytes(), news, outs)
	t.NoError(fact.IsValid(nil))

	op := NewSuffrageUpdate(fact)
	t.NoError(op.Sign(t.priv, t.networkID))

	t.NoError(op.IsValid(t.networkID))
}

func TestSuffrageUpdate(t *testing.T) {
	suite.Run(t, new(testSuffrageUpdate))
}

func TestSuffrageUpdateEncode(tt *testing.T) {
	t := new(encoder.BaseTestEncode)

	enc := jsonenc.NewEncoder()

	networkID := base.NetworkID(util.UUID().Bytes())

	t.Encode = func() (interface{}, []byte) {
		news := []base.Node{base.RandomNode()}
		outs := []base.Address{base.RandomAddress("")}

		fact := NewSuffrageUpdateFact(util.UUID().Bytes(), news, outs)
		t.NoError(fact.IsValid(nil))

		op := NewSuffrageUpdate(fact)
		t.NoError(op.Sign(base.NewMPrivatekey(), networkID))

		t.NoError(op.IsValid(networkID))

		b, err := enc.Marshal(op)
		t.NoError(err)

		t.T().Log("marshaled:", string(b))

		return op, b
	}
	t.Decode = func(b []byte) interface{} {
		t.NoError(enc.Add(encoder.DecodeDetail{Hint: base.MPublickeyHint, Instance: base.MPublickey{}}))
		t.NoError(enc.Add(encoder.DecodeDetail{Hint: base.StringAddressHint, Instance: base.StringAddress{}}))
		t.NoError(enc.Add(encoder.DecodeDetail{Hint: SuffrageUpdateFactHint, Instance: SuffrageUpdateFact{}}))
		t.NoError(enc.Add(encoder.DecodeDetail{Hint: SuffrageUpdateHint, Instance: base.BaseOperation{}}))
		t.NoError(enc.Add(encoder.DecodeDetail{Hint: base.DummyNodeHint, Instance: base.BaseNode{}}))

		i, err := enc.Decode(b)
		t.NoError(err)

		_, ok := i.(base.BaseOperation)
		t.True(ok)

		return i
	}
	t.Compare = func(a, b interface{}) {
		ao, ok := a.(base.BaseOperation)
		t.True(ok)
		bo, ok := b.(base.BaseOperation)
		t.True(ok)

		t.NoError(bo.IsValid(networkID))

		EqualSuffrageUpdateFact(t.Assert(), ao.Fact().(SuffrageUpdateFact), bo.Fact().(SuffrageUpdateFact))
	}

	suite.Run(tt, t)
}
