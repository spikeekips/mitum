package dop5

import (
	"slices"
	"testing"

	"github.com/spikeekips/mitum/example/test364_5/fixedtree"
	dutil "github.com/spikeekips/mitum/example/test364_5/util"
	"github.com/spikeekips/mitum/util"
	mencoder "github.com/spikeekips/mitum/util/encoder"
	"github.com/spikeekips/mitum/util/valuehash"
	"github.com/stretchr/testify/suite"
)

type testStateTreeNode struct {
	dutil.BaseSuite
}

func (t *testStateTreeNode) nodeWithoutHash() *StateTreeNode {
	n, _ := NewStateTreeNode(util.UUID().String(), valuehash.RandomSHA256())

	return n
}

func (t *testStateTreeNode) node() *StateTreeNode {
	n := t.nodeWithoutHash()
	_, f, err := n.SetHash(nil, nil)
	t.NoError(err)
	t.NoError(f())

	return n
}

func (t *testStateTreeNode) TestIsValid() {
	t.Run("ok", func() {
		n := t.node()

		t.NoError(n.IsValid(nil))
	})

	t.Run("empty node hash", func() {
		n := t.nodeWithoutHash()

		err := n.IsValid(nil)
		t.ErrorIs(err, util.ErrInvalid)
		t.ErrorContains(err, "empty node hash")
	})

	t.Run("empty state key", func() {
		n, err := NewStateTreeNode("", valuehash.RandomSHA256())
		t.NoError(err)

		err = n.IsValid(nil)
		t.ErrorIs(err, util.ErrInvalid)
		t.ErrorContains(err, "empty key")
	})

	t.Run("nil state key", func() {
		_, err := NewStateTreeNode(util.UUID().String(), nil)
		t.ErrorContains(err, "nil state hash")
	})
}

func (t *testStateTreeNode) TestHash() {
	t.Run("ok", func() {
		key, sh := util.UUID().String(), valuehash.RandomSHA256()
		n, err := NewStateTreeNode(key, sh)
		t.NoError(err)

		h, f, err := n.SetHash(nil, nil)
		t.NoError(err)
		t.NoError(f())

		rh, err := fixedtree.MakeBaseNodeHash(n, nil, nil, func() ([]byte, error) {
			return slices.Concat(
				[]byte(key),
				sh.Bytes(),
			), nil
		})
		t.NoError(err)

		t.Equal(h, rh)
	})

	t.Run("empty state key", func() {
		sh := valuehash.RandomSHA256()
		n, err := NewStateTreeNode("", sh)
		t.NoError(err)

		_, _, err = n.SetHash(nil, nil)
		t.ErrorContains(err, "empty key")
	})
}

func TestStateTreeNode(t *testing.T) {
	suite.Run(t, new(testStateTreeNode))
}

type testOperationTreeNode struct {
	dutil.BaseSuite
}

func (t *testOperationTreeNode) nodeWithoutHash() *OperationTreeNode {
	n, _ := NewOperationTreeNode(valuehash.RandomSHA256(), valuehash.RandomSHA256(), util.UUID().String())

	return n
}

func (t *testOperationTreeNode) node() *OperationTreeNode {
	n := t.nodeWithoutHash()
	_, f, err := n.SetHash(nil, nil)
	t.NoError(err)
	t.NoError(f())

	return n
}

func (t *testOperationTreeNode) TestOperationNodeIsValid() {
	t.Run("ok", func() {
		n := t.node()

		t.NoError(n.IsValid(nil))
	})

	t.Run("empty node hash", func() {
		n := t.nodeWithoutHash()

		err := n.IsValid(nil)
		t.ErrorIs(err, util.ErrInvalid)
		t.ErrorContains(err, "empty node hash")
	})

	t.Run("nil oph", func() {
		_, err := NewOperationTreeNode(nil, valuehash.RandomSHA256(), util.UUID().String())
		t.ErrorContains(err, "nil oph")
	})

	t.Run("nil fact", func() {
		_, err := NewOperationTreeNode(valuehash.RandomSHA256(), nil, util.UUID().String())
		t.ErrorContains(err, "nil fact")
	})
}

func (t *testOperationTreeNode) TestOperationNodeHash() {
	t.Run("ok", func() {
		oph, fact, reason := valuehash.RandomSHA256(), valuehash.RandomSHA256(), util.UUID().String()
		n, err := NewOperationTreeNode(oph, fact, reason)
		t.NoError(err)

		h, f, err := n.SetHash(nil, nil)
		t.NoError(err)
		t.NoError(f())

		rh, err := fixedtree.MakeBaseNodeHash(n, nil, nil, func() ([]byte, error) {
			return slices.Concat(
				[]byte(oph.String()),
				fact.Bytes(),
				util.BoolToBytes(reason == ""),
			), nil
		})
		t.NoError(err)

		t.Equal(h, rh)
	})
}

func (t *testOperationTreeNode) TestSetReason() {
	t.Run("hash", func() {
		oph, fact, reason := valuehash.RandomSHA256(), valuehash.RandomSHA256(), util.UUID().String()
		n, err := NewOperationTreeNode(oph, fact, reason)
		t.NoError(err)

		ah, _, err := n.SetHash(nil, nil)
		t.NoError(err)

		n.SetReason("")

		bh, _, err := n.SetHash(nil, nil)
		t.NoError(err)

		t.NotEqual(ah, bh)
	})
}

func TestOperationTreeNode(t *testing.T) {
	suite.Run(t, new(testOperationTreeNode))
}

func TestStateTreeNodeEncode(tt *testing.T) {
	t := new(mencoder.BaseTestEncode)

	t.Encode = func() (interface{}, []byte) {
		n, err := NewStateTreeNode(util.UUID().String(), valuehash.RandomSHA256())
		t.NoError(err)
		_, f, err := n.SetHash(nil, nil)
		t.NoError(f())

		b, err := util.MarshalJSON(n)
		t.NoError(err)

		t.T().Log("marshaled:", string(b))

		return n, b
	}
	t.Decode = func(b []byte) interface{} {
		var u *StateTreeNode
		t.NoError(util.UnmarshalJSON(b, &u))

		return u
	}
	t.Compare = func(a interface{}, b interface{}) {
		an := a.(*StateTreeNode)
		bn := b.(*StateTreeNode)

		t.NoError(an.IsValid(nil))
		t.NoError(bn.IsValid(nil))

		t.Equal(an.Key(), bn.Key())
		t.True(an.h.Equal(bn.h))

		t.True(an.Equal(bn))
	}

	suite.Run(tt, t)
}

func TestOperationTreeNodeEncode(tt *testing.T) {
	t := new(mencoder.BaseTestEncode)

	t.Encode = func() (interface{}, []byte) {
		n, err := NewOperationTreeNode(valuehash.RandomSHA256(), valuehash.RandomSHA256(), util.UUID().String())
		t.NoError(err)
		_, f, err := n.SetHash(nil, nil)
		t.NoError(f())

		b, err := util.MarshalJSON(n)
		t.NoError(err)

		t.T().Log("marshaled:", string(b))

		return n, b
	}
	t.Decode = func(b []byte) interface{} {
		var u *OperationTreeNode
		t.NoError(util.UnmarshalJSON(b, &u))

		return u
	}
	t.Compare = func(a interface{}, b interface{}) {
		an := a.(*OperationTreeNode)
		bn := b.(*OperationTreeNode)

		t.NoError(an.IsValid(nil))
		t.NoError(bn.IsValid(nil))

		t.Equal(an.Key(), bn.Key())
		t.True(an.fact.Equal(bn.fact))
		t.Equal(an.reason, bn.reason)

		t.True(an.Equal(bn))
	}

	suite.Run(tt, t)
}
