package fixedtree

import (
	"testing"

	"github.com/spikeekips/mitum/util"
	mencoder "github.com/spikeekips/mitum/util/encoder"
	"github.com/stretchr/testify/suite"
)

func emptyBaseNode() *BaseNode {
	return &BaseNode{}
}

type testNode struct {
	suite.Suite
}

func (t *testNode) newNode(left, right Node) *BaseNode {
	n := NewBaseNode(util.UUID().String())
	t.NoError(t.updateHash(n, nil, nil))

	return n
}

func (t *testNode) updateHash(n *BaseNode, left, right Node) error {
	_, c, err := n.SetHash(left, right)
	if err != nil {
		return err
	}

	return c()
}

func (t *testNode) TestNew() {
	n := t.newNode(nil, nil)

	t.NoError(n.IsValid(nil))
}

func (t *testNode) TestInvalid() {
	t.Run("empty key", func() {
		n := emptyBaseNode()

		err := n.IsValid(nil)
		t.ErrorIs(err, util.ErrInvalid)
		t.ErrorContains(err, "empty key")
	})

	t.Run("empty hash", func() {
		n := NewBaseNode(util.UUID().String())
		n.h = ""

		err := n.IsValid(nil)
		t.ErrorIs(err, util.ErrInvalid)
		t.ErrorContains(err, "empty node hash")
	})
}

func (t *testNode) TestHash() {
	t.Run("empty left, right", func() {
		key := util.UUID().String()

		a := NewBaseNode(key)
		t.NoError(t.updateHash(a, nil, nil))

		b := NewBaseNode(key)
		t.NoError(t.updateHash(b, nil, nil))

		t.T().Log("a:", a.Hash())
		t.T().Log("b:", a.Hash())

		t.True(a.Equal(b))
	})

	t.Run("nil left, not nil right", func() {
		key := util.UUID().String()

		right := t.newNode(nil, nil)

		a := NewBaseNode(key)
		t.NoError(t.updateHash(a, nil, right))

		b := NewBaseNode(key)
		t.NoError(t.updateHash(b, nil, right))

		t.True(a.Equal(b))
	})

	t.Run("not nil left, nil right", func() {
		key := util.UUID().String()

		left := t.newNode(nil, nil)

		a := NewBaseNode(key)
		t.NoError(t.updateHash(a, nil, left))

		b := NewBaseNode(key)
		t.NoError(t.updateHash(b, nil, left))

		t.True(a.Equal(b))
	})

	t.Run("already hashed", func() {
		a := t.newNode(nil, nil)
		err := t.updateHash(a, nil, nil)
		t.Error(err)
		t.ErrorContains(err, "already hashed")
	})
}

func (t *testNode) TestEqual() {
	n := t.newNode(nil, nil)

	t.NoError(n.IsValid(nil))

	t.Run("ok", func() {
		b := NewBaseNode(n.key)
		t.NoError(t.updateHash(b, nil, nil))

		t.True(n.Equal(b))
	})

	t.Run("key not match", func() {
		b := NewBaseNode(n.key + "showme")
		t.NoError(t.updateHash(b, nil, nil))

		t.False(n.Equal(b))
	})

	t.Run("empty hash", func() {
		b := NewBaseNode(n.key)

		t.False(n.Equal(b))
	})

	t.Run("hash not match", func() {
		b := NewBaseNode(n.key)
		b.h = util.UUID().String()

		t.False(n.Equal(b))
	})

	t.Run("ok with empty", func() {
		b := emptyBaseNode()

		t.False(n.Equal(b))
	})

	t.Run("empty with empty", func() {
		a := emptyBaseNode()
		b := emptyBaseNode()

		t.True(a.Equal(b))
	})

	t.Run("empty with none-empty", func() {
		a := emptyBaseNode()
		b := NewBaseNode(n.key)
		b.h = util.UUID().String()

		t.False(a.Equal(b))
	})
}

func TestNode(t *testing.T) {
	suite.Run(t, new(testNode))
}

func TestNodeEncode(tt *testing.T) {
	t := new(mencoder.BaseTestEncode)

	t.Encode = func() (interface{}, []byte) {
		n := NewBaseNode(util.UUID().String())
		_, c, err := n.SetHash(nil, nil)
		t.NoError(err)
		c()

		b, err := util.MarshalJSON(n)
		t.NoError(err)

		return n, b
	}
	t.Decode = func(b []byte) interface{} {
		var u *BaseNode
		t.NoError(util.UnmarshalJSON(b, &u))

		return u
	}
	t.Compare = func(a interface{}, b interface{}) {
		an := a.(*BaseNode)
		bn := b.(*BaseNode)

		t.NoError(an.IsValid(nil))
		t.NoError(bn.IsValid(nil))

		t.True(an.Equal(bn))
	}

	suite.Run(tt, t)
}

func TestEmptyNodeEncode(tt *testing.T) {
	t := new(mencoder.BaseTestEncode)

	t.Encode = func() (interface{}, []byte) {
		n := emptyBaseNode()

		b, err := util.MarshalJSON(n)
		t.NoError(err)
		t.T().Log("marshaled:", string(b))

		return n, b
	}
	t.Decode = func(b []byte) interface{} {
		var u *BaseNode
		t.NoError(util.UnmarshalJSON(b, &u))

		t.Nil(u)

		return u
	}
	t.Compare = func(a interface{}, b interface{}) {
		an := a.(*BaseNode)
		bn := b.(*BaseNode)

		t.Nil(bn)

		t.True(an.Equal(bn))
	}

	suite.Run(tt, t)
}

func TestNodeEncodeEmptyKey(tt *testing.T) {
	t := new(mencoder.BaseTestEncode)

	t.Encode = func() (interface{}, []byte) {
		n := NewBaseNode("")
		n.h = util.UUID().String()

		b, err := util.MarshalJSON(n)
		t.NoError(err)

		return n, b
	}
	t.Decode = func(b []byte) interface{} {
		var u *BaseNode
		t.NoError(util.UnmarshalJSON(b, &u))

		return u
	}
	t.Compare = func(a interface{}, b interface{}) {
		an := a.(*BaseNode)
		bn := b.(*BaseNode)

		t.Error(an.IsValid(nil))
		t.Error(bn.IsValid(nil))

		t.True(an.Equal(bn))
	}

	suite.Run(tt, t)
}

func TestNodeEncodeEmptyHash(tt *testing.T) {
	t := new(mencoder.BaseTestEncode)

	t.Encode = func() (interface{}, []byte) {
		n := NewBaseNode(util.UUID().String())
		n.h = ""

		b, err := util.MarshalJSON(n)
		t.NoError(err)

		return n, b
	}
	t.Decode = func(b []byte) interface{} {
		var u *BaseNode
		t.NoError(util.UnmarshalJSON(b, &u))

		return u
	}
	t.Compare = func(a interface{}, b interface{}) {
		an := a.(*BaseNode)
		bn := b.(*BaseNode)

		t.Error(an.IsValid(nil))
		t.Error(bn.IsValid(nil))

		t.True(an.Equal(bn))
	}

	suite.Run(tt, t)
}
