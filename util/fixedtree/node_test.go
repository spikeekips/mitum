package fixedtree

import (
	"testing"

	"github.com/pkg/errors"
	"github.com/spikeekips/mitum/util"
	"github.com/spikeekips/mitum/util/encoder"
	jsonenc "github.com/spikeekips/mitum/util/encoder/json"
	"github.com/spikeekips/mitum/util/valuehash"
	"github.com/stretchr/testify/suite"
)

type testNode struct {
	suite.Suite
}

func (t *testNode) TestNew() {
	n := NewBaseNode(util.UUID().String())
	n.h = valuehash.RandomSHA256()

	t.NoError(n.IsValid(nil))
}

func (t *testNode) TestString() {
	t.Run("basic", func() {
		n := NewBaseNode(util.UUID().String())
		n.h = valuehash.RandomSHA256()

		s := n.String()
		t.T().Log("to string:", s)

		rn, err := ParseBaseNodeString(s)
		t.NoError(err)

		t.True(n.Equal(rn))
	})

	t.Run("empty", func() {
		n := EmptyBaseNode()

		s := n.String()
		t.T().Log("to string:", s)

		rn, err := ParseBaseNodeString(s)
		t.NoError(err)

		t.True(rn.IsEmpty())
	})
}

func (t *testNode) TestInvalid() {
	t.Run("empty node", func() {
		n := EmptyBaseNode()

		t.Equal(0, len(n.Hash().Bytes()))

		t.NoError(n.IsValid(nil))
	})

	t.Run("empty key", func() {
		n := NewBaseNode("")
		n.h = valuehash.RandomSHA256()

		err := n.IsValid(nil)
		t.True(errors.Is(err, util.InvalidError))
		t.ErrorContains(err, "empty key")
	})

	t.Run("empty hash", func() {
		n := NewBaseNode(util.UUID().String())

		err := n.IsValid(nil)
		t.True(errors.Is(err, util.InvalidError))
		t.ErrorContains(err, "empty hash")
	})

	t.Run("invalid hash", func() {
		n := NewBaseNode(util.UUID().String())
		n.h = valuehash.NewBytes(make([]byte, 300))

		err := n.IsValid(nil)
		t.True(errors.Is(err, util.InvalidError))
	})
}

func (t *testNode) TestEqual() {
	n := NewBaseNode(util.UUID().String())
	n.h = valuehash.RandomSHA256()

	t.NoError(n.IsValid(nil))

	t.Run("ok", func() {
		b := NewBaseNode(n.key)
		b.h = n.h

		t.True(n.Equal(b))
	})

	t.Run("key not match", func() {
		b := NewBaseNode(n.key + "showme")
		b.h = n.h

		t.False(n.Equal(b))
	})

	t.Run("nil hash", func() {
		b := NewBaseNode(n.key)

		t.False(n.Equal(b))
	})

	t.Run("nil not match", func() {
		b := NewBaseNode(n.key)
		b.h = valuehash.RandomSHA256()

		t.False(n.Equal(b))
	})

	t.Run("ok with empty", func() {
		b := EmptyBaseNode()

		t.False(n.Equal(b))
	})

	t.Run("empty with empty", func() {
		a := EmptyBaseNode()
		b := EmptyBaseNode()

		t.False(a.Equal(b))
	})
}

func TestNode(t *testing.T) {
	suite.Run(t, new(testNode))
}

func TestNodeEncode(tt *testing.T) {
	t := new(encoder.BaseTestEncode)

	enc := jsonenc.NewEncoder()

	t.Encode = func() (interface{}, []byte) {
		n := NewBaseNode(util.UUID().String())
		n.h = valuehash.RandomSHA256()

		b, err := enc.Marshal(n)
		t.NoError(err)

		return n, b
	}
	t.Decode = func(b []byte) interface{} {
		var u BaseNode
		t.NoError(enc.Unmarshal(b, &u))

		return u
	}
	t.Compare = func(a interface{}, b interface{}) {
		an := a.(BaseNode)
		bn := b.(BaseNode)

		t.NoError(an.IsValid(nil))
		t.NoError(bn.IsValid(nil))

		t.True(an.Equal(bn))
	}

	suite.Run(tt, t)
}

func TestEmptyNodeEncode(tt *testing.T) {
	t := new(encoder.BaseTestEncode)

	enc := jsonenc.NewEncoder()

	t.Encode = func() (interface{}, []byte) {
		n := EmptyBaseNode()

		b, err := enc.Marshal(n)
		t.NoError(err)
		t.T().Log("marshaled:", string(b))

		return n, b
	}
	t.Decode = func(b []byte) interface{} {
		var u BaseNode
		t.NoError(enc.Unmarshal(b, &u))

		return u
	}
	t.Compare = func(a interface{}, b interface{}) {
		an := a.(BaseNode)
		bn := b.(BaseNode)

		t.NoError(an.IsValid(nil))
		t.NoError(bn.IsValid(nil))

		t.False(an.Equal(bn))
	}

	suite.Run(tt, t)
}

func TestNodeEncodeEmptyKey(tt *testing.T) {
	t := new(encoder.BaseTestEncode)

	enc := jsonenc.NewEncoder()

	t.Encode = func() (interface{}, []byte) {
		n := NewBaseNode("")
		n.h = valuehash.RandomSHA256()

		b, err := enc.Marshal(n)
		t.NoError(err)

		return n, b
	}
	t.Decode = func(b []byte) interface{} {
		var u BaseNode
		t.NoError(enc.Unmarshal(b, &u))

		return u
	}
	t.Compare = func(a interface{}, b interface{}) {
		an := a.(BaseNode)
		bn := b.(BaseNode)

		t.Error(an.IsValid(nil))
		t.Error(bn.IsValid(nil))

		t.True(an.Equal(bn))
	}

	suite.Run(tt, t)
}

func TestNodeEncodeEmptyHash(tt *testing.T) {
	t := new(encoder.BaseTestEncode)

	enc := jsonenc.NewEncoder()

	t.Encode = func() (interface{}, []byte) {
		n := NewBaseNode(util.UUID().String())
		n.h = nil

		b, err := enc.Marshal(n)
		t.NoError(err)

		return n, b
	}
	t.Decode = func(b []byte) interface{} {
		var u BaseNode
		t.NoError(enc.Unmarshal(b, &u))

		return u
	}
	t.Compare = func(a interface{}, b interface{}) {
		an := a.(BaseNode)
		bn := b.(BaseNode)

		t.Error(an.IsValid(nil))
		t.Error(bn.IsValid(nil))

		t.True(an.Equal(bn))
	}

	suite.Run(tt, t)
}
