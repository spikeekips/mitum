package isaac

import (
	"testing"

	"github.com/spikeekips/mitum/base"
	"github.com/spikeekips/mitum/util/encoder"
	jsonenc "github.com/spikeekips/mitum/util/encoder/json"
	"github.com/stretchr/testify/suite"
)

func TestLocalNodeEncode(tt *testing.T) {
	t := new(encoder.BaseTestEncode)

	enc := jsonenc.NewEncoder()

	t.Encode = func() (interface{}, []byte) {
		node := NewLocalNode(base.NewMPrivatekey(), base.RandomAddress(""))
		b, err := enc.Marshal(node)
		t.NoError(err)

		t.T().Log("marshaled:", string(b))

		return node, b
	}
	t.Decode = func(b []byte) interface{} {
		t.NoError(enc.Add(encoder.DecodeDetail{Hint: base.MPublickeyHint, Instance: &base.MPublickey{}}))
		t.NoError(enc.Add(encoder.DecodeDetail{Hint: base.StringAddressHint, Instance: base.StringAddress{}}))
		t.NoError(enc.Add(encoder.DecodeDetail{Hint: NodeHint, Instance: base.BaseNode{}}))

		i, err := enc.Decode(b)
		t.NoError(err)

		_, ok := i.(base.BaseNode)
		t.True(ok)

		return i
	}
	t.Compare = func(a, b interface{}) {
		an, ok := a.(base.Node)
		t.True(ok)
		bn, ok := b.(base.Node)
		t.True(ok)

		t.NoError(bn.IsValid(nil))

		t.True(base.IsEqualNode(an, bn))
	}

	suite.Run(tt, t)
}
