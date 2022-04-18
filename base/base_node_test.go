package base

import (
	"testing"

	"github.com/spikeekips/mitum/util/encoder"
	jsonenc "github.com/spikeekips/mitum/util/encoder/json"
	"github.com/spikeekips/mitum/util/hint"
	"github.com/stretchr/testify/suite"
)

func TestBaseNodeEncode(tt *testing.T) {
	t := new(encoder.BaseTestEncode)

	enc := jsonenc.NewEncoder()

	ht := hint.MustNewHint("test-node-v0.0.1")

	t.Encode = func() (interface{}, []byte) {
		node := NewBaseNode(ht, NewMPrivatekey().Publickey(), RandomAddress(""))
		b, err := enc.Marshal(node)
		t.NoError(err)

		t.T().Log("marshaled:", string(b))

		return node, b
	}
	t.Decode = func(b []byte) interface{} {
		t.NoError(enc.Add(encoder.DecodeDetail{Hint: MPublickeyHint, Instance: MPublickey{}}))
		t.NoError(enc.Add(encoder.DecodeDetail{Hint: StringAddressHint, Instance: StringAddress{}}))
		t.NoError(enc.Add(encoder.DecodeDetail{Hint: ht, Instance: BaseNode{}}))

		i, err := enc.Decode(b)
		t.NoError(err)

		_, ok := i.(Node)
		t.True(ok)
		_, ok = i.(BaseNode)
		t.True(ok)

		return i
	}
	t.Compare = func(a, b interface{}) {
		an, ok := a.(BaseNode)
		t.True(ok)
		bn, ok := b.(BaseNode)
		t.True(ok)

		t.NoError(bn.IsValid(nil))

		t.True(IsEqualNode(an, bn))
	}

	suite.Run(tt, t)
}
