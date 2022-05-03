package isaac

import (
	"testing"

	"github.com/spikeekips/mitum/base"
	"github.com/spikeekips/mitum/util"
	"github.com/spikeekips/mitum/util/encoder"
	jsonenc "github.com/spikeekips/mitum/util/encoder/json"
	"github.com/spikeekips/mitum/util/valuehash"
	"github.com/stretchr/testify/suite"
)

func TestSuffrageStateValueJSON(tt *testing.T) {
	t := new(encoder.BaseTestEncode)

	enc := jsonenc.NewEncoder()

	t.Encode = func() (interface{}, []byte) {
		t.NoError(enc.Add(encoder.DecodeDetail{Hint: base.StringAddressHint, Instance: base.StringAddress{}}))
		t.NoError(enc.Add(encoder.DecodeDetail{Hint: base.MPublickeyHint, Instance: base.MPublickey{}}))
		t.NoError(enc.Add(encoder.DecodeDetail{Hint: NodeHint, Instance: base.BaseNode{}}))
		t.NoError(enc.Add(encoder.DecodeDetail{Hint: SuffrageStateValueHint, Instance: SuffrageStateValue{}}))

		nodes := make([]base.Node, 3)
		for i := range nodes {
			nodes[i] = RandomLocalNode()
		}

		stv := NewSuffrageStateValue(base.Height(33), valuehash.RandomSHA256(), nodes)

		b, err := util.MarshalJSON(&stv)
		t.NoError(err)

		t.T().Log("marshaled:", string(b))

		return stv, b
	}

	t.Decode = func(b []byte) interface{} {
		i, err := enc.Decode(b)
		t.NoError(err)

		u, ok := i.(SuffrageStateValue)
		t.True(ok)

		return u
	}
	t.Compare = func(a, b interface{}) {
		av := a.(SuffrageStateValue)
		bv := b.(SuffrageStateValue)

		t.True(av.Hint().Equal(bv.Hint()))
		t.True(base.IsEqualStateValue(av, bv))
	}

	suite.Run(tt, t)
}

func TestSuffrageCandidateJSON(tt *testing.T) {
	t := new(encoder.BaseTestEncode)

	enc := jsonenc.NewEncoder()

	t.Encode = func() (interface{}, []byte) {
		t.NoError(enc.Add(encoder.DecodeDetail{Hint: base.StringAddressHint, Instance: base.StringAddress{}}))
		t.NoError(enc.Add(encoder.DecodeDetail{Hint: base.MPublickeyHint, Instance: base.MPublickey{}}))
		t.NoError(enc.Add(encoder.DecodeDetail{Hint: NodeHint, Instance: base.BaseNode{}}))
		t.NoError(enc.Add(encoder.DecodeDetail{Hint: SuffrageCandidateHint, Instance: SuffrageCandidate{}}))

		stv := NewSuffrageCandidate(RandomLocalNode(), base.Height(33), base.Height(55))

		b, err := util.MarshalJSON(&stv)
		t.NoError(err)

		t.T().Log("marshaled:", string(b))

		return stv, b
	}

	t.Decode = func(b []byte) interface{} {
		i, err := enc.Decode(b)
		t.NoError(err)

		u, ok := i.(SuffrageCandidate)
		t.True(ok)

		return u
	}
	t.Compare = func(a, b interface{}) {
		av := a.(SuffrageCandidate)
		bv := b.(SuffrageCandidate)

		base.EqualSuffrageCandidate(t.Assert(), av, bv)
	}

	suite.Run(tt, t)
}
