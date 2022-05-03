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

func TestSuffrageInfoJSON(tt *testing.T) {
	t := new(encoder.BaseTestEncode)

	enc := jsonenc.NewEncoder()

	t.Encode = func() (interface{}, []byte) {
		t.NoError(enc.Add(encoder.DecodeDetail{Hint: base.StringAddressHint, Instance: base.StringAddress{}}))
		t.NoError(enc.Add(encoder.DecodeDetail{Hint: base.MPublickeyHint, Instance: base.MPublickey{}}))
		t.NoError(enc.Add(encoder.DecodeDetail{Hint: NodeHint, Instance: base.BaseNode{}}))
		t.NoError(enc.Add(encoder.DecodeDetail{Hint: SuffrageCandidateHint, Instance: SuffrageCandidate{}}))
		t.NoError(enc.Add(encoder.DecodeDetail{Hint: SuffrageInfoHint, Instance: SuffrageInfo{}}))

		sufs := make([]base.Node, 3)
		for i := range sufs {
			sufs[i] = RandomLocalNode()
		}

		cans := make([]base.SuffrageCandidate, 4)
		for i := range cans {
			cans[i] = NewSuffrageCandidate(RandomLocalNode(), base.Height(33), base.Height(55))
		}

		stv := NewSuffrageNodesNetworkInfo(valuehash.RandomSHA256(), base.Height(3), sufs, cans)

		b, err := util.MarshalJSON(&stv)
		t.NoError(err)

		t.T().Log("marshaled:", string(b))

		return stv, b
	}

	t.Decode = func(b []byte) interface{} {
		i, err := enc.Decode(b)
		t.NoError(err)

		u, ok := i.(SuffrageInfo)
		t.True(ok)

		return u
	}
	t.Compare = func(a, b interface{}) {
		av := a.(SuffrageInfo)
		bv := b.(SuffrageInfo)

		t.Equal(len(av.suffrage), len(bv.suffrage))
		t.Equal(len(av.candidates), len(bv.candidates))

		for i := range av.suffrage {
			t.True(base.IsEqualNode(av.suffrage[i], bv.suffrage[i]))
		}

		for i := range av.candidates {
			base.EqualSuffrageCandidate(t.Assert(), av.candidates[i], bv.candidates[i])
		}
	}

	suite.Run(tt, t)
}
