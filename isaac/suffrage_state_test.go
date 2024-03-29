package isaac

import (
	"testing"

	"github.com/spikeekips/mitum/base"
	"github.com/spikeekips/mitum/util"
	"github.com/spikeekips/mitum/util/encoder"
	jsonenc "github.com/spikeekips/mitum/util/encoder/json"
	"github.com/stretchr/testify/suite"
)

func TestSuffrageNodeStateValueJSON(tt *testing.T) {
	t := new(encoder.BaseTestEncode)

	enc := jsonenc.NewEncoder()

	t.Encode = func() (interface{}, []byte) {
		t.NoError(enc.Add(encoder.DecodeDetail{Hint: base.StringAddressHint, Instance: base.StringAddress{}}))
		t.NoError(enc.Add(encoder.DecodeDetail{Hint: base.MPublickeyHint, Instance: &base.MPublickey{}}))
		t.NoError(enc.Add(encoder.DecodeDetail{Hint: base.DummyNodeHint, Instance: base.BaseNode{}}))
		t.NoError(enc.Add(encoder.DecodeDetail{Hint: SuffrageNodeStateValueHint, Instance: SuffrageNodeStateValue{}}))

		stv := NewSuffrageNodeStateValue(base.RandomLocalNode(), base.Height(33))

		b, err := util.MarshalJSON(stv)
		t.NoError(err)

		t.T().Log("marshaled:", string(b))

		return stv, b
	}

	t.Decode = func(b []byte) interface{} {
		i, err := enc.Decode(b)
		t.NoError(err)

		u, ok := i.(SuffrageNodeStateValue)
		t.True(ok)

		return u
	}
	t.Compare = func(a, b interface{}) {
		av := a.(SuffrageNodeStateValue)
		bv := b.(SuffrageNodeStateValue)

		t.True(av.Hint().Equal(bv.Hint()))
		t.True(base.IsEqualStateValue(av, bv))
	}

	suite.Run(tt, t)
}

func TestSuffrageNodesStateValueJSON(tt *testing.T) {
	t := new(encoder.BaseTestEncode)

	enc := jsonenc.NewEncoder()

	t.Encode = func() (interface{}, []byte) {
		t.NoError(enc.Add(encoder.DecodeDetail{Hint: base.StringAddressHint, Instance: base.StringAddress{}}))
		t.NoError(enc.Add(encoder.DecodeDetail{Hint: base.MPublickeyHint, Instance: &base.MPublickey{}}))
		t.NoError(enc.Add(encoder.DecodeDetail{Hint: base.DummyNodeHint, Instance: base.BaseNode{}}))
		t.NoError(enc.Add(encoder.DecodeDetail{Hint: SuffrageNodesStateValueHint, Instance: SuffrageNodesStateValue{}}))
		t.NoError(enc.Add(encoder.DecodeDetail{Hint: SuffrageNodeStateValueHint, Instance: SuffrageNodeStateValue{}}))

		nodes := make([]base.SuffrageNodeStateValue, 3)
		for i := range nodes {
			nodes[i] = NewSuffrageNodeStateValue(base.RandomLocalNode(), base.Height(33))
		}

		stv := NewSuffrageNodesStateValue(base.Height(33), nodes)

		b, err := util.MarshalJSON(stv)
		t.NoError(err)

		t.T().Log("marshaled:", string(b))

		return stv, b
	}

	t.Decode = func(b []byte) interface{} {
		i, err := enc.Decode(b)
		t.NoError(err)

		u, ok := i.(SuffrageNodesStateValue)
		t.True(ok)

		return u
	}
	t.Compare = func(a, b interface{}) {
		av := a.(SuffrageNodesStateValue)
		bv := b.(SuffrageNodesStateValue)

		t.True(av.Hint().Equal(bv.Hint()))
		t.True(base.IsEqualStateValue(av, bv))
	}

	suite.Run(tt, t)
}

func TestSuffrageCandidatesStateValueJSON(tt *testing.T) {
	t := new(encoder.BaseTestEncode)

	enc := jsonenc.NewEncoder()

	t.Encode = func() (interface{}, []byte) {
		t.NoError(enc.Add(encoder.DecodeDetail{Hint: base.StringAddressHint, Instance: base.StringAddress{}}))
		t.NoError(enc.Add(encoder.DecodeDetail{Hint: base.MPublickeyHint, Instance: &base.MPublickey{}}))
		t.NoError(enc.Add(encoder.DecodeDetail{Hint: base.DummyNodeHint, Instance: base.BaseNode{}}))
		t.NoError(enc.Add(encoder.DecodeDetail{Hint: SuffrageCandidateStateValueHint, Instance: SuffrageCandidateStateValue{}}))
		t.NoError(enc.Add(encoder.DecodeDetail{Hint: SuffrageCandidatesStateValueHint, Instance: SuffrageCandidatesStateValue{}}))

		nodes := make([]base.SuffrageCandidateStateValue, 3)
		for i := range nodes {
			nodes[i] = NewSuffrageCandidateStateValue(base.RandomLocalNode(), base.Height(33), base.Height(55))
		}

		stv := NewSuffrageCandidatesStateValue(nodes)

		b, err := util.MarshalJSON(stv)
		t.NoError(err)

		t.T().Log("marshaled:", string(b))

		return stv, b
	}

	t.Decode = func(b []byte) interface{} {
		i, err := enc.Decode(b)
		t.NoError(err)

		u, ok := i.(SuffrageCandidatesStateValue)
		t.True(ok)

		return u
	}
	t.Compare = func(a, b interface{}) {
		av := a.(SuffrageCandidatesStateValue)
		bv := b.(SuffrageCandidatesStateValue)

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
		t.NoError(enc.Add(encoder.DecodeDetail{Hint: base.MPublickeyHint, Instance: &base.MPublickey{}}))
		t.NoError(enc.Add(encoder.DecodeDetail{Hint: base.DummyNodeHint, Instance: base.BaseNode{}}))
		t.NoError(enc.Add(encoder.DecodeDetail{Hint: SuffrageCandidateStateValueHint, Instance: SuffrageCandidateStateValue{}}))

		stv := NewSuffrageCandidateStateValue(base.RandomLocalNode(), base.Height(33), base.Height(55))

		b, err := util.MarshalJSON(stv)
		t.NoError(err)

		t.T().Log("marshaled:", string(b))

		return stv, b
	}

	t.Decode = func(b []byte) interface{} {
		i, err := enc.Decode(b)
		t.NoError(err)

		u, ok := i.(SuffrageCandidateStateValue)
		t.True(ok)

		return u
	}
	t.Compare = func(a, b interface{}) {
		av := a.(SuffrageCandidateStateValue)
		bv := b.(SuffrageCandidateStateValue)

		base.EqualSuffrageCandidateStateValue(t.Assert(), av, bv)
	}

	suite.Run(tt, t)
}
