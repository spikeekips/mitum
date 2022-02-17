package isaac

import (
	"testing"
	"time"

	"github.com/spikeekips/mitum/base"
	"github.com/spikeekips/mitum/util"
	"github.com/spikeekips/mitum/util/encoder"
	jsonenc "github.com/spikeekips/mitum/util/encoder/json"
	"github.com/stretchr/testify/suite"
)

type testPolicyEncode struct {
	encoder.BaseTestEncode
}

func TestPolicyEncode(tt *testing.T) {
	t := new(testPolicyEncode)

	enc := jsonenc.NewEncoder()

	t.Encode = func() (interface{}, []byte) {
		t.NoError(enc.Add(encoder.DecodeDetail{Hint: PolicyHint, Instance: Policy{}}))

		p := NewPolicy()
		p.SetNetworkID(util.UUID().Bytes())
		p.SetThreshold(base.Threshold(77.7))
		p.SetIntervalBroadcastBallot(time.Second * 33)

		b, err := util.MarshalJSON(p)
		t.NoError(err)

		return p, b
	}

	t.Decode = func(b []byte) interface{} {
		i, err := enc.Decode(b)
		t.NoError(err)

		u, ok := i.(Policy)
		t.True(ok)

		return u
	}
	t.Compare = func(a, b interface{}) {
		t.Equal(a, b)
	}

	suite.Run(tt, t)
}
