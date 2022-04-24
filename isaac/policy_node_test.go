package isaac

import (
	"testing"
	"time"

	"github.com/pkg/errors"
	"github.com/spikeekips/mitum/base"
	"github.com/spikeekips/mitum/util"
	"github.com/spikeekips/mitum/util/encoder"
	jsonenc "github.com/spikeekips/mitum/util/encoder/json"
	"github.com/stretchr/testify/suite"
)

type testNodePolicy struct {
	suite.Suite
}

func (t *testNodePolicy) TestNew() {
	networkID := base.RandomNetworkID()

	p := DefaultNodePolicy(networkID)
	t.NoError(p.IsValid(networkID))

	_ = (interface{})(p).(base.NodePolicy)
}

func (t *testNodePolicy) TestIsValid() {
	networkID := base.RandomNetworkID()

	t.Run("network id does not match", func() {
		p := DefaultNodePolicy(networkID)
		err := p.IsValid(base.RandomNetworkID())
		t.Error(err)
		t.True(errors.Is(err, util.InvalidError))
		t.Contains(err.Error(), "network id does not match")
	})

	t.Run("wrong network id", func() {
		wrongnetworkID := make([]byte, base.MaxNetworkIDLength+1)

		p := DefaultNodePolicy(wrongnetworkID)
		err := p.IsValid(wrongnetworkID)
		t.Error(err)
		t.True(errors.Is(err, util.InvalidError))
		t.Contains(err.Error(), "network id too long")
	})

	t.Run("wrong threshold", func() {
		p := DefaultNodePolicy(networkID)
		p.SetThreshold(33)

		err := p.IsValid(networkID)
		t.Error(err)
		t.True(errors.Is(err, util.InvalidError))
		t.Contains(err.Error(), "risky threshold")
	})

	t.Run("wrong intervalBroadcastBallot", func() {
		p := DefaultNodePolicy(networkID)
		p.SetIntervalBroadcastBallot(-1)

		err := p.IsValid(networkID)
		t.Error(err)
		t.True(errors.Is(err, util.InvalidError))
		t.Contains(err.Error(), "wrong duration")
	})

	t.Run("wrong waitProcessingProposal", func() {
		p := DefaultNodePolicy(networkID)
		p.SetWaitProcessingProposal(-1)

		err := p.IsValid(networkID)
		t.Error(err)
		t.True(errors.Is(err, util.InvalidError))
		t.Contains(err.Error(), "wrong duration")
	})

	t.Run("wrong timeoutRequestProposal", func() {
		p := DefaultNodePolicy(networkID)
		p.SetTimeoutRequestProposal(-1)

		err := p.IsValid(networkID)
		t.Error(err)
		t.True(errors.Is(err, util.InvalidError))
		t.Contains(err.Error(), "wrong duration")
	})
}

func TestNodePolicy(t *testing.T) {
	suite.Run(t, new(testNodePolicy))
}

func TestNodePolicyJSON(tt *testing.T) {
	t := new(encoder.BaseTestEncode)

	enc := jsonenc.NewEncoder()

	t.Encode = func() (interface{}, []byte) {
		t.NoError(enc.Add(encoder.DecodeDetail{Hint: NodePolicyHint, Instance: NodePolicy{}}))

		p := DefaultNodePolicy(util.UUID().Bytes())
		p.SetThreshold(base.Threshold(77.7))
		p.SetIntervalBroadcastBallot(time.Second * 33)

		b, err := util.MarshalJSON(&p)
		t.NoError(err)

		return p, b
	}

	t.Decode = func(b []byte) interface{} {
		i, err := enc.Decode(b)
		t.NoError(err)

		u, ok := i.(NodePolicy)
		t.True(ok)

		return u
	}
	t.Compare = func(a, b interface{}) {
		ap := a.(NodePolicy)
		bp := b.(NodePolicy)

		t.True(ap.Hint().Equal(bp.Hint()))
		t.True(ap.networkID.Equal(bp.networkID))
		t.Equal(ap.threshold, bp.threshold)
		t.Equal(ap.intervalBroadcastBallot, bp.intervalBroadcastBallot)
		t.Equal(ap.waitProcessingProposal, bp.waitProcessingProposal)
		t.Equal(ap.timeoutRequestProposal, bp.timeoutRequestProposal)
	}

	suite.Run(tt, t)
}
