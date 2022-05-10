package isaac

import (
	"testing"

	"github.com/pkg/errors"
	"github.com/spikeekips/mitum/base"
	"github.com/spikeekips/mitum/util"
	"github.com/spikeekips/mitum/util/encoder"
	jsonenc "github.com/spikeekips/mitum/util/encoder/json"
	"github.com/stretchr/testify/suite"
)

type testNetworkPolicy struct {
	suite.Suite
}

func (t *testNetworkPolicy) TestNew() {
	p := DefaultNetworkPolicy()
	t.NoError(p.IsValid(nil))

	_ = (interface{})(p).(base.NetworkPolicy)
}

func (t *testNetworkPolicy) TestIsValid() {
	t.Run("wrong MaxOperationsInProposal", func() {
		p := DefaultNetworkPolicy()
		p.SetMaxOperationsInProposal(0)

		err := p.IsValid(nil)
		t.Error(err)
		t.True(errors.Is(err, util.ErrInvalid))
		t.ErrorContains(err, "under zero maxOperationsInProposal")
	})
}

func TestNetworkPolicy(t *testing.T) {
	suite.Run(t, new(testNetworkPolicy))
}

func TestNetworkPolicyJSON(tt *testing.T) {
	t := new(encoder.BaseTestEncode)

	enc := jsonenc.NewEncoder()

	t.Encode = func() (interface{}, []byte) {
		t.NoError(enc.Add(encoder.DecodeDetail{Hint: NetworkPolicyHint, Instance: NetworkPolicy{}}))

		p := DefaultNetworkPolicy()
		p.SetMaxOperationsInProposal(99)

		b, err := util.MarshalJSON(&p)
		t.NoError(err)

		return p, b
	}

	t.Decode = func(b []byte) interface{} {
		i, err := enc.Decode(b)
		t.NoError(err)

		u, ok := i.(NetworkPolicy)
		t.True(ok)

		return u
	}
	t.Compare = func(a, b interface{}) {
		ap := a.(NetworkPolicy)
		bp := b.(NetworkPolicy)

		t.True(ap.Hint().Equal(bp.Hint()))
		t.Equal(ap.maxOperationsInProposal, bp.maxOperationsInProposal)
	}

	suite.Run(tt, t)
}
