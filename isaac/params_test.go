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

type testParams struct {
	suite.Suite
}

func (t *testParams) TestNew() {
	networkID := base.RandomNetworkID()

	p := DefaultParams(networkID)
	t.NoError(p.IsValid(networkID))
}

func (t *testParams) TestIsValid() {
	networkID := base.RandomNetworkID()

	t.Run("network id does not match", func() {
		p := DefaultParams(networkID)
		err := p.IsValid(base.RandomNetworkID())
		t.Error(err)
		t.True(errors.Is(err, util.ErrInvalid))
		t.ErrorContains(err, "network id does not match")
	})

	t.Run("wrong network id", func() {
		wrongnetworkID := make([]byte, base.MaxNetworkIDLength+1)

		p := DefaultParams(wrongnetworkID)
		err := p.IsValid(wrongnetworkID)
		t.Error(err)
		t.True(errors.Is(err, util.ErrInvalid))
		t.ErrorContains(err, "network id too long")
	})

	t.Run("wrong threshold", func() {
		p := DefaultParams(networkID)
		p.threshold = 33

		err := p.IsValid(networkID)
		t.Error(err)
		t.True(errors.Is(err, util.ErrInvalid))
		t.ErrorContains(err, "under min threshold")
	})

	t.Run("wrong intervalBroadcastBallot", func() {
		p := DefaultParams(networkID)
		p.intervalBroadcastBallot = -1

		err := p.IsValid(networkID)
		t.Error(err)
		t.True(errors.Is(err, util.ErrInvalid))
		t.ErrorContains(err, "wrong duration")
	})

	t.Run("wrong waitPreparingINITBallot", func() {
		p := DefaultParams(networkID)
		p.waitPreparingINITBallot = -1

		err := p.IsValid(networkID)
		t.Error(err)
		t.True(errors.Is(err, util.ErrInvalid))
		t.ErrorContains(err, "wrong duration")
	})

	t.Run("wrong ballotStuckWait", func() {
		p := DefaultParams(networkID)
		p.ballotStuckWait = -1

		err := p.IsValid(networkID)
		t.Error(err)
		t.True(errors.Is(err, util.ErrInvalid))
		t.ErrorContains(err, "wrong duration")
	})

	t.Run("wrong ballotStuckResolveAfter", func() {
		p := DefaultParams(networkID)
		p.ballotStuckResolveAfter = -1

		err := p.IsValid(networkID)
		t.Error(err)
		t.True(errors.Is(err, util.ErrInvalid))
		t.ErrorContains(err, "wrong duration")
	})
}

func TestParams(t *testing.T) {
	suite.Run(t, new(testParams))
}

func TestParamsJSON(tt *testing.T) {
	t := new(encoder.BaseTestEncode)

	networkID := util.UUID().Bytes()
	enc := jsonenc.NewEncoder()

	t.Encode = func() (interface{}, []byte) {
		t.NoError(enc.Add(encoder.DecodeDetail{Hint: ParamsHint, Instance: Params{}}))

		p := DefaultParams(networkID)
		p.SetThreshold(base.Threshold(77.7))
		p.SetIntervalBroadcastBallot(time.Second * 33)
		p.SetMinWaitNextBlockINITBallot(time.Second * 33)

		b, err := util.MarshalJSON(p)
		t.NoError(err)

		t.T().Log("marshaled:", string(b))

		return p, b
	}

	t.Decode = func(b []byte) interface{} {
		p := NewParams(networkID)
		t.NoError(enc.Unmarshal(b, p))

		return p
	}
	t.Compare = func(a, b interface{}) {
		ap := a.(*Params)
		bp := b.(*Params)

		t.True(ap.Hint().Equal(bp.Hint()))
		t.True(ap.networkID.Equal(bp.networkID))
		t.Equal(ap.threshold, bp.threshold)
		t.Equal(ap.intervalBroadcastBallot, bp.intervalBroadcastBallot)
		t.Equal(ap.waitPreparingINITBallot, bp.waitPreparingINITBallot)
		t.Equal(ap.ballotStuckWait, bp.ballotStuckWait)
		t.Equal(ap.ballotStuckResolveAfter, bp.ballotStuckResolveAfter)
		t.Equal(ap.minWaitNextBlockINITBallot, bp.minWaitNextBlockINITBallot)
	}

	suite.Run(tt, t)
}

func TestParamsJSONMissing(tt *testing.T) {
	t := new(encoder.BaseTestEncode)
	t.SetT(tt)

	enc := jsonenc.NewEncoder()

	t.NoError(enc.Add(encoder.DecodeDetail{Hint: ParamsHint, Instance: Params{}}))

	p := DefaultParams(util.UUID().Bytes())
	p.SetThreshold(p.Threshold() + 3)
	p.SetIntervalBroadcastBallot(p.IntervalBroadcastBallot() + 3)
	p.SetWaitPreparingINITBallot(p.WaitPreparingINITBallot() + 3)
	p.SetBallotStuckWait(p.BallotStuckWait() + 3)
	p.SetBallotStuckResolveAfter(p.BallotStuckResolveAfter() + 3)

	b, err := util.MarshalJSON(p)
	t.NoError(err)

	t.T().Log("marshaled:", string(b))

	var m map[string]interface{}
	t.NoError(util.UnmarshalJSON(b, &m))

	for key := range m {
		if key == "_hint" {
			continue
		}

		nm := map[string]interface{}{}
		for i := range m {
			if i == key {
				continue
			}

			nm[i] = m[i]
		}

		mb, err := util.MarshalJSON(nm)
		t.NoError(err)

		var np *Params
		t.NoError(enc.Unmarshal(mb, &np))

		ub, err := util.MarshalJSON(np)
		t.NoError(err)

		var um map[string]interface{}
		t.NoError(util.UnmarshalJSON(ub, &um))

		for i := range m {
			if i == key {
				t.Empty(um[i])

				continue
			}

			t.Equal(nm[i], um[i], "%s: %v != %v", i, nm[i], um[i])
		}
	}
}
