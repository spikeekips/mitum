package isaac

import (
	"time"

	"github.com/pkg/errors"
	"github.com/spikeekips/mitum/base"
	"github.com/spikeekips/mitum/util"
	"github.com/spikeekips/mitum/util/hint"
)

var PolicyHint = hint.MustNewHint("isaac-policy-v0.0.1")

type Policy struct {
	base.BasePolicy
	intervalBroadcastBallot time.Duration
}

func NewPolicy() Policy {
	return Policy{
		BasePolicy:              base.NewBasePolicy(PolicyHint),
		intervalBroadcastBallot: time.Second * 3,
	}
}

func (p Policy) IntervalBroadcastBallot() time.Duration {
	return p.intervalBroadcastBallot
}

func (p *Policy) SetIntervalBroadcastBallot(d time.Duration) *Policy {
	p.intervalBroadcastBallot = d

	return p
}

type policyJSONMarshaler struct {
	base.BasePolicyJSONMarshaler
	IB time.Duration `json:"interval_broadcast_ballot"`
}

func (p Policy) MarshalJSON() ([]byte, error) {
	return util.MarshalJSON(policyJSONMarshaler{
		BasePolicyJSONMarshaler: base.BasePolicyJSONMarshaler{
			BaseHinter: p.BaseHinter,
			NetworkID:  p.NetworkID(),
			Threshold:  p.Threshold(),
		},
		IB: p.intervalBroadcastBallot,
	})
}

type policyJSONUnmarshaler struct {
	IB time.Duration `json:"interval_broadcast_ballot"`
}

func (p *Policy) UnmarshalJSON(b []byte) error {
	var ub base.BasePolicy
	if err := util.UnmarshalJSON(b, &ub); err != nil {
		return errors.Wrap(err, "failed to unmarshal BasePolicy")
	}

	var u policyJSONUnmarshaler
	if err := util.UnmarshalJSON(b, &u); err != nil {
		return errors.Wrap(err, "failed to unmarshal Policy")
	}

	p.BasePolicy = ub
	p.intervalBroadcastBallot = u.IB

	return nil
}
