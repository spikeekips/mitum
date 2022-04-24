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
	util.DefaultJSONMarshaled
	base.BasePolicy
	intervalBroadcastBallot time.Duration
	waitProcessingProposal  time.Duration
	timeoutRequestProposal  time.Duration
}

func DefaultPolicy(networkID base.NetworkID) Policy {
	b := base.NewBasePolicy(PolicyHint)
	b.SetNetworkID(networkID).
		SetThreshold(base.Threshold(100))

	return Policy{
		BasePolicy:              b,
		intervalBroadcastBallot: time.Second * 3,
		waitProcessingProposal:  time.Second * 3,
		timeoutRequestProposal:  time.Second * 3,
	}
}

func (p Policy) IntervalBroadcastBallot() time.Duration {
	return p.intervalBroadcastBallot
}

func (p *Policy) SetIntervalBroadcastBallot(d time.Duration) *Policy {
	p.intervalBroadcastBallot = d

	return p
}

func (p Policy) WaitProcessingProposal() time.Duration {
	return p.waitProcessingProposal
}

func (p *Policy) SetWaitProcessingProposal(d time.Duration) *Policy {
	p.waitProcessingProposal = d

	return p
}

func (p Policy) TimeoutRequestProposal() time.Duration {
	return p.timeoutRequestProposal
}

func (p *Policy) SetTimeoutRequestProposal(d time.Duration) *Policy {
	p.timeoutRequestProposal = d

	return p
}

type policyJSONMarshaler struct {
	base.BasePolicyJSONMarshaler
	IB time.Duration `json:"interval_broadcast_ballot"`
	WP time.Duration `json:"wait_processing_proposal"`
	TP time.Duration `json:"timeout_request_proposal"`
}

func (p Policy) MarshalJSON() ([]byte, error) {
	return util.MarshalJSON(policyJSONMarshaler{
		BasePolicyJSONMarshaler: base.BasePolicyJSONMarshaler{
			BaseHinter: p.BaseHinter,
			NetworkID:  p.NetworkID(),
			Threshold:  p.Threshold(),
		},
		IB: p.intervalBroadcastBallot,
		WP: p.waitProcessingProposal,
		TP: p.timeoutRequestProposal,
	})
}

type policyJSONUnmarshaler struct {
	IB time.Duration `json:"interval_broadcast_ballot"`
	WP time.Duration `json:"wait_processing_proposal"`
	TP time.Duration `json:"timeout_request_proposal"`
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
	p.waitProcessingProposal = u.WP
	p.timeoutRequestProposal = u.TP

	return nil
}
