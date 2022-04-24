package isaac

import (
	"time"

	"github.com/pkg/errors"
	"github.com/spikeekips/mitum/base"
	"github.com/spikeekips/mitum/util"
	"github.com/spikeekips/mitum/util/hint"
)

var NodePolicyHint = hint.MustNewHint("isaac-node-policy-v0.0.1")

type NodePolicy struct {
	util.DefaultJSONMarshaled
	hint.BaseHinter
	networkID               base.NetworkID
	threshold               base.Threshold
	intervalBroadcastBallot time.Duration
	waitProcessingProposal  time.Duration
	timeoutRequestProposal  time.Duration
}

func DefaultNodePolicy(networkID base.NetworkID) NodePolicy {
	return NodePolicy{
		BaseHinter:              hint.NewBaseHinter(NodePolicyHint),
		networkID:               networkID,
		threshold:               base.Threshold(100),
		intervalBroadcastBallot: time.Second * 3,
		waitProcessingProposal:  time.Second * 3,
		timeoutRequestProposal:  time.Second * 3,
	}
}

func (p NodePolicy) IsValid(networkID []byte) error {
	e := util.StringErrorFunc("invalid NodePolicy")

	if !p.networkID.Equal(networkID) {
		return e(util.InvalidError.Errorf("network id does not match"), "")
	}
	if err := util.CheckIsValid(networkID, false, p.networkID, p.threshold); err != nil {
		return e(err, "")
	}
	if p.intervalBroadcastBallot < 0 {
		return e(util.InvalidError.Errorf("wrong duration"), "invalid intervalBroadcastBallot")
	}
	if p.waitProcessingProposal < 0 {
		return e(util.InvalidError.Errorf("wrong duration"), "invalid waitProcessingProposal")
	}
	if p.timeoutRequestProposal < 0 {
		return e(util.InvalidError.Errorf("wrong duration"), "invalid timeoutRequestProposal")
	}

	return nil
}

func (p NodePolicy) NetworkID() base.NetworkID {
	return p.networkID
}

func (p *NodePolicy) SetNetworkID(n base.NetworkID) *NodePolicy {
	p.networkID = n

	return p
}

func (p NodePolicy) Threshold() base.Threshold {
	return p.threshold
}

func (p *NodePolicy) SetThreshold(t base.Threshold) *NodePolicy {
	p.threshold = t

	return p
}

func (p NodePolicy) IntervalBroadcastBallot() time.Duration {
	return p.intervalBroadcastBallot
}

func (p *NodePolicy) SetIntervalBroadcastBallot(d time.Duration) *NodePolicy {
	p.intervalBroadcastBallot = d

	return p
}

func (p NodePolicy) WaitProcessingProposal() time.Duration {
	return p.waitProcessingProposal
}

func (p *NodePolicy) SetWaitProcessingProposal(d time.Duration) *NodePolicy {
	p.waitProcessingProposal = d

	return p
}

func (p NodePolicy) TimeoutRequestProposal() time.Duration {
	return p.timeoutRequestProposal
}

func (p *NodePolicy) SetTimeoutRequestProposal(d time.Duration) *NodePolicy {
	p.timeoutRequestProposal = d

	return p
}

type policyJSONMarshaler struct {
	hint.BaseHinter
	NT base.NetworkID `json:"network_id"`
	TH base.Threshold `json:"threshold"`
	IB time.Duration  `json:"interval_broadcast_ballot"`
	WP time.Duration  `json:"wait_processing_proposal"`
	TP time.Duration  `json:"timeout_request_proposal"`
}

func (p NodePolicy) MarshalJSON() ([]byte, error) {
	return util.MarshalJSON(policyJSONMarshaler{
		BaseHinter: p.BaseHinter,
		NT:         p.networkID,
		TH:         p.threshold,
		IB:         p.intervalBroadcastBallot,
		WP:         p.waitProcessingProposal,
		TP:         p.timeoutRequestProposal,
	})
}

type policyJSONUnmarshaler struct {
	NT base.NetworkID `json:"network_id"`
	TH base.Threshold `json:"threshold"`
	IB time.Duration  `json:"interval_broadcast_ballot"`
	WP time.Duration  `json:"wait_processing_proposal"`
	TP time.Duration  `json:"timeout_request_proposal"`
}

func (p *NodePolicy) UnmarshalJSON(b []byte) error {
	var u policyJSONUnmarshaler
	if err := util.UnmarshalJSON(b, &u); err != nil {
		return errors.Wrap(err, "failed to unmarshal NodePolicy")
	}

	p.networkID = u.NT
	p.threshold = u.TH
	p.intervalBroadcastBallot = u.IB
	p.waitProcessingProposal = u.WP
	p.timeoutRequestProposal = u.TP

	return nil
}
