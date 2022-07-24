package isaac

import (
	"time"

	"github.com/pkg/errors"
	"github.com/spikeekips/mitum/base"
	"github.com/spikeekips/mitum/util"
	"github.com/spikeekips/mitum/util/hint"
)

var NodePolicyHint = hint.MustNewHint("node-policy-v0.0.1")

type NodePolicy struct {
	networkID base.NetworkID
	util.DefaultJSONMarshaled
	hint.BaseHinter
	threshold                             base.Threshold
	intervalBroadcastBallot               time.Duration
	waitPreparingINITBallot               time.Duration
	timeoutRequestProposal                time.Duration
	syncSourceCheckerInterval             time.Duration
	validProposalOperationExpire          time.Duration
	validProposalSuffrageOperationsExpire time.Duration
}

func DefaultNodePolicy(networkID base.NetworkID) NodePolicy {
	return NodePolicy{
		BaseHinter:                            hint.NewBaseHinter(NodePolicyHint),
		networkID:                             networkID,
		threshold:                             base.DefaultThreshold,
		intervalBroadcastBallot:               time.Second * 3,  //nolint:gomnd //...
		waitPreparingINITBallot:               time.Second * 5,  //nolint:gomnd //...
		timeoutRequestProposal:                time.Second * 3,  //nolint:gomnd //...
		syncSourceCheckerInterval:             time.Second * 30, //nolint:gomnd //...
		validProposalOperationExpire:          time.Hour * 24,   //nolint:gomnd //...
		validProposalSuffrageOperationsExpire: time.Hour * 2,    //nolint:gomnd //...
	}
}

func (p NodePolicy) IsValid(networkID []byte) error {
	e := util.StringErrorFunc("invalid NodePolicy")

	if err := p.BaseHinter.IsValid(NodePolicyHint.Type().Bytes()); err != nil {
		return e(err, "")
	}

	if !p.networkID.Equal(networkID) {
		return e(util.ErrInvalid.Errorf("network id does not match"), "")
	}

	if err := util.CheckIsValid(networkID, false, p.networkID, p.threshold); err != nil {
		return e(err, "")
	}

	if p.intervalBroadcastBallot < 0 {
		return e(util.ErrInvalid.Errorf("wrong duration"), "invalid intervalBroadcastBallot")
	}

	if p.waitPreparingINITBallot < 0 {
		return e(util.ErrInvalid.Errorf("wrong duration"), "invalid waitPreparingINITBallot")
	}

	if p.timeoutRequestProposal < 0 {
		return e(util.ErrInvalid.Errorf("wrong duration"), "invalid timeoutRequestProposal")
	}

	if p.syncSourceCheckerInterval < 0 {
		return e(util.ErrInvalid.Errorf("wrong duration"), "invalid syncSourceCheckerInterval")
	}

	if p.validProposalOperationExpire < 0 {
		return e(util.ErrInvalid.Errorf("wrong duration"), "invalid validProposalOperationExpire")
	}

	if p.validProposalSuffrageOperationsExpire < 0 {
		return e(util.ErrInvalid.Errorf("wrong duration"), "invalid validProposalSuffrageOperationsExpire")
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

func (p NodePolicy) WaitPreparingINITBallot() time.Duration {
	return p.waitPreparingINITBallot
}

func (p *NodePolicy) SetWaitPreparingINITBallot(d time.Duration) *NodePolicy {
	p.waitPreparingINITBallot = d

	return p
}

func (p NodePolicy) TimeoutRequestProposal() time.Duration {
	return p.timeoutRequestProposal
}

func (p *NodePolicy) SetTimeoutRequestProposal(d time.Duration) *NodePolicy {
	p.timeoutRequestProposal = d

	return p
}

func (p NodePolicy) SyncSourceCheckerInterval() time.Duration {
	return p.syncSourceCheckerInterval
}

func (p *NodePolicy) SetSyncSourceCheckerInterval(d time.Duration) *NodePolicy {
	p.syncSourceCheckerInterval = d

	return p
}

func (p NodePolicy) ValidProposalOperationExpire() time.Duration {
	return p.validProposalOperationExpire
}

func (p *NodePolicy) SetValidProposalOperationExpire(d time.Duration) *NodePolicy {
	p.validProposalOperationExpire = d

	return p
}

func (p NodePolicy) ValidProposalSuffrageOperationsExpire() time.Duration {
	return p.validProposalSuffrageOperationsExpire
}

func (p *NodePolicy) SetValidProposalSuffrageOperationsExpire(d time.Duration) *NodePolicy {
	p.validProposalSuffrageOperationsExpire = d

	return p
}

type nodePolicyJSONMarshaler struct {
	NetworkID base.NetworkID `json:"network_id"`
	hint.BaseHinter
	Threshold                             base.Threshold `json:"threshold"`
	IntervalBroadcastBallot               time.Duration  `json:"interval_broadcast_ballot"`
	WaitPreparingINITBallot               time.Duration  `json:"wait_preparing_init_ballot"`
	TimeoutRequestProposal                time.Duration  `json:"timeout_request_proposal"`
	SyncSourceCheckerInterval             time.Duration  `json:"sync_source_checker_interval"`
	ValidProposalOperationExpire          time.Duration  `json:"valid_proposal_operation_expire"`
	ValidProposalSuffrageOperationsExpire time.Duration  `json:"valid_proposal_suffrage_operations_expire"`
}

func (p NodePolicy) MarshalJSON() ([]byte, error) {
	return util.MarshalJSON(nodePolicyJSONMarshaler{
		BaseHinter:                            p.BaseHinter,
		NetworkID:                             p.networkID,
		Threshold:                             p.threshold,
		IntervalBroadcastBallot:               p.intervalBroadcastBallot,
		WaitPreparingINITBallot:               p.waitPreparingINITBallot,
		TimeoutRequestProposal:                p.timeoutRequestProposal,
		SyncSourceCheckerInterval:             p.syncSourceCheckerInterval,
		ValidProposalOperationExpire:          p.validProposalOperationExpire,
		ValidProposalSuffrageOperationsExpire: p.validProposalSuffrageOperationsExpire,
	})
}

type nodePolicyJSONUnmarshaler struct {
	NetworkID                             base.NetworkID `json:"network_id"`
	Threshold                             base.Threshold `json:"threshold"`
	IntervalBroadcastBallot               time.Duration  `json:"interval_broadcast_ballot"`
	WaitPreparingINITBallot               time.Duration  `json:"wait_preparing_init_ballot"`
	TimeoutRequestProposal                time.Duration  `json:"timeout_request_proposal"`
	SyncSourceCheckerInterval             time.Duration  `json:"sync_source_checker_interval"`
	ValidProposalOperationExpire          time.Duration  `json:"valid_proposal_operation_expire"`
	ValidProposalSuffrageOperationsExpire time.Duration  `json:"valid_proposal_suffrage_operations_expire"`
}

func (p *NodePolicy) UnmarshalJSON(b []byte) error {
	var u nodePolicyJSONUnmarshaler
	if err := util.UnmarshalJSON(b, &u); err != nil {
		return errors.Wrap(err, "failed to unmarshal NodePolicy")
	}

	p.networkID = u.NetworkID
	p.threshold = u.Threshold
	p.intervalBroadcastBallot = u.IntervalBroadcastBallot
	p.waitPreparingINITBallot = u.WaitPreparingINITBallot
	p.timeoutRequestProposal = u.TimeoutRequestProposal
	p.syncSourceCheckerInterval = u.SyncSourceCheckerInterval
	p.validProposalOperationExpire = u.ValidProposalOperationExpire
	p.validProposalSuffrageOperationsExpire = u.ValidProposalSuffrageOperationsExpire

	return nil
}
