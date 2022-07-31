package isaac

import (
	"sync"
	"time"

	"github.com/pkg/errors"
	"github.com/spikeekips/mitum/base"
	"github.com/spikeekips/mitum/util"
	"github.com/spikeekips/mitum/util/hint"
)

var NodePolicyHint = hint.MustNewHint("node-policy-v0.0.1")

type NodePolicy struct {
	id        string
	networkID base.NetworkID
	util.DefaultJSONMarshaled
	hint.BaseHinter
	intervalBroadcastBallot               time.Duration
	threshold                             base.Threshold
	waitPreparingINITBallot               time.Duration
	timeoutRequestProposal                time.Duration
	syncSourceCheckerInterval             time.Duration
	validProposalOperationExpire          time.Duration
	validProposalSuffrageOperationsExpire time.Duration
	sync.RWMutex
}

func DefaultNodePolicy(networkID base.NetworkID) *NodePolicy {
	return &NodePolicy{
		id:                                    util.UUID().String(),
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

func (p *NodePolicy) IsValid(networkID []byte) error {
	e := util.ErrInvalid.Errorf("invalid NodePolicy")

	if len(p.id) < 1 {
		return e.Errorf("empty id")
	}

	if err := p.BaseHinter.IsValid(NodePolicyHint.Type().Bytes()); err != nil {
		return e.Wrap(err)
	}

	if !p.networkID.Equal(networkID) {
		return e.Errorf("network id does not match")
	}

	if err := util.CheckIsValid(networkID, false, p.networkID, p.threshold); err != nil {
		return e.Wrap(err)
	}

	if p.intervalBroadcastBallot < 0 {
		return e.Errorf("wrong duration; invalid intervalBroadcastBallot")
	}

	if p.waitPreparingINITBallot < 0 {
		return e.Errorf("wrong duration; invalid waitPreparingINITBallot")
	}

	if p.timeoutRequestProposal < 0 {
		return e.Errorf("wrong duration; invalid timeoutRequestProposal")
	}

	if p.syncSourceCheckerInterval < 0 {
		return e.Errorf("wrong duration; invalid syncSourceCheckerInterval")
	}

	if p.validProposalOperationExpire < 0 {
		return e.Errorf("wrong duration; invalid validProposalOperationExpire")
	}

	if p.validProposalSuffrageOperationsExpire < 0 {
		return e.Errorf("wrong duration; invalid validProposalSuffrageOperationsExpire")
	}

	return nil
}

func (p *NodePolicy) ID() string {
	p.RLock()
	defer p.RUnlock()

	return p.id
}

func (p *NodePolicy) NetworkID() base.NetworkID {
	p.RLock()
	defer p.RUnlock()

	return p.networkID
}

func (p *NodePolicy) SetNetworkID(n base.NetworkID) *NodePolicy {
	p.Lock()
	defer p.Unlock()

	switch {
	case p.networkID == nil, n == nil:
		return p
	default:
		p.networkID = n

		p.id = util.UUID().String()

		return p
	}
}

func (p *NodePolicy) Threshold() base.Threshold {
	p.RLock()
	defer p.RUnlock()

	return p.threshold
}

func (p *NodePolicy) SetThreshold(t base.Threshold) *NodePolicy {
	p.Lock()
	defer p.Unlock()

	if p.threshold == t {
		return p
	}

	p.threshold = t

	p.id = util.UUID().String()

	return p
}

func (p *NodePolicy) IntervalBroadcastBallot() time.Duration {
	p.RLock()
	defer p.RUnlock()

	return p.intervalBroadcastBallot
}

func (p *NodePolicy) SetIntervalBroadcastBallot(d time.Duration) *NodePolicy {
	p.Lock()
	defer p.Unlock()

	if p.intervalBroadcastBallot == d {
		return p
	}

	p.intervalBroadcastBallot = d

	p.id = util.UUID().String()

	return p
}

func (p *NodePolicy) WaitPreparingINITBallot() time.Duration {
	p.RLock()
	defer p.RUnlock()

	return p.waitPreparingINITBallot
}

func (p *NodePolicy) SetWaitPreparingINITBallot(d time.Duration) *NodePolicy {
	p.Lock()
	defer p.Unlock()

	if p.waitPreparingINITBallot == d {
		return p
	}

	p.waitPreparingINITBallot = d

	p.id = util.UUID().String()

	return p
}

func (p *NodePolicy) TimeoutRequestProposal() time.Duration {
	p.RLock()
	defer p.RUnlock()

	return p.timeoutRequestProposal
}

func (p *NodePolicy) SetTimeoutRequestProposal(d time.Duration) *NodePolicy {
	p.Lock()
	defer p.Unlock()

	if p.timeoutRequestProposal == d {
		return p
	}

	p.timeoutRequestProposal = d

	p.id = util.UUID().String()

	return p
}

func (p *NodePolicy) SyncSourceCheckerInterval() time.Duration {
	p.RLock()
	defer p.RUnlock()

	return p.syncSourceCheckerInterval
}

func (p *NodePolicy) SetSyncSourceCheckerInterval(d time.Duration) *NodePolicy {
	p.Lock()
	defer p.Unlock()

	if p.syncSourceCheckerInterval == d {
		return p
	}

	p.syncSourceCheckerInterval = d

	p.id = util.UUID().String()

	return p
}

func (p *NodePolicy) ValidProposalOperationExpire() time.Duration {
	p.RLock()
	defer p.RUnlock()

	return p.validProposalOperationExpire
}

func (p *NodePolicy) SetValidProposalOperationExpire(d time.Duration) *NodePolicy {
	p.Lock()
	defer p.Unlock()

	if p.validProposalOperationExpire == d {
		return p
	}

	p.validProposalOperationExpire = d

	p.id = util.UUID().String()

	return p
}

func (p *NodePolicy) ValidProposalSuffrageOperationsExpire() time.Duration {
	p.RLock()
	defer p.RUnlock()

	return p.validProposalSuffrageOperationsExpire
}

func (p *NodePolicy) SetValidProposalSuffrageOperationsExpire(d time.Duration) *NodePolicy {
	p.Lock()
	defer p.Unlock()

	if p.validProposalSuffrageOperationsExpire == d {
		return p
	}

	p.validProposalSuffrageOperationsExpire = d

	p.id = util.UUID().String()

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

func (p *NodePolicy) MarshalJSON() ([]byte, error) {
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

func (p *NodePolicy) UnmarshalJSON(b []byte) error {
	var u nodePolicyJSONUnmarshaler
	if err := util.UnmarshalJSON(b, &u); err != nil {
		return errors.Wrap(err, "failed to unmarshal NodePolicy")
	}

	p.BaseHinter = u.BaseHinter
	p.id = util.UUID().String()
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
