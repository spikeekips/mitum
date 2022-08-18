package isaac

import (
	"reflect"
	"sync"
	"time"

	"github.com/pkg/errors"
	"github.com/spikeekips/mitum/base"
	"github.com/spikeekips/mitum/util"
	"github.com/spikeekips/mitum/util/hint"
)

var LocalParamsHint = hint.MustNewHint("local-params-v0.0.1")

type LocalParams struct {
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
	maxOperationSize                      uint64
	sameMemberLimit                       uint64
	sync.RWMutex
}

func DefaultLocalParams(networkID base.NetworkID) *LocalParams {
	return &LocalParams{
		id:                                    util.UUID().String(),
		BaseHinter:                            hint.NewBaseHinter(LocalParamsHint),
		networkID:                             networkID,
		threshold:                             base.DefaultThreshold,
		intervalBroadcastBallot:               time.Second * 3,  //nolint:gomnd //...
		waitPreparingINITBallot:               time.Second * 5,  //nolint:gomnd //...
		timeoutRequestProposal:                time.Second * 3,  //nolint:gomnd //...
		syncSourceCheckerInterval:             time.Second * 30, //nolint:gomnd //...
		validProposalOperationExpire:          time.Hour * 24,   //nolint:gomnd //...
		validProposalSuffrageOperationsExpire: time.Hour * 2,    //nolint:gomnd //...
		maxOperationSize:                      1 << 10,          //nolint:gomnd //...
		sameMemberLimit:                       3,                //nolint:gomnd //...
	}
}

func (p *LocalParams) IsValid(networkID []byte) error {
	e := util.ErrInvalid.Errorf("invalid LocalParams")

	if len(p.id) < 1 {
		return e.Errorf("empty id")
	}

	if err := p.BaseHinter.IsValid(LocalParamsHint.Type().Bytes()); err != nil {
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

	if p.maxOperationSize < 1 {
		return e.Errorf("wrong maxOperationSize")
	}

	return nil
}

func (p *LocalParams) ID() string {
	p.RLock()
	defer p.RUnlock()

	return p.id
}

func (p *LocalParams) NetworkID() base.NetworkID {
	p.RLock()
	defer p.RUnlock()

	return p.networkID
}

func (p *LocalParams) SetNetworkID(n base.NetworkID) *LocalParams {
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

func (p *LocalParams) Threshold() base.Threshold {
	p.RLock()
	defer p.RUnlock()

	return p.threshold
}

func (p *LocalParams) SetThreshold(t base.Threshold) *LocalParams {
	p.Lock()
	defer p.Unlock()

	if p.threshold == t {
		return p
	}

	p.threshold = t

	p.id = util.UUID().String()

	return p
}

func (p *LocalParams) IntervalBroadcastBallot() time.Duration {
	p.RLock()
	defer p.RUnlock()

	return p.intervalBroadcastBallot
}

func (p *LocalParams) SetIntervalBroadcastBallot(d time.Duration) *LocalParams {
	p.Lock()
	defer p.Unlock()

	if p.intervalBroadcastBallot == d {
		return p
	}

	p.intervalBroadcastBallot = d

	p.id = util.UUID().String()

	return p
}

func (p *LocalParams) WaitPreparingINITBallot() time.Duration {
	p.RLock()
	defer p.RUnlock()

	return p.waitPreparingINITBallot
}

func (p *LocalParams) SetWaitPreparingINITBallot(d time.Duration) *LocalParams {
	p.Lock()
	defer p.Unlock()

	if p.waitPreparingINITBallot == d {
		return p
	}

	p.waitPreparingINITBallot = d

	p.id = util.UUID().String()

	return p
}

func (p *LocalParams) TimeoutRequestProposal() time.Duration {
	p.RLock()
	defer p.RUnlock()

	return p.timeoutRequestProposal
}

func (p *LocalParams) SetTimeoutRequestProposal(d time.Duration) *LocalParams {
	p.Lock()
	defer p.Unlock()

	if p.timeoutRequestProposal == d {
		return p
	}

	p.timeoutRequestProposal = d

	p.id = util.UUID().String()

	return p
}

func (p *LocalParams) SyncSourceCheckerInterval() time.Duration {
	p.RLock()
	defer p.RUnlock()

	return p.syncSourceCheckerInterval
}

func (p *LocalParams) SetSyncSourceCheckerInterval(d time.Duration) *LocalParams {
	p.Lock()
	defer p.Unlock()

	if p.syncSourceCheckerInterval == d {
		return p
	}

	p.syncSourceCheckerInterval = d

	p.id = util.UUID().String()

	return p
}

func (p *LocalParams) ValidProposalOperationExpire() time.Duration {
	p.RLock()
	defer p.RUnlock()

	return p.validProposalOperationExpire
}

func (p *LocalParams) SetValidProposalOperationExpire(d time.Duration) *LocalParams {
	p.Lock()
	defer p.Unlock()

	if p.validProposalOperationExpire == d {
		return p
	}

	p.validProposalOperationExpire = d

	p.id = util.UUID().String()

	return p
}

func (p *LocalParams) ValidProposalSuffrageOperationsExpire() time.Duration {
	p.RLock()
	defer p.RUnlock()

	return p.validProposalSuffrageOperationsExpire
}

func (p *LocalParams) SetValidProposalSuffrageOperationsExpire(d time.Duration) *LocalParams {
	p.Lock()
	defer p.Unlock()

	if p.validProposalSuffrageOperationsExpire == d {
		return p
	}

	p.validProposalSuffrageOperationsExpire = d

	p.id = util.UUID().String()

	return p
}

func (p *LocalParams) MaxOperationSize() uint64 {
	p.RLock()
	defer p.RUnlock()

	return p.maxOperationSize
}

func (p *LocalParams) SetMaxOperationSize(s uint64) *LocalParams {
	p.Lock()
	defer p.Unlock()

	if p.maxOperationSize == s {
		return p
	}

	p.maxOperationSize = s

	p.id = util.UUID().String()

	return p
}

func (p *LocalParams) SameMemberLimit() uint64 {
	p.RLock()
	defer p.RUnlock()

	return p.sameMemberLimit
}

func (p *LocalParams) SetSameMemberLimit(s uint64) *LocalParams {
	p.Lock()
	defer p.Unlock()

	if p.sameMemberLimit == s {
		return p
	}

	p.sameMemberLimit = s

	p.id = util.UUID().String()

	return p
}

type localParamsJSONMarshaler struct {
	NetworkID base.NetworkID `json:"network_id,omitempty"`
	hint.BaseHinter
	Threshold                             base.Threshold `json:"threshold,omitempty"`
	IntervalBroadcastBallot               time.Duration  `json:"interval_broadcast_ballot,omitempty"`
	WaitPreparingINITBallot               time.Duration  `json:"wait_preparing_init_ballot,omitempty"`
	TimeoutRequestProposal                time.Duration  `json:"timeout_request_proposal,omitempty"`
	SyncSourceCheckerInterval             time.Duration  `json:"sync_source_checker_interval,omitempty"`
	ValidProposalOperationExpire          time.Duration  `json:"valid_proposal_operation_expire,omitempty"`
	ValidProposalSuffrageOperationsExpire time.Duration  `json:"valid_proposal_suffrage_operations_expire,omitempty"`
	MaxOperationSize                      uint64         `json:"max_operation_size,omitempty"`
	SameMemberLimit                       uint64         `json:"same_member_limit,omitempty"`
}

func (p *LocalParams) MarshalJSON() ([]byte, error) {
	return util.MarshalJSON(localParamsJSONMarshaler{
		BaseHinter:                            p.BaseHinter,
		NetworkID:                             p.networkID,
		Threshold:                             p.threshold,
		IntervalBroadcastBallot:               p.intervalBroadcastBallot,
		WaitPreparingINITBallot:               p.waitPreparingINITBallot,
		TimeoutRequestProposal:                p.timeoutRequestProposal,
		SyncSourceCheckerInterval:             p.syncSourceCheckerInterval,
		ValidProposalOperationExpire:          p.validProposalOperationExpire,
		ValidProposalSuffrageOperationsExpire: p.validProposalSuffrageOperationsExpire,
		MaxOperationSize:                      p.maxOperationSize,
		SameMemberLimit:                       p.sameMemberLimit,
	})
}

type localParamsJSONUnmarshaler struct {
	NetworkID                             *base.NetworkID `json:"network_id"`
	Threshold                             *base.Threshold `json:"threshold"`
	IntervalBroadcastBallot               *time.Duration  `json:"interval_broadcast_ballot"`
	WaitPreparingINITBallot               *time.Duration  `json:"wait_preparing_init_ballot"`
	TimeoutRequestProposal                *time.Duration  `json:"timeout_request_proposal"`
	SyncSourceCheckerInterval             *time.Duration  `json:"sync_source_checker_interval"`
	ValidProposalOperationExpire          *time.Duration  `json:"valid_proposal_operation_expire"`
	ValidProposalSuffrageOperationsExpire *time.Duration  `json:"valid_proposal_suffrage_operations_expire"`
	MaxOperationSize                      *uint64         `json:"max_operation_size"`
	SameMemberLimit                       *uint64         `json:"same_member_limit"`
	hint.BaseHinter
}

func (p *LocalParams) UnmarshalJSON(b []byte) error {
	var u localParamsJSONUnmarshaler
	if err := util.UnmarshalJSON(b, &u); err != nil {
		return errors.Wrap(err, "failed to unmarshal LocalParams")
	}

	set := func(v, target interface{}) error {
		if reflect.ValueOf(v).IsZero() {
			return nil
		}

		return util.InterfaceSetValue(reflect.ValueOf(v).Elem().Interface(), target)
	}

	p.BaseHinter = u.BaseHinter

	args := [][2]interface{}{
		{u.NetworkID, &p.networkID},
		{u.Threshold, &p.threshold},
		{u.IntervalBroadcastBallot, &p.intervalBroadcastBallot},
		{u.WaitPreparingINITBallot, &p.waitPreparingINITBallot},
		{u.TimeoutRequestProposal, &p.timeoutRequestProposal},
		{u.SyncSourceCheckerInterval, &p.syncSourceCheckerInterval},
		{u.ValidProposalOperationExpire, &p.validProposalOperationExpire},
		{u.ValidProposalSuffrageOperationsExpire, &p.validProposalSuffrageOperationsExpire},
		{u.MaxOperationSize, &p.maxOperationSize},
		{u.SameMemberLimit, &p.sameMemberLimit},
	}

	for i := range args {
		if err := set(args[i][0], args[i][1]); err != nil {
			return err
		}
	}

	p.id = util.UUID().String()

	return nil
}
