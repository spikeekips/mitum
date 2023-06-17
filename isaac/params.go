package isaac

import (
	"reflect"
	"time"

	"github.com/pkg/errors"
	"github.com/spikeekips/mitum/base"
	"github.com/spikeekips/mitum/util"
	"github.com/spikeekips/mitum/util/hint"
)

var ParamsHint = hint.MustNewHint("isaac-params-v0.0.1")

var (
	DefaultntervalBroadcastBallot  = time.Second * 3
	DefaultWaitPreparingINITBallot = time.Second * 5
	DefaultWaitStuckInterval       = time.Second * 33
	DefaultTimeoutRequest          = time.Second * 3
)

type Params struct {
	*util.BaseParams
	networkID base.NetworkID
	hint.BaseHinter
	threshold                     base.Threshold
	intervalBroadcastBallot       time.Duration
	waitPreparingINITBallot       time.Duration
	ballotStuckWait               time.Duration
	ballotStuckResolveAfter       time.Duration
	maxTryHandoverYBrokerSyncData uint64
}

func NewParams(networkID base.NetworkID) *Params {
	return &Params{
		BaseParams: util.NewBaseParams(),
		BaseHinter: hint.NewBaseHinter(ParamsHint),
		networkID:  networkID,
	}
}

func DefaultParams(networkID base.NetworkID) *Params {
	return &Params{
		BaseParams:                    util.NewBaseParams(),
		BaseHinter:                    hint.NewBaseHinter(ParamsHint),
		networkID:                     networkID,
		threshold:                     base.DefaultThreshold,
		intervalBroadcastBallot:       DefaultntervalBroadcastBallot,
		waitPreparingINITBallot:       DefaultWaitPreparingINITBallot,
		ballotStuckWait:               time.Second * 33, //nolint:gomnd // waitPreparingINITBallot * 10
		ballotStuckResolveAfter:       time.Second * 66, //nolint:gomnd // ballotStuckWait * 2
		maxTryHandoverYBrokerSyncData: 33,               //nolint:gomnd //...
	}
}

func (p *Params) IsValid(networkID []byte) error {
	e := util.ErrInvalid.Errorf("invalid Params")

	if err := p.BaseParams.IsValid(nil); err != nil {
		return e.Wrap(err)
	}

	if err := p.BaseHinter.IsValid(ParamsHint.Type().Bytes()); err != nil {
		return e.Wrap(err)
	}

	if !p.networkID.Equal(networkID) {
		return e.Errorf("network id does not match")
	}

	if err := util.CheckIsValiders(networkID, false, p.networkID, p.threshold); err != nil {
		return e.Wrap(err)
	}

	if p.intervalBroadcastBallot < 0 {
		return e.Errorf("wrong duration; invalid intervalBroadcastBallot")
	}

	if p.waitPreparingINITBallot < 0 {
		return e.Errorf("wrong duration; invalid waitPreparingINITBallot")
	}

	if p.ballotStuckWait < 0 {
		return e.Errorf("wrong duration; invalid ballotStuckWait")
	}

	if p.ballotStuckResolveAfter < 0 {
		return e.Errorf("wrong duration; invalid ballotStuckResolveAfter")
	}

	return nil
}

func (p *Params) NetworkID() base.NetworkID {
	p.RLock()
	defer p.RUnlock()

	return p.networkID
}

func (p *Params) SetNetworkID(n base.NetworkID) error {
	return p.Set(func() (bool, error) {
		if n == nil {
			return false, errors.Errorf("empty network id")
		}

		p.networkID = n

		return true, nil
	})
}

func (p *Params) Threshold() base.Threshold {
	p.RLock()
	defer p.RUnlock()

	return p.threshold
}

func (p *Params) SetThreshold(t base.Threshold) error {
	return p.Set(func() (bool, error) {
		switch {
		case t < 1:
			return false, errors.Errorf("under zero")
		case p.threshold == t:
			return false, nil
		default:
			p.threshold = t

			return true, nil
		}
	})
}

func (p *Params) IntervalBroadcastBallot() time.Duration {
	p.RLock()
	defer p.RUnlock()

	return p.intervalBroadcastBallot
}

func (p *Params) SetIntervalBroadcastBallot(d time.Duration) error {
	return p.SetDuration(d, func(d time.Duration) (bool, error) {
		if p.intervalBroadcastBallot == d {
			return false, nil
		}

		p.intervalBroadcastBallot = d

		return true, nil
	})
}

func (p *Params) WaitPreparingINITBallot() time.Duration {
	p.RLock()
	defer p.RUnlock()

	return p.waitPreparingINITBallot
}

func (p *Params) SetWaitPreparingINITBallot(d time.Duration) error {
	return p.SetDuration(d, func(d time.Duration) (bool, error) {
		if p.waitPreparingINITBallot == d {
			return false, nil
		}

		p.waitPreparingINITBallot = d

		return true, nil
	})
}

func (p *Params) BallotStuckWait() time.Duration {
	p.RLock()
	defer p.RUnlock()

	return p.ballotStuckWait
}

func (p *Params) SetBallotStuckWait(d time.Duration) error {
	return p.SetDuration(d, func(d time.Duration) (bool, error) {
		if p.ballotStuckWait == d {
			return false, nil
		}

		p.ballotStuckWait = d

		return true, nil
	})
}

func (p *Params) BallotStuckResolveAfter() time.Duration {
	p.RLock()
	defer p.RUnlock()

	return p.ballotStuckResolveAfter
}

func (p *Params) SetBallotStuckResolveAfter(d time.Duration) error {
	return p.SetDuration(d, func(d time.Duration) (bool, error) {
		if p.ballotStuckResolveAfter == d {
			return false, nil
		}

		p.ballotStuckResolveAfter = d

		return true, nil
	})
}

func (p *Params) MaxTryHandoverYBrokerSyncData() uint64 {
	p.RLock()
	defer p.RUnlock()

	return p.maxTryHandoverYBrokerSyncData
}

func (p *Params) SetMaxTryHandoverYBrokerSyncData(d uint64) error {
	return p.SetUint64(d, func(d uint64) (bool, error) {
		if p.maxTryHandoverYBrokerSyncData == d {
			return false, nil
		}

		p.maxTryHandoverYBrokerSyncData = d

		return true, nil
	})
}

type paramsJSONMarshaler struct {
	//revive:disable:line-length-limit
	hint.BaseHinter
	Threshold                     base.Threshold        `json:"threshold,omitempty"`
	IntervalBroadcastBallot       util.ReadableDuration `json:"interval_broadcast_ballot,omitempty"`
	WaitPreparingINITBallot       util.ReadableDuration `json:"wait_preparing_init_ballot,omitempty"`
	BallotStuckWait               util.ReadableDuration `json:"ballot_stuck_wait,omitempty"`
	BallotStuckResolveAfter       util.ReadableDuration `json:"ballot_stuck_resolve_after,omitempty"`
	MaxTryHandoverYBrokerSyncData uint64                `json:"max_try_handover_y_broker_sync_data,omitempty"`
	//revive:enable:line-length-limit
}

func (p *Params) MarshalJSON() ([]byte, error) {
	return util.MarshalJSON(paramsJSONMarshaler{
		BaseHinter:                    p.BaseHinter,
		Threshold:                     p.threshold,
		IntervalBroadcastBallot:       util.ReadableDuration(p.intervalBroadcastBallot),
		WaitPreparingINITBallot:       util.ReadableDuration(p.waitPreparingINITBallot),
		BallotStuckResolveAfter:       util.ReadableDuration(p.ballotStuckResolveAfter),
		BallotStuckWait:               util.ReadableDuration(p.ballotStuckWait),
		MaxTryHandoverYBrokerSyncData: p.maxTryHandoverYBrokerSyncData,
	})
}

type paramsJSONUnmarshaler struct {
	//revive:disable:line-length-limit
	Threshold                     interface{}            `json:"threshold"`
	IntervalBroadcastBallot       *util.ReadableDuration `json:"interval_broadcast_ballot"`
	WaitPreparingINITBallot       *util.ReadableDuration `json:"wait_preparing_init_ballot"`
	BallotStuckWait               *util.ReadableDuration `json:"ballot_stuck_wait,omitempty"`
	BallotStuckResolveAfter       *util.ReadableDuration `json:"ballot_stuck_resolve_after,omitempty"`
	MaxTryHandoverYBrokerSyncData *uint64                `json:"max_try_handover_y_broker_sync_data,omitempty"`
	hint.BaseHinter
	//revive:enable:line-length-limit
}

func (p *Params) UnmarshalJSON(b []byte) error {
	var u paramsJSONUnmarshaler
	if err := util.UnmarshalJSON(b, &u); err != nil {
		return errors.Wrap(err, "unmarshal Params")
	}

	p.BaseParams = util.NewBaseParams()
	p.BaseHinter = u.BaseHinter

	args := [][2]interface{}{
		{u.MaxTryHandoverYBrokerSyncData, &p.maxTryHandoverYBrokerSyncData},
	}

	for i := range args {
		if reflect.ValueOf(args[i][0]).IsZero() {
			continue
		}

		if err := util.InterfaceSetValue(reflect.ValueOf(args[i][0]).Elem().Interface(), args[i][1]); err != nil {
			return err
		}
	}

	durargs := [][2]interface{}{
		{u.IntervalBroadcastBallot, &p.intervalBroadcastBallot},
		{u.WaitPreparingINITBallot, &p.waitPreparingINITBallot},
		{u.BallotStuckResolveAfter, &p.ballotStuckResolveAfter},
		{u.BallotStuckWait, &p.ballotStuckWait},
	}

	for i := range durargs {
		v := durargs[i][0].(*util.ReadableDuration) //nolint:forcetypeassert //...

		if reflect.ValueOf(v).IsZero() {
			continue
		}

		if err := util.InterfaceSetValue(time.Duration(*v), durargs[i][1]); err != nil {
			return err
		}
	}

	switch t := u.Threshold.(type) {
	case string:
		if err := p.threshold.UnmarshalText([]byte(t)); err != nil {
			return err
		}
	case float64:
		p.threshold = base.Threshold(t)
	case int64:
		p.threshold = base.Threshold(float64(t))
	}

	return nil
}