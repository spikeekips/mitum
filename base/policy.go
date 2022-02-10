package base

import "github.com/spikeekips/mitum/util/hint"

var BasePolicyHint = hint.MustNewHint("base-policy-v0.0.1")

type Policy interface {
	hint.Hinter
	NetworkID() NetworkID
	Threshold() Threshold
}

type BasePolicy struct {
	hint.BaseHinter
	networkID NetworkID
	threshold Threshold
}

func NewBasePolicy() BasePolicy {
	return BasePolicy{
		BaseHinter: hint.NewBaseHinter(BasePolicyHint),
	}
}

func (p BasePolicy) NetworkID() NetworkID {
	return p.networkID
}

func (p *BasePolicy) SetNetworkID(n NetworkID) *BasePolicy {
	p.networkID = n

	return p
}

func (p BasePolicy) Threshold() Threshold {
	return p.threshold
}

func (p *BasePolicy) SetThreshold(t Threshold) *BasePolicy {
	p.threshold = t

	return p
}
