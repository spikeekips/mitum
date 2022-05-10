package isaac

import (
	"github.com/pkg/errors"
	"github.com/spikeekips/mitum/base"
	"github.com/spikeekips/mitum/util"
	"github.com/spikeekips/mitum/util/hint"
)

var (
	NetworkPolicyHint           = hint.MustNewHint("network-policy-v0.0.1")
	NetworkPolicyStateValueHint = hint.MustNewHint("network-policy-state-value-v0.0.1")

	DefaultMaxOperationsInProposal uint64 = 333
)

type NetworkPolicy struct {
	util.DefaultJSONMarshaled
	hint.BaseHinter
	maxOperationsInProposal uint64
}

func DefaultNetworkPolicy() NetworkPolicy {
	return NetworkPolicy{
		BaseHinter:              hint.NewBaseHinter(NetworkPolicyHint),
		maxOperationsInProposal: DefaultMaxOperationsInProposal,
	}
}

func (p NetworkPolicy) IsValid([]byte) error {
	e := util.StringErrorFunc("invalid NetworkPolicy")

	if err := p.BaseHinter.IsValid(NetworkPolicyHint.Type().Bytes()); err != nil {
		return e(err, "")
	}

	if p.maxOperationsInProposal < 1 {
		return e(util.ErrInvalid.Errorf("under zero maxOperationsInProposal"), "")
	}

	return nil
}

func (p NetworkPolicy) HashBytes() []byte {
	return util.Uint64ToBytes(p.maxOperationsInProposal)
}

func (p NetworkPolicy) MaxOperationsInProposal() uint64 {
	return p.maxOperationsInProposal
}

func (p *NetworkPolicy) SetMaxOperationsInProposal(i uint64) *NetworkPolicy {
	p.maxOperationsInProposal = i

	return p
}

type networkPolicyJSONMarshaler struct {
	hint.BaseHinter
	MaxOperationsInProposal uint64 `json:"max_operations_in_proposal"`
}

func (p NetworkPolicy) MarshalJSON() ([]byte, error) {
	return util.MarshalJSON(networkPolicyJSONMarshaler{
		BaseHinter:              p.BaseHinter,
		MaxOperationsInProposal: p.maxOperationsInProposal,
	})
}

type networkPolicyJSONUnmarshaler struct {
	MaxOperationsInProposal uint64 `json:"max_operations_in_proposal"`
}

func (p *NetworkPolicy) UnmarshalJSON(b []byte) error {
	var u networkPolicyJSONUnmarshaler
	if err := util.UnmarshalJSON(b, &u); err != nil {
		return errors.Wrap(err, "failed to unmarshal NetworkPolicy")
	}

	p.maxOperationsInProposal = u.MaxOperationsInProposal

	return nil
}

type NetworkPolicyStateValue struct {
	policy base.NetworkPolicy
	hint.BaseHinter
}

func NewNetworkPolicyStateValue(policy base.NetworkPolicy) NetworkPolicyStateValue {
	return NetworkPolicyStateValue{
		BaseHinter: hint.NewBaseHinter(NetworkPolicyStateValueHint),
		policy:     policy,
	}
}

func (s NetworkPolicyStateValue) HashBytes() []byte {
	return s.policy.HashBytes()
}

func (s NetworkPolicyStateValue) IsValid([]byte) error {
	e := util.StringErrorFunc("invalid NetworkPolicyStateValue")
	if err := s.BaseHinter.IsValid(NetworkPolicyStateValueHint.Type().Bytes()); err != nil {
		return e(err, "")
	}

	if err := util.CheckIsValid(nil, false, s.policy); err != nil {
		return e(err, "")
	}

	return nil
}

func (s NetworkPolicyStateValue) Policy() base.NetworkPolicy {
	return s.policy
}
