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

	// NOTE suffrage candidate can be approved within lifespan height; almost 15
	// days(based on 5 second for one block)
	DefaultSuffrageCandidateLifespan base.Height = 1 << 18
)

type NetworkPolicy struct {
	util.DefaultJSONMarshaled
	hint.BaseHinter
	maxOperationsInProposal   uint64
	suffrageCandidateLifespan base.Height
}

func DefaultNetworkPolicy() NetworkPolicy {
	return NetworkPolicy{
		BaseHinter:                hint.NewBaseHinter(NetworkPolicyHint),
		maxOperationsInProposal:   DefaultMaxOperationsInProposal,
		suffrageCandidateLifespan: DefaultSuffrageCandidateLifespan,
	}
}

func (p NetworkPolicy) IsValid([]byte) error {
	e := util.ErrInvalid.Errorf("invalid NetworkPolicy")

	if err := p.BaseHinter.IsValid(NetworkPolicyHint.Type().Bytes()); err != nil {
		return e.Wrap(err)
	}

	if p.maxOperationsInProposal < 1 {
		return e.Errorf("under zero maxOperationsInProposal")
	}

	switch err := p.suffrageCandidateLifespan.IsValid(nil); {
	case err != nil:
		return e.Wrapf(err, "invalid SuffrageCandidateLifespan")
	case p.suffrageCandidateLifespan <= base.GenesisHeight:
		return e.Errorf("zero SuffrageCandidateLifespan")
	}

	return nil
}

func (p NetworkPolicy) HashBytes() []byte {
	return util.Uint64ToBytes(p.maxOperationsInProposal)
}

func (p NetworkPolicy) MaxOperationsInProposal() uint64 {
	return p.maxOperationsInProposal
}

func (p NetworkPolicy) SuffrageCandidateLifespan() base.Height {
	return p.suffrageCandidateLifespan
}

type networkPolicyJSONMarshaler struct {
	hint.BaseHinter
	MaxOperationsInProposal   uint64      `json:"max_operations_in_proposal"`
	SuffrageCandidateLifespan base.Height `json:"suffrage_candidate_lifespan"`
}

func (p NetworkPolicy) MarshalJSON() ([]byte, error) {
	return util.MarshalJSON(networkPolicyJSONMarshaler{
		BaseHinter:                p.BaseHinter,
		MaxOperationsInProposal:   p.maxOperationsInProposal,
		SuffrageCandidateLifespan: p.suffrageCandidateLifespan,
	})
}

type networkPolicyJSONUnmarshaler struct {
	MaxOperationsInProposal   uint64      `json:"max_operations_in_proposal"`
	SuffrageCandidateLifespan base.Height `json:"suffrage_candidate_lifespan"`
}

func (p *NetworkPolicy) UnmarshalJSON(b []byte) error {
	var u networkPolicyJSONUnmarshaler
	if err := util.UnmarshalJSON(b, &u); err != nil {
		return errors.Wrap(err, "failed to unmarshal NetworkPolicy")
	}

	p.maxOperationsInProposal = u.MaxOperationsInProposal
	p.suffrageCandidateLifespan = u.SuffrageCandidateLifespan

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
