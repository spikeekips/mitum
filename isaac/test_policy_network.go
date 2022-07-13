//go:build test
// +build test

package isaac

import "github.com/spikeekips/mitum/base"

func (p *NetworkPolicy) SetMaxOperationsInProposal(i uint64) *NetworkPolicy {
	p.maxOperationsInProposal = i

	return p
}

func (p *NetworkPolicy) SetSuffrageCandidateLifespan(i base.Height) *NetworkPolicy {
	p.suffrageCandidateLifespan = i

	return p
}

func (p *NetworkPolicy) SetSuffrageCandidateLimiterRule(i base.SuffrageCandidateLimiterRule) *NetworkPolicy {
	p.suffrageCandidateLimiterRule = i

	return p
}
